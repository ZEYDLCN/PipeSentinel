"""Bir pipeline run'ını uçtan uca yürütür: üret → (opsiyonel hata enjekte
et) → yükle → profille → baseline ile karşılaştır → tespit et → kaydet.

CLI (`sentinel run` / `sentinel bootstrap`) ve testler bu modülü kullanır.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import Engine, text

from .contracts import DEFAULT_CONTRACTS
from .detector import DetectionReport, run_detection
from .faults import FAULT_CATALOG, apply_fault
from .pipeline import (
    add_lineage_edge,
    finish_run,
    get_all_column_ids,
    get_baseline_profile,
    load_dataframe,
    register_dataset,
    register_external_asset,
    register_job,
    save_profile,
    save_signals,
    start_run,
)
from .profiler import profile_dataset
from .synthetic import generate_commerce_batch

NAMESPACE = "commerce"
JOB_NAMES = {
    "customers": "customers_etl",
    "orders": "orders_etl",
    "payments": "payments_etl",
}


@dataclass
class RunOutcome:
    run_ids: dict[str, str]
    reports: dict[str, DetectionReport]
    fault_id: str | None


def _max_id(engine: Engine, table: str, id_column: str) -> int:
    with engine.begin() as conn:
        row = conn.execute(text(f"SELECT COALESCE(MAX({id_column}), 0) FROM commerce.{table}")).first()
        return int(row[0]) if row else 0


def _existing_customer_ids(engine: Engine, limit: int = 3000) -> list[int]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT customer_id FROM commerce.customers ORDER BY random() LIMIT :limit"),
            {"limit": limit},
        ).all()
    return [r[0] for r in rows]


def execute_pipeline_run(
    engine: Engine,
    fault_id: str | None = None,
    n_customers: int = 50,
    n_orders: int = 500,
    seed: int | None = None,
) -> RunOutcome:
    seed = seed if seed is not None else random.randint(1, 1_000_000)
    run_at = datetime.now(timezone.utc)

    next_customer_id = _max_id(engine, "customers", "customer_id") + 1
    next_order_id = _max_id(engine, "orders", "order_id") + 1
    next_payment_id = _max_id(engine, "payments", "payment_id") + 1

    batch = generate_commerce_batch(
        n_customers=n_customers,
        n_orders=n_orders,
        seed=seed,
        customer_start_id=next_customer_id,
        order_start_id=next_order_id,
        payment_start_id=next_payment_id,
        run_at=run_at,
    )

    existing_customer_ids = _existing_customer_ids(engine)
    customer_pool = existing_customer_ids + batch.customers["customer_id"].tolist()
    if customer_pool:
        rng = random.Random(seed)
        batch.orders["customer_id"] = [rng.choice(customer_pool) for _ in range(len(batch.orders))]

    frames: dict[str, pd.DataFrame] = {
        "customers": batch.customers,
        "orders": batch.orders,
        "payments": batch.payments,
    }

    fault_result = None
    if fault_id:
        if fault_id not in FAULT_CATALOG:
            raise ValueError(f"Bilinmeyen fault_id: {fault_id}")
        frames, fault_result = apply_fault(frames, fault_id)

    run_ids: dict[str, str] = {}
    reports: dict[str, DetectionReport] = {}
    dataset_ids: dict[str, str] = {}
    job_ids: dict[str, str] = {}
    column_ids_by_dataset: dict[str, dict[str, str]] = {}

    for dataset_name in ("customers", "orders", "payments"):
        df = frames[dataset_name]
        job_name = JOB_NAMES[dataset_name]
        this_fault = fault_id if (fault_result and fault_result.dataset == dataset_name) else None

        # Fault enjekte edilmiş bir run'ın (mutasyona uğramış) dtype'ı
        # kanonik `columns.data_type` katalogunu bozmasın (ör. F02 sonrası
        # 'status' kalıcı olarak integer görünmesin) — bkz. register_dataset
        # docstring'i.
        dataset_id, column_ids = register_dataset(
            engine, NAMESPACE, dataset_name, df, track_column_types=this_fault is None
        )
        job_id = register_job(engine, NAMESPACE, job_name, code_ref=f"examples/commerce-pipeline/{dataset_name}")
        dataset_ids[dataset_name] = dataset_id
        job_ids[dataset_name] = job_id
        column_ids_by_dataset[dataset_name] = column_ids

        run_id = start_run(engine, job_id, fault_id=this_fault)
        run_ids[dataset_name] = run_id

        load_dataframe(engine, "commerce", dataset_name, df, if_exists="append")

        current_profile = profile_dataset(df)
        save_profile(engine, run_id, column_ids, current_profile)

        baseline_profile, baseline_row_count = get_baseline_profile(
            engine, job_id, column_ids, exclude_run_id=run_id
        )

        contract = DEFAULT_CONTRACTS[dataset_name]
        report = run_detection(
            dataset=dataset_name,
            df=df,
            contract=contract,
            current_profile=current_profile,
            baseline_profile=baseline_profile or None,
            baseline_row_count=baseline_row_count,
        )
        # Sinyal bir kolonu referans ediyor olabilir ama o kolon bu run'ın
        # df'inde artık yok (F01 gibi bir "kolon silindi" senaryosu) —
        # bu durumda current_run'ın column_ids'i o adı içermez. `columns`
        # tablosundaki tüm tarihsel kolon id'leriyle birleştirerek
        # kolon adının kaybolmamasını sağlıyoruz (bkz. get_all_column_ids).
        all_column_ids = get_all_column_ids(engine, dataset_id)
        save_signals(engine, run_id, dataset_id, {**all_column_ids, **column_ids}, report.signals)
        reports[dataset_name] = report

        finish_run(engine, run_id, status="success", metadata={"row_count": len(df), "fault_id": this_fault})

    sync_commerce_lineage(engine, dataset_ids, job_ids, column_ids_by_dataset)

    return RunOutcome(run_ids=run_ids, reports=reports, fault_id=fault_id)


def sync_commerce_lineage(
    engine: Engine,
    dataset_ids: dict[str, str],
    job_ids: dict[str, str],
    column_ids_by_dataset: dict[str, dict[str, str]],
) -> None:
    """`examples/commerce-pipeline` için sabit yapısal lineage graph'ını
    idempotent olarak (yeniden) bildirir (§9). Her çağrı no-op'a yakındır —
    kenarlar `ON CONFLICT DO NOTHING` ile eklenir (bkz. pipeline.py).

    Bir run'da bir kolon eksikse (örn. F01 `customer_id`'yi siler) o kenar
    o run'da atlanır; kolon başka bir (sağlıklı) run'da mevcutsa graf zaten
    kurulmuş olur — lineage yalnızca eklenir, run bazlı silinmez.
    """

    def edge(source_id, source_type, target_id, target_type, edge_type):
        if source_id and target_id:
            add_lineage_edge(engine, source_id, source_type, target_id, target_type, edge_type)

    # job -> dataset (job'ın ürettiği veri) — WRITES_TO
    for dataset_name, job_id in job_ids.items():
        edge(job_id, "job", dataset_ids[dataset_name], "dataset", "WRITES_TO")

    # dataset -> job (job'ın okuduğu upstream veri) — READS_FROM
    edge(dataset_ids.get("customers"), "dataset", job_ids.get("orders"), "job", "READS_FROM")
    edge(dataset_ids.get("orders"), "dataset", job_ids.get("payments"), "job", "READS_FROM")

    # kolon seviyesi DERIVED_FROM (origin -> derived)
    customers_cols = column_ids_by_dataset.get("customers", {})
    orders_cols = column_ids_by_dataset.get("orders", {})
    payments_cols = column_ids_by_dataset.get("payments", {})

    edge(customers_cols.get("customer_id"), "column", orders_cols.get("customer_id"), "column", "DERIVED_FROM")
    edge(orders_cols.get("order_id"), "column", payments_cols.get("order_id"), "column", "DERIVED_FROM")
    edge(orders_cols.get("total_amount"), "column", payments_cols.get("amount"), "column", "DERIVED_FROM")

    # dataset/column -> external asset — FEEDS (§3.4, §12.1 örnek senaryo)
    if orders_cols.get("total_amount"):
        dashboard_id = register_external_asset(engine, "dashboard", "finance_dashboard", owner="finance-team")
        ml_feature_id = register_external_asset(engine, "ml_feature", "revenue_forecast_model", owner="ml-team")
        edge(orders_cols["total_amount"], "column", dashboard_id, "external_asset", "FEEDS")
        edge(orders_cols["total_amount"], "column", ml_feature_id, "external_asset", "FEEDS")
