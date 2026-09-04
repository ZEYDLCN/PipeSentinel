"""Sentetik commerce verisi üretimi (examples/commerce-pipeline).

Üç veri kümesi üretir: ``customers``, ``orders``, ``payments``. Üretim,
``docs/pipeline-sentinel-design.md`` §6.2'deki profil örneğine yakın bir
dağılım hedefler (ör. ``total_amount`` ortalaması ~480, std ~130) ki
F05 (TL → kuruş) gibi enjekte edilen hatalar gerçekçi bir baseline'a karşı
ölçülebilsin.

Fonksiyonlar saf pandas/numpy üzerinde çalışır; veritabanı bağımlılığı
yoktur, bu yüzden birim testlerde doğrudan çağrılabilir.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker

ORDER_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["credit_card", "bank_transfer", "wallet", "cash_on_delivery"]
COUNTRIES = ["TR", "DE", "NL", "GB", "US", "FR"]

MEAN_AMOUNT = 482.17
STDDEV_AMOUNT = 131.44


@dataclass(frozen=True)
class CommerceBatch:
    """Bir pipeline run'ında üretilen üç tablonun DataFrame'leri."""

    customers: pd.DataFrame
    orders: pd.DataFrame
    payments: pd.DataFrame


def generate_customers(n: int, seed: int = 42, start_id: int = 1) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(seed)
    rng = np.random.default_rng(seed)

    rows = []
    for i in range(n):
        signup_days_ago = int(rng.integers(1, 900))
        rows.append(
            {
                "customer_id": start_id + i,
                "email": fake.unique.email(),
                "country": rng.choice(COUNTRIES, p=[0.45, 0.15, 0.1, 0.1, 0.1, 0.1]),
                "signup_at": datetime.now(timezone.utc) - timedelta(days=signup_days_ago),
                "is_active": bool(rng.random() > 0.05),
            }
        )
    return pd.DataFrame(rows)


def generate_orders(
    n: int,
    customer_ids: list[int],
    seed: int = 42,
    start_id: int = 1,
    run_at: datetime | None = None,
    mean_amount: float = MEAN_AMOUNT,
    stddev_amount: float = STDDEV_AMOUNT,
    currency: str = "TRY",
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    run_at = run_at or datetime.now(timezone.utc)

    amounts = rng.normal(loc=mean_amount, scale=stddev_amount, size=n)
    amounts = np.clip(amounts, 5.0, None).round(2)

    rows = []
    for i in range(n):
        created_offset = timedelta(minutes=int(rng.integers(0, 24 * 60)))
        rows.append(
            {
                "order_id": start_id + i,
                "customer_id": int(rng.choice(customer_ids)),
                "status": rng.choice(
                    ORDER_STATUSES, p=[0.15, 0.35, 0.2, 0.25, 0.05]
                ),
                "total_amount": float(amounts[i]),
                "currency": currency,
                "created_at": run_at - created_offset,
            }
        )
    return pd.DataFrame(rows)


def generate_payments(
    orders: pd.DataFrame,
    seed: int = 42,
    start_id: int = 1,
    missing_rate: float = 0.01,
) -> pd.DataFrame:
    """Her sipariş için bir ödeme üretir; ``missing_rate`` kadarını atlar
    (completeness/referential-integrity testlerinde baseline gürültüsü)."""
    rng = np.random.default_rng(seed)
    rows = []
    pid = start_id
    for _, order in orders.iterrows():
        if rng.random() < missing_rate:
            continue
        paid_offset = timedelta(minutes=int(rng.integers(1, 120)))
        rows.append(
            {
                "payment_id": pid,
                "order_id": int(order["order_id"]),
                "amount": float(order["total_amount"]),
                "method": rng.choice(PAYMENT_METHODS, p=[0.5, 0.2, 0.2, 0.1]),
                "paid_at": order["created_at"] + paid_offset,
            }
        )
        pid += 1
    return pd.DataFrame(rows)


def generate_commerce_batch(
    n_customers: int,
    n_orders: int,
    seed: int = 42,
    customer_start_id: int = 1,
    order_start_id: int = 1,
    payment_start_id: int = 1,
    run_at: datetime | None = None,
) -> CommerceBatch:
    customers = generate_customers(n_customers, seed=seed, start_id=customer_start_id)
    orders = generate_orders(
        n_orders,
        customer_ids=customers["customer_id"].tolist(),
        seed=seed,
        start_id=order_start_id,
        run_at=run_at,
    )
    payments = generate_payments(orders, seed=seed, start_id=payment_start_id)
    return CommerceBatch(customers=customers, orders=orders, payments=payments)
