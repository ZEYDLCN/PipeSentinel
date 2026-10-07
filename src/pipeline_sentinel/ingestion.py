"""dbt artifacts and OpenLineage RunEvents normalized into explicit graphs."""
from __future__ import annotations

from . import column_lineage
from .contract_io import digest


def dbt_graph(manifest: dict, results: dict | None = None) -> dict:
    if not isinstance(manifest.get("nodes"), dict) or not isinstance(manifest.get("metadata"), dict):
        raise ValueError("dbt manifest metadata/nodes gerekli")
    schema = manifest["metadata"].get("dbt_schema_version", "")
    if not schema.startswith("https://schemas.getdbt.com/dbt/manifest/v"):
        raise ValueError("dbt manifest schema sürümü gerekli")
    if any(not isinstance(manifest.get(key, {}), dict) for key in ("sources", "exposures")):
        raise ValueError("dbt sources/exposures nesne olmalı")
    nodes = {**manifest.get("sources", {}), **manifest["nodes"], **manifest.get("exposures", {})}
    if len(nodes) > 20000:
        raise ValueError("En fazla 20000 dbt düğümü")
    normalized = {}
    edges = []
    for uid, node in nodes.items():
        if not isinstance(node, dict) or not isinstance(node.get("depends_on", {}), dict) or not isinstance(node.get("depends_on", {}).get("nodes", []), list):
            raise ValueError("Geçersiz dbt düğüm veya bağımlılık kaydı")
        normalized[uid] = {"name": node.get("name", uid), "type": node.get("resource_type", "source"),
                           "relation": node.get("relation_name"), "columns": sorted(node.get("columns", {})),
                           "owner": node.get("owner"), "checksum": digest({
                               "code": node.get("raw_code", node.get("raw_sql", "")),
                               "config": node.get("config", {}), "columns": node.get("columns", {}),
                               "depends_on": node.get("depends_on", {})})}
        for parent in node.get("depends_on", {}).get("nodes", []):
            if parent in nodes:
                edges.append([parent, uid])
    runs = []
    if results is not None:
        if not isinstance(results.get("results"), list):
            raise ValueError("run_results.results liste olmalı")
        for result in results["results"]:
            if not isinstance(result, dict):
                raise ValueError("Geçersiz run_results kaydı")
            uid = result.get("unique_id")
            if uid not in nodes:
                raise ValueError("run_results ile manifest aynı projeye ait olmalı")
            runs.append({"asset": uid, "status": result.get("status"), "execution_time": result.get("execution_time"),
                         "timing": result.get("timing", [])})
    column_edges, column_stats = [], None
    if column_lineage.available() and any(isinstance(n, dict) and (n.get("compiled_code") or n.get("compiled_sql"))
                                          for n in nodes.values()):
        column_edges, column_stats = column_lineage.dbt_column_edges(nodes, manifest["metadata"].get("adapter_type"))
    coverage = ("dataset dependencies; column lineage inferred best-effort from compiled SQL"
                if column_stats else "dataset dependencies; no column lineage (needs compiled SQL and the sqlglot extra)")
    return {"nodes": normalized, "edges": sorted(edges), "runs": runs,
            "generated_at": manifest["metadata"].get("generated_at"), "hash": digest(manifest),
            "column_edges": column_edges, "column_stats": column_stats, "coverage": coverage}


def openlineage_graph(event: dict) -> dict:
    if event.get("eventType") not in {"START", "RUNNING", "COMPLETE", "FAIL", "ABORT", "OTHER"}:
        raise ValueError("Geçersiz OpenLineage eventType")
    if not event.get("run", {}).get("runId") or not event.get("eventTime"):
        raise ValueError("OpenLineage runId/eventTime gerekli")
    def key(item):
        if not isinstance(item, dict) or not isinstance(item.get("namespace"), str) or not isinstance(item.get("name"), str) or not item["namespace"] or not item["name"]:
            raise ValueError("OpenLineage namespace/name gerekli")
        return item["namespace"] + "::" + item["name"]
    job = event.get("job", {})
    job_key = "job::" + key(job)
    nodes = {job_key: {"name": job["name"], "type": "job"}}
    edges = []
    for direction in ("inputs", "outputs"):
        if not isinstance(event.get(direction, []), list) or len(event.get(direction, [])) > 10000:
            raise ValueError("OpenLineage inputs/outputs sınırlı liste olmalı")
        for dataset in event.get(direction, []):
            uid = key(dataset)
            nodes[uid] = {"name": dataset["name"], "type": "dataset"}
            edges.append([uid, job_key] if direction == "inputs" else [job_key, uid])
    # Kolon bağlantıları yalnızca olayın standart `columnLineage` facet'inde açıkça bildirilmişse alınır;
    # SQL'den veya job giriş/çıkışlarından kolon türetimi uydurulmaz.
    column_edges = []
    for dataset in event.get("outputs", []):
        fields = ((dataset.get("facets") or {}).get("columnLineage") or {}).get("fields") or {}
        if not isinstance(fields, dict) or len(fields) > 2000:
            raise ValueError("OpenLineage columnLineage.fields sınırlı bir nesne olmalı")
        for out_column, spec in fields.items():
            inputs = (spec or {}).get("inputFields", []) if isinstance(spec, dict) else []
            if not isinstance(inputs, list) or len(inputs) > 200:
                raise ValueError("OpenLineage inputFields sınırlı liste olmalı")
            for source in inputs:
                if not isinstance(source.get("field") if isinstance(source, dict) else None, str) or not source["field"]:
                    raise ValueError("OpenLineage inputFields.field gerekli")
                column_edges.append([key(source), source["field"].lower(), key(dataset), str(out_column).lower()])
    return {"nodes": nodes, "edges": edges, "runs": [{"asset": job_key, "run_id": event["run"]["runId"],
             "status": event["eventType"], "event_time": event["eventTime"]}], "hash": digest(event),
            "column_edges": column_edges, "column_stats": None,
            "coverage": "job input/output reachability; potential impact only"
                        + ("; column lineage from the columnLineage facet" if column_edges else "")}


def reachable_columns(column_edges: list, asset: str, column: str, reverse=False, limit=200) -> list[tuple[str, str]]:
    """(varlık, kolon) çiftleri üzerinde yön izleyen BFS; başlangıç hariç, en fazla `limit` sonuç."""
    adjacency = {}
    for source_asset, source_column, target_asset, target_column in column_edges:
        left, right = (source_asset, source_column), (target_asset, target_column)
        if reverse:
            left, right = right, left
        adjacency.setdefault(left, set()).add(right)
    start = (asset, column.lower())
    seen, queue, found = {start}, [start], []
    while queue and len(found) < limit:
        for node in sorted(adjacency.get(queue.pop(0), ())):
            if node not in seen:
                seen.add(node)
                queue.append(node)
                found.append(node)
    return found[:limit]


def reachable(edges: list, start: str, reverse=False) -> set[str]:
    adjacency = {}
    for left, right in edges:
        if reverse:
            left, right = right, left
        adjacency.setdefault(left, set()).add(right)
    seen, stack = {start}, [start]
    while stack:
        for node in adjacency.get(stack.pop(), ()):
            if node not in seen:
                seen.add(node)
                stack.append(node)
    return seen - {start}
