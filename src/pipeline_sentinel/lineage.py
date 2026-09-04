"""Lineage graph — saf traversal mantığı (§9).

Bu modül veritabanı bilmez: girdisi bir `Edge` listesidir (DB'den tek
sorguyla çekilir), çıktısı upstream/downstream düğüm listeleridir. Küçük
graf (birkaç düzine kenar) için tüm kenarları belleğe çekip BFS yapmak,
recursive CTE yazmaktan daha basit, test edilebilir ve düğüm tipi
karışıklığına daha az açıktır.

Yön kuralı (§9.2 uygulama notuna bakın — `infra/migrations/003_lineage.sql`):
her kenar `source -> target` VERİ/ETKİ AKIŞI yönünde saklanır: source
bozulursa target etkilenir. Bu yüzden:

    downstream_impact(N) = source == N olan kenarları takip ederek ulaşılanlar
    upstream_sources(N)  = target == N olan kenarları takip ederek ulaşılanlar
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

NodeType = str  # "dataset" | "column" | "job" | "external_asset"
EdgeType = str  # "READS_FROM" | "WRITES_TO" | "DERIVED_FROM" | "FEEDS"


@dataclass(frozen=True)
class Edge:
    source_id: str
    source_type: NodeType
    target_id: str
    target_type: NodeType
    edge_type: EdgeType


@dataclass(frozen=True)
class GraphHit:
    """Bir traversal sonucunda ulaşılan düğüm."""

    node_id: str
    node_type: NodeType
    hop: int
    via_edge_type: EdgeType
    path: tuple[str, ...]  # kök düğümden bu düğüme kadar izlenen id'ler

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "hop": self.hop,
            "via_edge_type": self.via_edge_type,
            "path": list(self.path),
        }


def _traverse(edges: list[Edge], start_id: str, forward: bool, max_hops: int = 6) -> list[GraphHit]:
    """BFS. forward=True → source->target (downstream), False → target->source (upstream)."""
    adjacency: dict[str, list[Edge]] = {}
    for e in edges:
        key = e.source_id if forward else e.target_id
        adjacency.setdefault(key, []).append(e)

    visited: set[str] = {start_id}
    hits: list[GraphHit] = []
    queue: deque[tuple[str, int, tuple[str, ...]]] = deque([(start_id, 0, (start_id,))])

    while queue:
        current_id, hop, path = queue.popleft()
        if hop >= max_hops:
            continue
        for edge in adjacency.get(current_id, []):
            next_id = edge.target_id if forward else edge.source_id
            next_type = edge.target_type if forward else edge.source_type
            if next_id in visited:
                continue
            visited.add(next_id)
            next_path = path + (next_id,)
            hits.append(
                GraphHit(
                    node_id=next_id,
                    node_type=next_type,
                    hop=hop + 1,
                    via_edge_type=edge.edge_type,
                    path=next_path,
                )
            )
            queue.append((next_id, hop + 1, next_path))

    return hits


def downstream_impact(edges: list[Edge], node_id: str, max_hops: int = 6) -> list[GraphHit]:
    """`node_id` bozulursa etkilenecek tüm düğümler (§9.3 "Downstream etki")."""
    return _traverse(edges, node_id, forward=True, max_hops=max_hops)


def upstream_sources(edges: list[Edge], node_id: str, max_hops: int = 6) -> list[GraphHit]:
    """`node_id`'yi etkileyebilecek olası upstream kaynaklar, en yakından en
    uzağa sıralı (§9.3 "İlk olası upstream kaynak" — `ORDER BY length(p) ASC`
    ile aynı sıralama BFS'in doğal sonucudur)."""
    return _traverse(edges, node_id, forward=False, max_hops=max_hops)
