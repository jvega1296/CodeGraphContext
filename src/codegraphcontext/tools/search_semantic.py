"""Semantic search over node embeddings (search cascade tier 3).

Tier 3 of the 3-tier search cascade: exact Cypher match -> SQLite FTS5
lexical fallback (Task 19) -> this semantic embedding tier. Consumes the
existing `_get_embedder()`/`probe_embedding_backend()` from
`indexing/embeddings.py` unchanged, and the `embedding` node property
Phase 4's `EmbeddingPipeline` already writes when ENABLE_VECTOR_RESOLVE=true.

Ranking happens in plain Python (`cosine_similarity` from
`indexing/embeddings.py`, no numpy dependency) rather than a Cypher vector
function -- a deliberate choice from the design doc's constraint not to
assume LadybugDB Cypher-dialect parity for anything unproven.
"""

from __future__ import annotations

from typing import List, Tuple

from .indexing.embeddings import cosine_similarity

# Upper bound on how many embedded candidates are pulled from the graph
# before ranking in Python -- keeps the query bounded on large repos.
_CANDIDATE_CAP = 500


def semantic_search(session, embedder, query: str, limit: int = 20) -> List[Tuple[str, float]]:
    """Embed the query, fetch candidate node embeddings via Cypher, rank by cosine similarity in Python.

    Returns (node_uid, similarity) pairs, best first. Returns `[]` when no
    node carries a non-null `embedding` property yet -- e.g. a repo indexed
    without ENABLE_VECTOR_RESOLVE=true -- rather than raising, so the
    cascade's "no tier found anything" path stays reachable.
    """
    candidates = [
        (row["uid"], row["embedding"])
        for row in session.run(
            "MATCH (n) WHERE n.embedding IS NOT NULL "
            "RETURN n.uid AS uid, n.embedding AS embedding LIMIT $candidate_cap",
            candidate_cap=_CANDIDATE_CAP,
        )
        if row["uid"] and row["embedding"]
    ]
    if not candidates:
        return []

    query_vector = embedder.embed_batch([query])[0]
    scored = [(uid, cosine_similarity(query_vector, vec)) for uid, vec in candidates]
    scored.sort(key=lambda pair: pair[1], reverse=True)
    return scored[:limit]
