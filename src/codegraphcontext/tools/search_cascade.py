"""3-tier search cascade orchestrator + `search_code` MCP tool (Piece 5, Task 21).

Cascades exact Cypher match -> SQLite FTS5 lexical fallback (Task 19) ->
semantic embedding search (Task 20). Availability-gated, not
threshold-blended: whichever tier finds something first wins outright and
the cascade returns immediately -- there is no score-mixing across tiers,
per the explicit design conversation with Javier recorded in the plan's
Piece 5 intro. This also keeps the Anti-goal ("no added latency on the
fast/exact search path") literally true: when the exact tier finds
anything, the function returns before a `driver.session()` for FTS5/semantic
is ever opened.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from .code_finder import _portable_node_fields
from .indexing.embeddings import _get_embedder
from .search_fts5 import fts5_db_path, fts5_search
from .search_semantic import semantic_search
from ..utils.debug_log import warning_logger


def _fts5_conn(code_finder) -> sqlite3.Connection:
    """Open a connection to the FTS5 sidecar for the currently active context.

    `CodeFinder.__init__` only stores `db_manager` (see code_finder.py) --
    it carries no `ResolvedContext` of its own -- so this independently
    resolves the active context the same way Task 19's Phase 7 pipeline
    wiring does when it first builds the sidecar (indexing/pipeline.py),
    then locates the sidecar file via `fts5_db_path`.

    If `ENABLE_FTS5_INDEX` was never turned on for this repo, the sidecar
    file doesn't exist yet. `sqlite3.connect` to a missing path still
    succeeds (SQLite creates an empty file on first use), but the resulting
    connection has no `nodes_fts` table. `fts5_search` has no handling for
    that case today -- it issues `SELECT ... FROM nodes_fts` unconditionally
    with no guard clause (confirmed by reading search_fts5.py) -- so
    querying an unbuilt sidecar raises `sqlite3.OperationalError: no such
    table: nodes_fts` from inside `fts5_search` itself. That gap belongs to
    Task 19's own module and is flagged as a cross-task concern in this
    task's report rather than papered over here with a try/except that
    would hide it.
    """
    from ..cli.config_manager import resolve_context

    context = resolve_context()
    return sqlite3.connect(str(fts5_db_path(context)))


def _hydrate(session, pairs: List[Tuple[str, float]]) -> List[Dict[str, Any]]:
    """Fetch full node dicts for a list of `(uid, score)` pairs.

    One Cypher round trip regardless of how many pairs are supplied, via
    `_portable_node_fields` -- the same cross-backend (neo4j/falkordb/
    ladybug/kuzu) node-shape extraction `analyze_impact`'s traversal already
    relies on -- so FTS5/semantic results carry the same name/path/type
    shape as `find_related_code`'s existing `ranked_results` entries. Each
    tier's own score is reattached as `relevance_score`, and the tier's own
    ranking order is preserved (Cypher's `IN` match makes no result-order
    guarantee). A uid the sidecar/embedding index still remembers but the
    graph no longer has (stale index) is silently skipped rather than
    raised, since it is not actionable for the caller.
    """
    if not pairs:
        return []

    uids = [uid for uid, _ in pairs]
    scores = dict(pairs)
    nodes_by_uid: Dict[str, Dict[str, Any]] = {}
    for row in session.run("MATCH (n) WHERE n.uid IN $uids RETURN n", uids=uids):
        fields = _portable_node_fields(row["n"])
        uid = fields.get("uid")
        if uid:
            nodes_by_uid[uid] = fields

    hydrated: List[Dict[str, Any]] = []
    for uid in uids:
        fields = nodes_by_uid.get(uid)
        if fields is None:
            continue
        hydrated.append({**fields, "relevance_score": scores[uid]})
    return hydrated


def search_code_cascade(
    code_finder, query: str, limit: int = 20, repo_path: Optional[str] = None
) -> Dict[str, Any]:
    """Cascade exact -> FTS5 -> semantic search, stopping at the first tier
    that finds anything. Returns `{"results": [...], "tier_used":
    "exact"|"fts5"|"semantic"|"none", "query": query}`.

    `edit_distance=2` matches `find_code`'s own `DEFAULT_EDIT_DISTANCE`
    (analysis_handlers.py); its value is functionally inert here since
    `fuzzy_search=False` -- `find_by_function_name`/`find_by_class_name`
    only read `edit_distance` inside their own `if fuzzy_search:` branches
    -- but `find_related_code` requires the parameter regardless.

    `repo_path` only scopes the exact tier (`find_related_code` already
    accepts and applies it there) -- the FTS5 and semantic tiers have no
    repo-scoping of their own and are out of scope for this parameter.
    """
    exact = code_finder.find_related_code(
        query, fuzzy_search=False, edit_distance=2, repo_path=repo_path
    )
    # find_related_code always returns every key (ranked_results=[],
    # total_matches=0 on a true miss), so the dict itself is always truthy;
    # total_matches is the only reliable "did this tier actually find
    # anything" signal.
    if exact["total_matches"] > 0:
        return {
            "results": exact["ranked_results"][:limit],
            "tier_used": "exact",
            "query": query,
        }

    with code_finder.driver.session() as session:
        fts = fts5_search(_fts5_conn(code_finder), query, limit=limit)
        if fts:
            return {
                "results": _hydrate(session, fts),
                "tier_used": "fts5",
                "query": query,
            }

        try:
            embedder = _get_embedder()
        except ImportError as exc:
            warning_logger(f"[search_code] semantic tier unavailable: {exc}")
            return {"results": [], "tier_used": "none", "query": query}

        semantic = semantic_search(session, embedder, query, limit=limit)
        if semantic:
            return {
                "results": _hydrate(session, semantic),
                "tier_used": "semantic",
                "query": query,
            }

    return {"results": [], "tier_used": "none", "query": query}
