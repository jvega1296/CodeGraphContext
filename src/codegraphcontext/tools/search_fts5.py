"""SQLite FTS5 lexical search sidecar (search cascade tier 2).

Tier 2 of the 3-tier search cascade: exact Cypher match -> this FTS5 lexical
fallback -> semantic embeddings (Task 20). A full clear-and-reinsert rebuild
only -- no incremental delta tracking this pass (YAGNI, per Piece 5's intro).
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

from ..utils.debug_log import info_logger, warning_logger

# Weights for bm25() ranking, keyed by column name. Only the columns a query
# should actually favor need an entry; any column missing from this dict
# (just `uid`, which is UNINDEXED and never matched against) falls back to
# `_DEFAULT_COLUMN_WEIGHT` in `_fts_column_weights`.
_FTS_COLUMN_WEIGHTS: Dict[str, float] = {
    "name": 10.0, "qualified_name": 6.0, "file_path": 1.0,
    "signature": 2.0, "docstring": 1.5, "name_tokens": 4.0,
}

_DEFAULT_COLUMN_WEIGHT = 1.0


def split_identifier(name: str) -> str:
    """snake_case/camelCase/PascalCase/dotted -> space-separated words.

    NOT a verbatim port of code-review-graph's ``_split_identifier``
    (embeddings.py:1076-1091). That function's single lowercase->uppercase
    regex never splits an acronym boundary such as "APIRoute" -- there is no
    lowercase letter immediately before that transition -- which contradicts
    its own docstring example; running it against that exact example
    confirms the mismatch. This adds an acronym-boundary rule ahead of the
    standard camelCase rule so "APIRoute" -> "API Route" while the snake_case
    and plain camelCase behavior stays identical.
    """
    if not name:
        return ""
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name)
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", spaced)
    spaced = re.sub(r"[_./\-]+", " ", spaced)
    return " ".join(spaced.split())


def fts5_db_path(context) -> Path:
    """Sidecar file location, sibling to the active context's db directory.

    ``context`` is a ``codegraphcontext.cli.config_manager.ResolvedContext``
    (or any object exposing ``.db_path``), e.g. the result of
    ``resolve_context()``. The sidecar is backend-agnostic, so it lives next
    to -- not nested inside -- the active backend's own directory:
    ``.../db/search_fts5.sqlite``, alongside ``.../db/ladybugdb/`` or
    ``.../db/kuzudb/``.
    """
    return Path(context.db_path).parent / "search_fts5.sqlite"


def ensure_fts5_schema(conn: sqlite3.Connection) -> None:
    """Create the ``nodes_fts`` virtual table if it does not already exist."""
    conn.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5("
        "uid UNINDEXED, name, qualified_name, file_path, signature, "
        "docstring, name_tokens)"
    )
    conn.commit()


_REBUILD_QUERY = (
    "MATCH (n) WHERE n:Function OR n:Class OR n:Variable "
    "RETURN n.uid AS uid, n.name AS name, n.path AS path, "
    "n.qualified_name AS qualified_name, n.docstring AS docstring"
)


def rebuild_fts5_index(conn: sqlite3.Connection, session) -> int:
    """Full rebuild from the current graph's Function/Class/Variable nodes.

    Clear-and-reinsert only -- no incremental delta logic this pass (YAGNI,
    per Piece 5's intro). ``signature`` is always inserted empty: no language
    parser in this codebase currently sets a ``signature`` node property, so
    there is nothing to read yet; the column exists to satisfy the fixed
    Task 21 interface and for forward compatibility once that property
    exists. Rows with no ``uid`` are skipped -- they would be unreachable
    through ``fts5_search``'s return value. Returns the number of rows
    indexed.
    """
    ensure_fts5_schema(conn)

    rows_out: List[Tuple[str, str, str, str, str, str, str]] = []
    for row in session.run(_REBUILD_QUERY):
        uid = row["uid"]
        if not uid:
            continue
        name = row["name"] or ""
        rows_out.append((
            uid,
            name,
            row["qualified_name"] or "",
            row["path"] or "",
            "",
            row["docstring"] or "",
            split_identifier(name),
        ))

    conn.execute("DELETE FROM nodes_fts")
    conn.executemany(
        "INSERT INTO nodes_fts "
        "(uid, name, qualified_name, file_path, signature, docstring, name_tokens) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows_out,
    )
    conn.commit()
    info_logger(f"[FTS5] Rebuilt nodes_fts index: {len(rows_out)} rows")
    return len(rows_out)


def _fts_column_weights(conn: sqlite3.Connection) -> str:
    """Build the ``bm25()`` weight-argument list from the table's live column order.

    ``bm25()``'s weight arguments map positionally to *every* declared
    column of the FTS5 table, including ``UNINDEXED`` ones -- verified
    empirically: SQLite does not skip ``UNINDEXED`` columns when consuming
    weight arguments. Reading the column order from ``PRAGMA table_info``
    (rather than hardcoding a 6-value string keyed only to the indexed
    columns) is therefore required, not just defensive: a hardcoded 6-value
    list would silently shift every weight one column to the left against
    this schema's leading ``uid UNINDEXED`` column (e.g. the ``name`` weight
    would land on ``qualified_name`` instead).
    """
    columns = [row[1] for row in conn.execute("PRAGMA table_info(nodes_fts)")]
    weights = [_FTS_COLUMN_WEIGHTS.get(column, _DEFAULT_COLUMN_WEIGHT) for column in columns]
    return ", ".join(f"{weight:.1f}" for weight in weights)


def fts5_search(conn: sqlite3.Connection, query: str, limit: int = 20) -> List[Tuple[str, float]]:
    """Returns (node_uid, bm25_score) pairs, best first.

    SQLite's ``bm25()`` returns lower (more negative) values for better
    matches. Order ascending on the raw value in SQL (so the best match is
    already first), then negate each score in Python after fetching, so
    callers see "higher score = better match" without re-sorting.

    ``ENABLE_FTS5_INDEX`` defaults to ``false``, so a sidecar connection
    with no ``nodes_fts`` table at all is the common case, not an edge
    case -- the same guard code-review-graph's own ``_fts_search`` already
    carries. ``PRAGMA table_info`` on a missing table returns zero rows
    rather than raising, so ``_fts_column_weights`` silently yields an
    empty weights string first; the actual ``sqlite3.OperationalError``
    (``no such table: nodes_fts``, surfaced here as a malformed
    ``bm25(nodes_fts, )`` call) only fires once the query itself executes.
    Catching around the whole body covers that real failure point.
    """
    try:
        weights = _fts_column_weights(conn)
        sql = (
            f"SELECT uid, bm25(nodes_fts, {weights}) AS score FROM nodes_fts "
            "WHERE nodes_fts MATCH ? ORDER BY score LIMIT ?"
        )
        rows = conn.execute(sql, (query, limit)).fetchall()
    except sqlite3.OperationalError as exc:
        warning_logger(f"[FTS5] lexical search unavailable: {exc}")
        return []
    return [(row[0], -row[1]) for row in rows]
