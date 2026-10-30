"""Unit tests for the FTS5 lexical search sidecar (Piece 5, Task 19)."""

import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

from codegraphcontext.tools.search_fts5 import (
    _FTS_COLUMN_WEIGHTS,
    _fts_column_weights,
    ensure_fts5_schema,
    fts5_db_path,
    fts5_search,
    rebuild_fts5_index,
    split_identifier,
)


def test_split_identifier_snake_camel_pascal_dotted():
    assert split_identifier("get_route_handler") == "get route handler"
    assert split_identifier("APIRoute") == "API Route"
    assert split_identifier("dispatch_request") == "dispatch request"
    assert split_identifier("full_dispatch_request") == "full dispatch request"


def test_split_identifier_empty_string():
    assert split_identifier("") == ""


def test_sqlite_fts5_is_available():
    """Capability probe, not a unit of new code -- if this fails in some
    environment, FTS5 support is unavailable and nothing else here can work."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(x)")


def test_fts5_db_path_is_sibling_of_context_db_directory():
    class _FakeContext:
        db_path = str(Path("/home/user/.codegraphcontext/global/db/ladybugdb"))

    result = fts5_db_path(_FakeContext())
    assert result == Path("/home/user/.codegraphcontext/global/db") / "search_fts5.sqlite"


def test_ensure_fts5_schema_is_idempotent(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "idempotent.sqlite"))
    ensure_fts5_schema(conn)
    ensure_fts5_schema(conn)  # must not raise


# ---------------------------------------------------------------------------
# Fake session matching this codebase's `.run(query, **kwargs)` fake-session
# convention (see tests/unit/tools/test_risk_scoring.py's _FakeNodeSession).
# ---------------------------------------------------------------------------

class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def run(self, query: str, **kwargs: Any) -> _FakeResult:
        return _FakeResult(self._rows)


@pytest.fixture
def fake_session_with_functions_and_classes() -> _FakeSession:
    return _FakeSession([
        {
            "uid": "uid-create-order",
            "name": "createOrder",
            "path": "src/main/java/com/az/oms/OrderService.java",
            "qualified_name": "com.az.oms.OrderService.createOrder",
            "docstring": "Creates a new order.",
        },
        {
            "uid": "uid-order-service-class",
            "name": "OrderService",
            "path": "src/main/java/com/az/oms/OrderService.java",
            "qualified_name": "com.az.oms.OrderService",
            "docstring": "Handles order lifecycle.",
        },
        {
            # No uid: must be skipped rather than inserted as unreachable noise.
            "uid": None,
            "name": "anonymous",
            "path": "src/main/java/com/az/oms/OrderService.java",
            "qualified_name": None,
            "docstring": None,
        },
    ])


def test_rebuild_fts5_index_populates_from_fake_session(tmp_path, fake_session_with_functions_and_classes):
    conn = sqlite3.connect(str(tmp_path / "test.sqlite"))
    ensure_fts5_schema(conn)
    count = rebuild_fts5_index(conn, fake_session_with_functions_and_classes)
    assert count > 0


def test_rebuild_fts5_index_skips_rows_without_uid(tmp_path, fake_session_with_functions_and_classes):
    conn = sqlite3.connect(str(tmp_path / "skip.sqlite"))
    ensure_fts5_schema(conn)
    count = rebuild_fts5_index(conn, fake_session_with_functions_and_classes)
    assert count == 2  # the uid=None row must be excluded


def test_rebuild_fts5_index_is_a_full_clear_and_reinsert(tmp_path):
    """Second rebuild with fewer rows must not leave stale rows behind --
    this sidecar does full clear-and-reinsert, not incremental delta tracking."""
    conn = sqlite3.connect(str(tmp_path / "rebuild.sqlite"))
    ensure_fts5_schema(conn)
    first_session = _FakeSession([
        {"uid": "uid-a", "name": "alpha", "path": "a.py", "qualified_name": "a.alpha", "docstring": ""},
        {"uid": "uid-b", "name": "beta", "path": "b.py", "qualified_name": "b.beta", "docstring": ""},
    ])
    rebuild_fts5_index(conn, first_session)

    second_session = _FakeSession([
        {"uid": "uid-a", "name": "alpha", "path": "a.py", "qualified_name": "a.alpha", "docstring": ""},
    ])
    count = rebuild_fts5_index(conn, second_session)

    assert count == 1
    remaining = conn.execute("SELECT uid FROM nodes_fts").fetchall()
    assert remaining == [("uid-a",)]


def test_rebuild_fts5_index_computes_name_tokens(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "tokens.sqlite"))
    ensure_fts5_schema(conn)
    session = _FakeSession([
        {"uid": "uid-handler", "name": "get_route_handler", "path": "h.py", "qualified_name": "", "docstring": ""},
    ])
    rebuild_fts5_index(conn, session)
    name_tokens = conn.execute("SELECT name_tokens FROM nodes_fts WHERE uid = 'uid-handler'").fetchone()[0]
    assert name_tokens == "get route handler"


@pytest.fixture
def populated_fts5_conn(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "search.sqlite"))
    session = _FakeSession([
        {
            "uid": "uid-of-createorder-function",
            "name": "createOrder",
            "path": "src/main/java/com/az/oms/OrderService.java",
            "qualified_name": "com.az.oms.OrderService.createOrder",
            "docstring": "Validates input before persisting.",
        },
        {
            "uid": "uid-of-unrelated-function",
            "name": "validateInput",
            "path": "src/main/java/com/az/oms/OrderService.java",
            "qualified_name": "com.az.oms.OrderService.validateInput",
            "docstring": "Called before createOrder runs to validate order fields.",
        },
    ])
    ensure_fts5_schema(conn)
    rebuild_fts5_index(conn, session)
    return conn


def test_fts5_search_ranks_name_match_above_docstring_match(populated_fts5_conn):
    results = fts5_search(populated_fts5_conn, "createOrder")
    # a node literally named createOrder should outrank one that only mentions it in a docstring
    assert results[0][0] == "uid-of-createorder-function"


def test_fts5_search_returns_best_first_with_positive_scores(populated_fts5_conn):
    results = fts5_search(populated_fts5_conn, "createOrder")
    assert len(results) == 2
    scores = [score for _uid, score in results]
    assert scores == sorted(scores, reverse=True)


def test_fts5_search_respects_limit(populated_fts5_conn):
    results = fts5_search(populated_fts5_conn, "createOrder OR validateInput", limit=1)
    assert len(results) == 1


def test_fts_column_weights_align_with_live_schema_column_order(populated_fts5_conn):
    """Regression guard for the exact silent-misalignment risk the brief calls
    out: bm25()'s weight arguments map positionally to EVERY declared column
    of `nodes_fts`, including the UNINDEXED `uid` column -- not just the 6
    indexed ones (verified empirically). A hardcoded 6-value weight string
    would silently shift every weight one column to the left."""
    columns = [row[1] for row in populated_fts5_conn.execute("PRAGMA table_info(nodes_fts)")]
    weights_str = _fts_column_weights(populated_fts5_conn)
    weight_values = [float(w) for w in weights_str.split(", ")]

    assert len(weight_values) == len(columns) == 7
    assert columns[0] == "uid"
    # uid (UNINDEXED, never matched) must not receive the `name` column's weight.
    assert weight_values[0] != _FTS_COLUMN_WEIGHTS["name"]
    for column, weight in zip(columns, weight_values):
        expected = _FTS_COLUMN_WEIGHTS.get(column, 1.0)
        assert weight == expected
