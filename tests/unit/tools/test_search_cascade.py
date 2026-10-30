"""Unit tests for the 3-tier search cascade orchestrator (Piece 5, Task 21).

Follows this codebase's hand-rolled fake-session/driver convention (see
test_search_semantic.py's _FakeSession/_FakeResult and
test_analyze_change_risk.py's context-manager-capable _FakeSession), with
one deliberate exception: `pytest-mock`'s `mocker` fixture is used for the
"didn't fall through" assertion class (proving `fts5_search`/
`semantic_search` were *not* called), which is awkward to express with pure
hand-written stubs. Every other assertion in this file stays on the
hand-rolled convention.

`_fts5_conn` is also patched in every test that reaches the FTS5 branch:
left unmocked, it would call the real `resolve_context()` (environment- and
cwd-dependent, and capable of creating a real `.codegraphcontext/` directory
as a side effect in per-repo mode) purely to produce a connection object
that the mocked `fts5_search` never actually touches. Patching it keeps
these tests deterministic and side-effect-free without weakening any
assertion the brief specifies.
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest

from codegraphcontext.tools.search_cascade import _hydrate, search_code_cascade


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def data(self) -> List[Dict[str, Any]]:
        return self._rows


class _FakeSession:
    """Context-manager-capable fake session (search_code_cascade uses
    `with code_finder.driver.session() as session:`).
    """

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None):
        self._rows = rows or []
        self.queries: List[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def run(self, query: str, **kwargs: Any) -> _FakeResult:
        self.queries.append(query)
        return _FakeResult(self._rows)


class _FakeDriver:
    def __init__(self, session: _FakeSession):
        self._session = session

    def session(self) -> _FakeSession:
        return self._session


class _FakeCodeFinder:
    """Stands in for CodeFinder: exposes `.find_related_code` and `.driver`
    exactly as `search_code_cascade` consumes them.
    """

    def __init__(self, find_related_code_result: Dict[str, Any], session: Optional[_FakeSession] = None):
        self._find_related_code_result = find_related_code_result
        self.driver = _FakeDriver(session or _FakeSession())
        self.find_related_code_calls: List[Dict[str, Any]] = []

    def find_related_code(self, user_query, fuzzy_search, edit_distance, repo_path=None, graph_name=None):
        self.find_related_code_calls.append(
            {
                "user_query": user_query,
                "fuzzy_search": fuzzy_search,
                "edit_distance": edit_distance,
                "repo_path": repo_path,
                "graph_name": graph_name,
            }
        )
        return self._find_related_code_result


def _exact_match_result(query: str) -> Dict[str, Any]:
    """Mirrors find_related_code's real return shape (code_finder.py) on a hit."""
    match = {
        "name": "createOrder",
        "path": "/repo/orders.py",
        "line_number": 10,
        "source": "def createOrder(): ...",
        "docstring": None,
        "is_dependency": False,
        "search_type": "function_name",
        "relevance_score": 0.9,
    }
    return {
        "query": query,
        "functions_by_name": [match],
        "classes_by_name": [],
        "variables_by_name": [],
        "content_matches": [],
        "ranked_results": [match],
        "total_matches": 1,
    }


def _no_match_result(query: str) -> Dict[str, Any]:
    """Mirrors find_related_code's real return shape on a true miss: every
    key is still present (never an empty/falsy dict), just with empty lists
    and total_matches == 0 -- the exact bug the brief flagged in the
    original pseudocode's `if exact:` check.
    """
    return {
        "query": query,
        "functions_by_name": [],
        "classes_by_name": [],
        "variables_by_name": [],
        "content_matches": [],
        "ranked_results": [],
        "total_matches": 0,
    }


@pytest.fixture
def fake_code_finder_with_exact_match() -> _FakeCodeFinder:
    return _FakeCodeFinder(_exact_match_result("createOrder"))


@pytest.fixture
def fake_code_finder_with_no_exact_match() -> _FakeCodeFinder:
    return _FakeCodeFinder(_no_match_result("create order flow"))


# ---------------------------------------------------------------------------
# Step 1: exact tier found -> no fallthrough, no added latency
# ---------------------------------------------------------------------------


def test_search_code_cascade_stops_at_exact_tier_when_found(mocker, fake_code_finder_with_exact_match):
    fts5_spy = mocker.patch("codegraphcontext.tools.search_cascade.fts5_search")
    semantic_spy = mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")
    res = search_code_cascade(fake_code_finder_with_exact_match, "createOrder")
    assert res["tier_used"] == "exact"
    fts5_spy.assert_not_called()
    semantic_spy.assert_not_called()


def test_search_code_cascade_exact_tier_returns_ranked_results_and_query(fake_code_finder_with_exact_match):
    res = search_code_cascade(fake_code_finder_with_exact_match, "createOrder")
    assert res["query"] == "createOrder"
    assert res["results"] == [
        {
            "name": "createOrder",
            "path": "/repo/orders.py",
            "line_number": 10,
            "source": "def createOrder(): ...",
            "docstring": None,
            "is_dependency": False,
            "search_type": "function_name",
            "relevance_score": 0.9,
        }
    ]


def test_search_code_cascade_exact_tier_does_not_open_a_session(fake_code_finder_with_exact_match):
    """Anti-goal: no added latency on the fast/exact path -- a session for
    FTS5/semantic must never be opened when the exact tier already found
    something.
    """
    search_code_cascade(fake_code_finder_with_exact_match, "createOrder")
    assert fake_code_finder_with_exact_match.driver._session.queries == []


def test_search_code_cascade_passes_edit_distance_to_find_related_code(fake_code_finder_with_exact_match):
    """Regression guard for the brief's Bug 1: find_related_code's
    `edit_distance` has no default and must always be supplied.
    """
    search_code_cascade(fake_code_finder_with_exact_match, "createOrder")
    call = fake_code_finder_with_exact_match.find_related_code_calls[0]
    assert call["fuzzy_search"] is False
    assert call["edit_distance"] == 2
    assert call["repo_path"] is None


def test_search_code_cascade_exact_tier_respects_limit():
    many_matches = [
        {"name": f"fn{i}", "path": "/repo/x.py", "relevance_score": 1.0 - i / 100}
        for i in range(5)
    ]
    result = {
        "query": "fn",
        "functions_by_name": many_matches,
        "classes_by_name": [],
        "variables_by_name": [],
        "content_matches": [],
        "ranked_results": many_matches,
        "total_matches": len(many_matches),
    }
    code_finder = _FakeCodeFinder(result)
    res = search_code_cascade(code_finder, "fn", limit=2)
    assert len(res["results"]) == 2
    assert res["results"] == many_matches[:2]


# ---------------------------------------------------------------------------
# Step 5: exact tier empty -> falls through to FTS5, stops there
# ---------------------------------------------------------------------------


def test_search_code_cascade_falls_through_to_fts5_when_exact_finds_nothing(mocker, fake_code_finder_with_no_exact_match):
    mocker.patch("codegraphcontext.tools.search_cascade._fts5_conn", return_value=None)
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[("uid-1", 2.5)])
    semantic_spy = mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")
    res = search_code_cascade(fake_code_finder_with_no_exact_match, "create order flow")
    assert res["tier_used"] == "fts5"
    semantic_spy.assert_not_called()


def test_search_code_cascade_fts5_tier_hydrates_results(mocker):
    session = _FakeSession(rows=[{"n": {"uid": "uid-1", "name": "createOrder", "path": "/repo/orders.py", "_label": "Function"}}])
    code_finder = _FakeCodeFinder(_no_match_result("create order flow"), session=session)
    mocker.patch("codegraphcontext.tools.search_cascade._fts5_conn", return_value=None)
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[("uid-1", 2.5)])
    mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")

    res = search_code_cascade(code_finder, "create order flow")

    assert res["tier_used"] == "fts5"
    assert res["results"] == [
        {"uid": "uid-1", "name": "createOrder", "path": "/repo/orders.py", "type": "Function", "relevance_score": 2.5}
    ]


# ---------------------------------------------------------------------------
# fts5 empty -> falls through to semantic tier
# ---------------------------------------------------------------------------


def test_search_code_cascade_falls_through_to_semantic_when_fts5_finds_nothing(mocker):
    session = _FakeSession(rows=[{"n": {"uid": "uid-2", "name": "placeOrder", "path": "/repo/place.py", "_label": "Function"}}])
    code_finder = _FakeCodeFinder(_no_match_result("order placement flow"), session=session)
    mocker.patch("codegraphcontext.tools.search_cascade._fts5_conn", return_value=None)
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[])
    fake_embedder = object()
    mocker.patch("codegraphcontext.tools.search_cascade._get_embedder", return_value=fake_embedder)
    semantic_spy = mocker.patch(
        "codegraphcontext.tools.search_cascade.semantic_search",
        return_value=[("uid-2", 0.83)],
    )

    res = search_code_cascade(code_finder, "order placement flow")

    assert res["tier_used"] == "semantic"
    semantic_spy.assert_called_once()
    assert res["results"] == [
        {"uid": "uid-2", "name": "placeOrder", "path": "/repo/place.py", "type": "Function", "relevance_score": 0.83}
    ]


# ---------------------------------------------------------------------------
# Every tier empty -> tier_used="none"
# ---------------------------------------------------------------------------


def test_search_code_cascade_returns_none_tier_when_every_tier_finds_nothing(mocker):
    code_finder = _FakeCodeFinder(_no_match_result("totally unmatched query"))
    mocker.patch("codegraphcontext.tools.search_cascade._fts5_conn", return_value=None)
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[])
    mocker.patch("codegraphcontext.tools.search_cascade._get_embedder", return_value=object())
    mocker.patch("codegraphcontext.tools.search_cascade.semantic_search", return_value=[])

    res = search_code_cascade(code_finder, "totally unmatched query")

    assert res == {"results": [], "tier_used": "none", "query": "totally unmatched query"}


# ---------------------------------------------------------------------------
# Embedder unavailable (fastembed/sentence-transformers not installed):
# must degrade to tier_used="none", never raise ImportError out of the tool.
# ---------------------------------------------------------------------------


def test_search_code_cascade_handles_missing_embedding_backend_gracefully(mocker):
    code_finder = _FakeCodeFinder(_no_match_result("needs semantic tier"))
    mocker.patch("codegraphcontext.tools.search_cascade._fts5_conn", return_value=None)
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[])
    mocker.patch(
        "codegraphcontext.tools.search_cascade._get_embedder",
        side_effect=ImportError("fastembed package required: pip install fastembed"),
    )
    semantic_spy = mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")

    res = search_code_cascade(code_finder, "needs semantic tier")

    assert res["tier_used"] == "none"
    assert res["results"] == []
    semantic_spy.assert_not_called()


# ---------------------------------------------------------------------------
# _hydrate unit tests
# ---------------------------------------------------------------------------


def test_hydrate_returns_empty_list_for_empty_pairs():
    assert _hydrate(_FakeSession(), []) == []


def test_hydrate_preserves_tier_order_and_attaches_score():
    session = _FakeSession(
        rows=[
            {"n": {"uid": "uid-b", "name": "b", "path": "/b.py", "_label": "Function"}},
            {"n": {"uid": "uid-a", "name": "a", "path": "/a.py", "_label": "Function"}},
        ]
    )
    # Tier order is uid-a then uid-b, even though the Cypher match returned
    # uid-b first -- `IN` makes no row-order promise, so _hydrate must
    # reorder to the tier's own ranking rather than the Cypher result order.
    pairs: List[Tuple[str, float]] = [("uid-a", 9.0), ("uid-b", 1.0)]
    result = _hydrate(session, pairs)
    assert [r["uid"] for r in result] == ["uid-a", "uid-b"]
    assert result[0]["relevance_score"] == 9.0
    assert result[1]["relevance_score"] == 1.0


def test_hydrate_skips_stale_uids_not_found_in_the_graph():
    session = _FakeSession(rows=[{"n": {"uid": "uid-a", "name": "a", "path": "/a.py", "_label": "Function"}}])
    result = _hydrate(session, [("uid-a", 1.0), ("uid-deleted", 0.5)])
    assert len(result) == 1
    assert result[0]["uid"] == "uid-a"
