"""Tests for CodeFinder.analyze_change_risk (Task 18): the diff-level,
priority-ordered risk report that orchestrates Tasks 13-17's risk_scoring.py
surface into a single MCP tool.

Follows this codebase's established fake-session convention (see
test_impact_analysis.py's _FakeSession/_FakeDriver/_FakeDBManager and
test_risk_scoring.py's _FakeNodeSession/_FakeFunctionSession): a single
_FakeSession replays each real query's own filter/shape against a small
in-memory fixture, rather than canning a fixed response, so the tests
actually exercise analyze_change_risk's wiring instead of just trusting it.

``risk_scoring.parse_diff_ranges`` is monkeypatched directly (not exercised
against a real git repository): it shells out to git, and Tasks 13-17 already
cover its own parsing/VCS-detection logic in test_risk_scoring.py. What this
file tests is what analyze_change_risk does with whatever ranges come back.
"""

from typing import Any, Dict, List, Optional

import pytest

import codegraphcontext.tools.risk_scoring as risk_scoring_module
from codegraphcontext.tools.code_finder import CodeFinder


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def data(self) -> List[Dict[str, Any]]:
        return self._rows

    def single(self) -> Optional[Dict[str, Any]]:
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    """Covers every query analyze_change_risk issues: map_changes_to_nodes'
    overlap query, _criticality_query("upstream"), _caller_paths_query and
    find_test_coverage_count (both via _compute_ported_risk_score), and the
    new _caller_name_path_query (the one-hop indirect-test-gap check).
    """

    def __init__(self, fixture: Dict[str, Any]):
        self._fixture = fixture

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def run(self, query: str, **kwargs: Any) -> _FakeResult:
        fx = self._fixture

        if "n.path ENDS WITH $file_path OR n.path = $file_path" in query:
            file_path = kwargs.get("file_path", "")
            rows = [
                row for row in fx["node_rows"]
                if row["n.path"].endswith(file_path) or row["n.path"] == file_path
            ]
            return _FakeResult(rows)

        if "MATCH (target)<-[r:" in query:
            uid = kwargs["target_uids"][0]
            return _FakeResult([{"count": fx["in_degree"].get(uid, 0)}])

        if "RETURN c.name as name, c.path as path" in query:
            uid = kwargs["target_uids"][0]
            return _FakeResult(list(fx["caller_name_paths"].get(uid, [])))

        if "RETURN c.path as path" in query:
            uid = kwargs["target_uids"][0]
            return _FakeResult([{"path": p} for p in fx["caller_paths"].get(uid, [])])

        if "MATCH (fn:Function)" in query:
            prefix = kwargs.get("test_tree_prefix", "")
            candidate_names = set(kwargs.get("candidate_names", []))
            count = sum(
                1
                for fn in fx["test_functions"]
                if fn["path"].startswith(prefix) and fn["name"] in candidate_names
            )
            return _FakeResult([{"count": count}])

        return _FakeResult([])


class _FakeDriver:
    def __init__(self, fixture: Dict[str, Any]):
        self._fixture = fixture

    def session(self):
        return _FakeSession(self._fixture)


class _FakeDBManager:
    def __init__(self, fixture: Dict[str, Any]):
        self._fixture = fixture

    def get_driver(self, graph_name=None):
        return _FakeDriver(self._fixture)

    def get_backend_type(self) -> str:
        return "kuzudb"


def _make_finder(fixture: Dict[str, Any]) -> CodeFinder:
    return CodeFinder(_FakeDBManager(fixture))


def _empty_fixture(node_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "node_rows": node_rows,
        "in_degree": {},
        "caller_paths": {},
        "caller_name_paths": {},
        "test_functions": [],
    }


def _patch_diff_ranges(monkeypatch, ranges: Dict[str, List[tuple]]) -> None:
    monkeypatch.setattr(
        risk_scoring_module,
        "parse_diff_ranges",
        lambda repo_root, base="HEAD~1", *, require_vcs=False: ranges,
    )


@pytest.fixture
def fake_finder_with_git_repo_and_changed_files(monkeypatch):
    _patch_diff_ranges(monkeypatch, {
        "src/main/java/com/az/oms/order/OrderService.java": [(1, 50)],
    })
    fixture = _empty_fixture([
        {
            "n.uid": "func_create_order",
            "n.name": "createOrder",
            "n.path": "src/main/java/com/az/oms/order/OrderService.java",
            "n.line_number": 10,
            "n.end_line": 20,
        },
    ])
    fixture["in_degree"] = {"func_create_order": 3}
    return _make_finder(fixture)


def test_analyze_change_risk_returns_summary_and_top_ten_priorities(fake_finder_with_git_repo_and_changed_files):
    finder = fake_finder_with_git_repo_and_changed_files
    res = finder.analyze_change_risk(repo_path="/r", base="HEAD~1")
    assert "summary" in res
    assert "review_priorities" in res and len(res["review_priorities"]) <= 10
    assert "test_gaps" in res and "test_gaps_total" in res
    assert "functions_truncated" in res
    assert "churn_status" in res


@pytest.fixture
def fake_finder_with_600_changed_functions(monkeypatch):
    _patch_diff_ranges(monkeypatch, {"src/main/java/Big.java": [(1, 100000)]})
    node_rows = [
        {
            "n.uid": f"func_{i}",
            "n.name": f"func{i}",
            "n.path": "src/main/java/Big.java",
            "n.line_number": 1,
            "n.end_line": 2,
        }
        for i in range(600)
    ]
    return _make_finder(_empty_fixture(node_rows))


def test_analyze_change_risk_caps_functions_and_sets_truncated_flag(fake_finder_with_600_changed_functions, monkeypatch):
    monkeypatch.setenv("CGC_MAX_CHANGED_FUNCS", "500")
    res = fake_finder_with_600_changed_functions.analyze_change_risk(repo_path="/r")
    assert res["functions_truncated"] is True
    assert len(res["changed_functions"]) == 500


@pytest.fixture
def fake_finder_with_setup_method_and_a_test_file_change(monkeypatch):
    _patch_diff_ranges(monkeypatch, {
        "src/main/java/com/az/oms/order/OrderValidator.java": [(1, 50)],
        "src/main/java/com/az/oms/order/OrderValidatorTest.java": [(1, 50)],
        "src/main/java/com/az/oms/order/OrderService.java": [(1, 50)],
    })
    node_rows = [
        # Lifecycle method -- must be exempted from the gap report even
        # though it is a changed, uncovered, non-test-file Function.
        {
            "n.uid": "u_setup",
            "n.name": "setUp",
            "n.path": "src/main/java/com/az/oms/order/OrderValidator.java",
            "n.line_number": 5,
            "n.end_line": 10,
        },
        # A JUnit-naming filename ("<Name>Test.java") outside src/test --
        # proves the filename-pattern branch of is_test_file, not just its
        # directory-based branch.
        {
            "n.uid": "u_test_helper",
            "n.name": "helperAssert",
            "n.path": "src/main/java/com/az/oms/order/OrderValidatorTest.java",
            "n.line_number": 5,
            "n.end_line": 10,
        },
        # A genuine, untested gap candidate -- confirms the two exclusions
        # above are not merely leaving test_gaps empty by coincidence.
        {
            "n.uid": "u_create_order",
            "n.name": "createOrder",
            "n.path": "src/main/java/com/az/oms/order/OrderService.java",
            "n.line_number": 5,
            "n.end_line": 10,
        },
    ]
    return _make_finder(_empty_fixture(node_rows))


def test_analyze_change_risk_excludes_lifecycle_methods_and_test_files_from_gaps(fake_finder_with_setup_method_and_a_test_file_change):
    res = fake_finder_with_setup_method_and_a_test_file_change.analyze_change_risk(repo_path="/r")
    gap_names = {g["name"] for g in res["test_gaps"]}
    assert "setUp" not in gap_names
    assert not any("Test.java" in g["file"] for g in res["test_gaps"])
    # The exclusions above aren't just an empty list: a real gap survives.
    assert gap_names == {"createOrder"}


def test_analyze_change_risk_marks_directly_tested_nodes_as_no_gap(monkeypatch):
    _patch_diff_ranges(monkeypatch, {
        "src/main/java/com/az/oms/order/OrderValidator.java": [(1, 50)],
    })
    fixture = _empty_fixture([
        {
            "n.uid": "func_validate",
            "n.name": "validateOrder",
            "n.path": "src/main/java/com/az/oms/order/OrderValidator.java",
            "n.line_number": 10,
            "n.end_line": 20,
        },
    ])
    fixture["test_functions"] = [
        {
            "name": "validateOrderTest",
            "path": "src/test/java/com/az/oms/order/OrderValidatorTest.java",
        },
    ]
    finder = _make_finder(fixture)
    res = finder.analyze_change_risk(repo_path="/r")
    assert res["test_gaps"] == []
    assert res["test_gaps_total"] == 0


def test_analyze_change_risk_classifies_indirect_coverage_via_tested_caller(monkeypatch):
    _patch_diff_ranges(monkeypatch, {
        "src/main/java/com/az/oms/order/OrderValidator.java": [(1, 50)],
    })
    fixture = _empty_fixture([
        {
            "n.uid": "func_helper",
            "n.name": "normalizeOrderId",
            "n.path": "src/main/java/com/az/oms/order/OrderValidator.java",
            "n.line_number": 10,
            "n.end_line": 20,
        },
    ])
    fixture["caller_name_paths"] = {
        "func_helper": [
            {"name": "validateOrder", "path": "src/main/java/com/az/oms/order/OrderValidator.java"},
        ],
    }
    fixture["test_functions"] = [
        {
            "name": "validateOrderTest",
            "path": "src/test/java/com/az/oms/order/OrderValidatorTest.java",
        },
    ]
    finder = _make_finder(fixture)
    res = finder.analyze_change_risk(repo_path="/r")
    assert res["test_gaps_total"] == 1
    assert res["test_gaps_indirect"] == 1
    assert res["test_gaps_uncovered"] == 0
    gap = res["test_gaps"][0]
    assert gap["coverage"] == "indirect"
    assert gap["covered_via"] == "validateOrder"
    assert gap["covered_depth"] == 1


def test_analyze_change_risk_require_vcs_propagates_change_discovery_error(monkeypatch):
    def _raise(*args, **kwargs):
        raise risk_scoring_module.ChangeDiscoveryError("git timed out")

    monkeypatch.setattr(risk_scoring_module, "parse_diff_ranges", _raise)
    finder = _make_finder(_empty_fixture([]))
    with pytest.raises(risk_scoring_module.ChangeDiscoveryError):
        finder.analyze_change_risk(repo_path="/r", require_vcs=True)


def test_analyze_change_risk_defaults_churn_off(fake_finder_with_git_repo_and_changed_files):
    res = fake_finder_with_git_repo_and_changed_files.analyze_change_risk(repo_path="/r")
    assert res["churn_status"] == risk_scoring_module.CHURN_OFF


def test_analyze_change_risk_returns_empty_dict_shape_with_no_changes(monkeypatch):
    _patch_diff_ranges(monkeypatch, {})
    finder = _make_finder(_empty_fixture([]))
    res = finder.analyze_change_risk(repo_path="/r")
    assert res["changed_functions"] == []
    assert res["review_priorities"] == []
    assert res["risk_score"] == 0.0
    assert res["test_gaps"] == []
    assert res["affected_flows"] == []
    assert res["functions_truncated"] is False
    assert res["diff_ranges_unavailable"] == ""
