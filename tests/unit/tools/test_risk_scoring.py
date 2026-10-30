"""Tests for the core pure risk-scoring factors ported from code-review-graph."""

import subprocess
from typing import Any, Dict, List, Optional

import pytest

from codegraphcontext.tools.risk_scoring import (
    CHURN_OK,
    CHURN_UNAVAILABLE,
    ChangeDiscoveryError,
    caller_count_score,
    churn_score,
    clear_churn_cache,
    community_crossing_score,
    compute_file_churn_with_status,
    find_test_coverage_count,
    map_changes_to_nodes,
    parse_diff_ranges,
    parse_git_diff_ranges,
    parse_svn_diff_ranges,
    qualified_name_proxy,
    security_sensitivity_score,
)
from codegraphcontext.tools.risk_scoring import _diff_header_path, _unquote_c_path
# `test_coverage_score` is imported locally inside the tests that use it
# (not here at module level): its name matches pytest's `test_*` collection
# prefix, so a bare top-level import would make pytest try to collect and
# run it as a test function in its own right.


@pytest.fixture(autouse=True)
def _clear_churn_cache():
    clear_churn_cache()
    yield
    clear_churn_cache()


def test_security_sensitivity_score_matches_keyword():
    assert security_sensitivity_score("validateOrder", "com.az.oms.validateOrder") == 0.20


def test_security_sensitivity_score_no_match():
    assert security_sensitivity_score("createOrder", "com.az.oms.createOrder") == 0.0


def test_caller_count_score_caps_at_point_one():
    assert caller_count_score(1000) == 0.10


def test_caller_count_score_below_cap():
    assert caller_count_score(1) == 0.05
    assert caller_count_score(0) == 0.0


def test_community_crossing_score_counts_cross_module_callers_capped_at_point_fifteen():
    target_path = "src/main/java/com/az/oms/order/OrderValidator.java"
    caller_paths = [
        "src/main/java/com/az/oms/order/OrderService.java",   # same top-level module (order) -- doesn't count
        "src/main/java/com/az/oms/shipping/ShippingService.java",  # different module -- counts
        "src/main/java/com/az/oms/payment/PaymentService.java",    # different module -- counts
        "src/main/java/com/az/oms/inventory/InventoryService.java",# different module -- counts
        "src/main/java/com/az/oms/vendor/VendorService.java",      # 4th crossing -- still counts, pre-cap
    ]
    # 4 crossings * 0.05 = 0.20, capped at 0.15
    assert community_crossing_score(target_path, caller_paths) == 0.15


def test_qualified_name_proxy_strips_source_root_and_extension():
    assert qualified_name_proxy(
        "src/main/java/com/az/oms/order/OrderValidator.java"
    ) == "com.az.oms.order.OrderValidator"


def test_qualified_name_proxy_falls_back_to_bare_filename_without_a_source_root():
    assert qualified_name_proxy("src/auth.py") == "auth"


def test_qualified_name_proxy_excludes_repo_name_noise_from_security_sensitivity():
    # The repo-name/outer-directory segment ("query-service-repo") contains a
    # SECURITY_KEYWORDS word ("query") that has nothing to do with the
    # function actually being scored -- the raw path alone would trigger a
    # false-positive match; the proxy must not leak that segment in.
    path = "query-service-repo/src/main/java/com/az/billing/InvoiceGenerator.java"
    proxy = qualified_name_proxy(path)
    assert proxy == "com.az.billing.InvoiceGenerator"
    assert "query" not in proxy.lower()
    assert security_sensitivity_score("generateInvoice", proxy) == 0.0

    # Sanity check the other direction: a keyword that is actually part of
    # the package/class identity (not just the unrelated repo-name prefix)
    # must still be detected through the same proxy.
    sibling_path = "query-service-repo/src/main/java/com/az/billing/query/QueryBuilder.java"
    assert security_sensitivity_score("generateInvoice", qualified_name_proxy(sibling_path)) == 0.20


def _init_git_repo(repo_path) -> None:
    subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repo_path, check=True, capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repo_path, check=True, capture_output=True,
    )
    (repo_path / "file.txt").write_text("hello\n")
    subprocess.run(["git", "add", "file.txt"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial commit"],
        cwd=repo_path, check=True, capture_output=True,
    )


def test_churn_cache_key_is_repo_head_window(tmp_path):
    _init_git_repo(tmp_path)

    counts1, status1 = compute_file_churn_with_status(str(tmp_path), window_days=90)
    assert status1 == CHURN_OK
    assert counts1 == {"file.txt": 1}

    real_run = subprocess.run

    def _fail_on_numstat_log(*args, **kwargs):
        argv = args[0] if args else kwargs.get("args")
        if argv and "--numstat" in argv:
            raise AssertionError("git log --numstat should not be invoked on a cache hit")
        return real_run(*args, **kwargs)

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    original = risk_scoring_module.subprocess.run
    risk_scoring_module.subprocess.run = _fail_on_numstat_log
    try:
        counts2, status2 = compute_file_churn_with_status(str(tmp_path), window_days=90)
    finally:
        risk_scoring_module.subprocess.run = original

    assert status2 == CHURN_OK
    assert counts2 == {"file.txt": 1}


def test_churn_degrades_to_unavailable_for_non_git_directory(tmp_path):
    counts, status = compute_file_churn_with_status(str(tmp_path), window_days=90)
    assert status == CHURN_UNAVAILABLE
    assert counts == {}

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("git should not be invoked again once marked unavailable")

    original = risk_scoring_module.subprocess.run
    risk_scoring_module.subprocess.run = _fail_if_called
    try:
        counts2, status2 = compute_file_churn_with_status(str(tmp_path), window_days=90)
    finally:
        risk_scoring_module.subprocess.run = original

    assert status2 == CHURN_UNAVAILABLE
    assert counts2 == {}


def test_churn_score_formula():
    assert churn_score(0) == 0.0
    assert churn_score(5) == pytest.approx(5 / 10.0 * 0.15)
    assert churn_score(10) == pytest.approx(0.15)
    assert churn_score(1000) == pytest.approx(0.15)


class _FakeFunctionResult:
    """Mirrors tests/unit/tools/test_impact_analysis.py's _FakeResult shape
    (`.data()` / `.single()` / `__iter__`) for a single aggregate row."""

    def __init__(self, data_list: List[Dict[str, Any]]):
        self._data = data_list

    def data(self) -> List[Dict[str, Any]]:
        return self._data

    def single(self) -> Optional[Dict[str, Any]]:
        if self._data:
            return self._data[0]
        return None

    def __iter__(self):
        return iter(self._data)


class _FakeFunctionSession:
    """Minimal fake session for find_test_coverage_count, matching this
    codebase's established `.run(query, **kwargs)` fake-session convention
    (see test_impact_analysis.py's _FakeSession). Rather than canning a
    fixed count, it replays the same path-prefix/name-membership filter the
    real Cypher query expresses, against a small in-memory Function-node
    fixture -- so the test actually exercises find_test_coverage_count's
    substitution logic instead of just trusting it.
    """

    def __init__(self, functions: List[Dict[str, str]]):
        self._functions = functions

    def run(self, query: str, **kwargs: Any) -> _FakeFunctionResult:
        if "MATCH (fn:Function)" in query:
            prefix = kwargs.get("test_tree_prefix", "")
            candidate_names = set(kwargs.get("candidate_names", []))
            count = sum(
                1
                for fn in self._functions
                if fn["path"].startswith(prefix) and fn["name"] in candidate_names
            )
            return _FakeFunctionResult([{"count": count}])
        return _FakeFunctionResult([])


@pytest.fixture
def fake_session_with_main_and_test_functions() -> _FakeFunctionSession:
    functions = [
        {"name": "validateOrder", "path": "src/main/java/com/az/oms/OrderValidator.java"},
        {"name": "testValidateOrder", "path": "src/test/java/com/az/oms/OrderValidatorTest.java"},
        {"name": "validateOrderTest", "path": "src/test/java/com/az/oms/OrderValidatorTests.java"},
        # Decoy A: correct directory, but the name doesn't match any
        # test-naming variant -- must not be counted.
        {"name": "helperMethod", "path": "src/test/java/com/az/oms/OrderValidatorTest.java"},
        # Decoy B: a matching test-naming variant, but under a sibling
        # package that doesn't share the target's directory prefix --
        # must not be counted.
        {"name": "testValidateOrder", "path": "src/test/java/com/az/shipping/OtherValidatorTest.java"},
    ]
    return _FakeFunctionSession(functions)


def test_find_test_coverage_count_matches_colocated_junit_naming(fake_session_with_main_and_test_functions):
    count = find_test_coverage_count(
        fake_session_with_main_and_test_functions,
        "validateOrder",
        "src/main/java/com/az/oms/OrderValidator.java",
    )
    assert count == 2  # fixture has testValidateOrder and validateOrderTest, both under src/test/java/com/az/oms/


def test_test_coverage_score_zero_tests_is_point_three():
    from codegraphcontext.tools.risk_scoring import test_coverage_score

    assert test_coverage_score(0) == pytest.approx(0.30)


def test_test_coverage_score_five_or_more_tests_floors_at_point_zero_five():
    # pytest.approx, not ==: 0.30 - 0.25 is 0.049999999999999988 in IEEE-754
    # double precision, not exactly 0.05 -- the same float-dust reason
    # test_churn_score_formula above compares with pytest.approx.
    from codegraphcontext.tools.risk_scoring import test_coverage_score

    assert test_coverage_score(5) == pytest.approx(0.05)
    assert test_coverage_score(100) == pytest.approx(0.05)


def test_unquote_c_path_decodes_octal_escapes():
    # port the exact code-review-graph test fixture value for a non-ASCII filename
    assert _unquote_c_path('"caf\\303\\251.py"') == "café.py"


def test_diff_header_path_handles_space_via_tab_separator():
    assert _diff_header_path("b/src/my file.py\t") == "src/my file.py"


def test_diff_header_path_handles_c_quoted_non_ascii_path():
    assert _diff_header_path('"b/src/caf\\303\\251.py"') == "src/café.py"


def test_diff_header_path_returns_none_for_dev_null():
    assert _diff_header_path("/dev/null") is None


def _init_git_repo_with_commit(repo_path, filename: str, content: str) -> None:
    subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=repo_path, check=True, capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test User"],
        cwd=repo_path, check=True, capture_output=True,
    )
    (repo_path / filename).write_text(content)
    subprocess.run(["git", "add", filename], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial commit"],
        cwd=repo_path, check=True, capture_output=True,
    )


def test_parse_git_diff_ranges_rejects_unsafe_ref(tmp_path):
    ranges = parse_git_diff_ranges(str(tmp_path), base="HEAD; rm -rf /")
    assert ranges == {}


def test_parse_git_diff_ranges_rejects_unsafe_ref_even_with_require_vcs():
    # The ref-format guard rejects before any subprocess is attempted, so
    # this is a format rejection, not a VCS-unavailable condition --
    # require_vcs must not turn it into a raised ChangeDiscoveryError.
    ranges = parse_git_diff_ranges("/nonexistent", base="HEAD; rm -rf /", require_vcs=True)
    assert ranges == {}


def test_parse_svn_diff_ranges_rejects_unsafe_revision(tmp_path):
    ranges = parse_svn_diff_ranges(str(tmp_path), rev_range="100:HEAD; rm -rf /")
    assert ranges == {}


def test_parse_git_diff_ranges_extracts_changed_line_ranges(tmp_path):
    _init_git_repo_with_commit(tmp_path, "file.txt", "line1\nline2\nline3\n")
    (tmp_path / "file.txt").write_text("line1\nCHANGED\nline3\nline4\n")
    subprocess.run(["git", "add", "file.txt"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "second commit"],
        cwd=tmp_path, check=True, capture_output=True,
    )
    ranges = parse_git_diff_ranges(str(tmp_path), base="HEAD~1")
    assert "file.txt" in ranges
    assert ranges["file.txt"]


def test_parse_git_diff_ranges_raises_when_require_vcs_and_git_missing(tmp_path, monkeypatch):
    def _raise_missing(*args, **kwargs):
        raise FileNotFoundError("git not found")

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    monkeypatch.setattr(risk_scoring_module.subprocess, "run", _raise_missing)
    with pytest.raises(ChangeDiscoveryError):
        parse_git_diff_ranges(str(tmp_path), base="HEAD~1", require_vcs=True)


def test_parse_git_diff_ranges_returns_empty_without_require_vcs_when_git_missing(tmp_path, monkeypatch):
    def _raise_missing(*args, **kwargs):
        raise FileNotFoundError("git not found")

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    monkeypatch.setattr(risk_scoring_module.subprocess, "run", _raise_missing)
    assert parse_git_diff_ranges(str(tmp_path), base="HEAD~1") == {}


def test_parse_diff_ranges_detects_svn_via_dot_svn_directory(tmp_path, monkeypatch):
    (tmp_path / ".svn").mkdir()
    sentinel = {"file.py": [(1, 2)]}
    calls = []

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    def _fake_svn(repo_root, rev_range=None, *, require_vcs=False):
        calls.append("svn")
        return sentinel

    def _fake_git(repo_root, base="HEAD~1", *, require_vcs=False):
        calls.append("git")
        return {}

    monkeypatch.setattr(risk_scoring_module, "parse_svn_diff_ranges", _fake_svn)
    monkeypatch.setattr(risk_scoring_module, "parse_git_diff_ranges", _fake_git)

    result = risk_scoring_module.parse_diff_ranges(str(tmp_path))
    assert result == sentinel
    assert calls == ["svn"]


def test_parse_diff_ranges_dispatches_to_git_without_dot_svn_directory(tmp_path, monkeypatch):
    calls = []

    import codegraphcontext.tools.risk_scoring as risk_scoring_module

    def _fake_svn(repo_root, rev_range=None, *, require_vcs=False):
        calls.append("svn")
        return {}

    def _fake_git(repo_root, base="HEAD~1", *, require_vcs=False):
        calls.append("git")
        return {}

    monkeypatch.setattr(risk_scoring_module, "parse_svn_diff_ranges", _fake_svn)
    monkeypatch.setattr(risk_scoring_module, "parse_git_diff_ranges", _fake_git)

    risk_scoring_module.parse_diff_ranges(str(tmp_path))
    assert calls == ["git"]


class _FakeNodeResult:
    """Mirrors _FakeFunctionResult's shape (`.data()` / `.single()` /
    `__iter__`) for a list of Function/Class node rows."""

    def __init__(self, data_list: List[Dict[str, Any]]):
        self._data = data_list

    def data(self) -> List[Dict[str, Any]]:
        return self._data

    def single(self) -> Optional[Dict[str, Any]]:
        if self._data:
            return self._data[0]
        return None

    def __iter__(self):
        return iter(self._data)


class _FakeNodeSession:
    """Minimal fake session for map_changes_to_nodes, matching this
    codebase's established `.run(query, **kwargs)` fake-session convention.
    """

    def __init__(self, nodes_by_path: Dict[str, List[Dict[str, Any]]]):
        self._nodes_by_path = nodes_by_path

    def run(self, query: str, **kwargs: Any) -> _FakeNodeResult:
        if "n:Function OR n:Class" in query:
            file_path = kwargs.get("file_path")
            return _FakeNodeResult(self._nodes_by_path.get(file_path, []))
        return _FakeNodeResult([])


@pytest.fixture
def fake_session_with_one_function_lines_10_to_30() -> _FakeNodeSession:
    return _FakeNodeSession({
        "src/main/java/Foo.java": [
            {
                "n.uid": "func_foo",
                "n.name": "foo",
                "n.path": "src/main/java/Foo.java",
                "n.line_number": 10,
                "n.end_line": 30,
            },
        ],
    })


def test_map_changes_to_nodes_overlap_check(fake_session_with_one_function_lines_10_to_30):
    nodes = map_changes_to_nodes(fake_session_with_one_function_lines_10_to_30, {"src/main/java/Foo.java": [(25, 35)]})
    assert len(nodes) == 1  # 25-35 overlaps 10-30


def test_map_changes_to_nodes_excludes_non_overlapping_range(fake_session_with_one_function_lines_10_to_30):
    nodes = map_changes_to_nodes(fake_session_with_one_function_lines_10_to_30, {"src/main/java/Foo.java": [(50, 60)]})
    assert nodes == []


def test_map_changes_to_nodes_handles_multiple_ranges_and_files():
    session = _FakeNodeSession({
        "a.py": [
            {"n.uid": "func_a", "n.name": "a", "n.path": "a.py", "n.line_number": 1, "n.end_line": 5},
        ],
        "b.py": [
            {"n.uid": "func_b", "n.name": "b", "n.path": "b.py", "n.line_number": 100, "n.end_line": 120},
        ],
    })
    nodes = map_changes_to_nodes(session, {"a.py": [(1, 2)], "b.py": [(200, 210), (110, 115)]})
    uids = {n["uid"] for n in nodes}
    assert uids == {"func_a", "func_b"}
