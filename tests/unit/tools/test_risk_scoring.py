"""Tests for the core pure risk-scoring factors ported from code-review-graph."""

import subprocess
from typing import Any, Dict, List, Optional

import pytest

from codegraphcontext.tools.risk_scoring import (
    CHURN_OK,
    CHURN_UNAVAILABLE,
    caller_count_score,
    churn_score,
    clear_churn_cache,
    community_crossing_score,
    compute_file_churn_with_status,
    find_test_coverage_count,
    qualified_name_proxy,
    security_sensitivity_score,
)
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
