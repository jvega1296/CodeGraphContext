"""Tests for the core pure risk-scoring factors ported from code-review-graph."""

import subprocess

import pytest

from codegraphcontext.tools.risk_scoring import (
    CHURN_OK,
    CHURN_UNAVAILABLE,
    caller_count_score,
    churn_score,
    clear_churn_cache,
    community_crossing_score,
    compute_file_churn_with_status,
    security_sensitivity_score,
)


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
