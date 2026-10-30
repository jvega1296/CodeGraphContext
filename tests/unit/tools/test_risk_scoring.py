"""Tests for the core pure risk-scoring factors ported from code-review-graph."""

from codegraphcontext.tools.risk_scoring import (
    caller_count_score,
    community_crossing_score,
    security_sensitivity_score,
)


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
