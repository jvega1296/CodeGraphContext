"""Unit tests for wire.terraform_stitcher (Piece 3, Task 12)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from codegraphcontext.wire.terraform_stitcher import (
    StitchStats,
    stitch_topics_to_terraform,
)


# ── Test doubles ────────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def single(self) -> Optional[Dict[str, Any]]:
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    """Dispatches canned responses by substring-matching the Cypher text --
    the same style test_impact_analysis.py's _FakeSession already uses,
    since stitch_topics_to_terraform issues several distinct statements
    rather than one."""

    def __init__(self, recorder: Dict[str, Any], responses: Dict[str, Any]):
        self._recorder = recorder
        self._recorder.setdefault("queries", [])
        self._responses = responses

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, query: str, **params: Any) -> _FakeResult:
        self._recorder["queries"].append({"query": query, "params": params})

        if "WHERE c.topic = t.name" in query:
            return _FakeResult(self._responses.get("phase1_match", [{"matched": 0}]))

        if "NOT (t)-[:BACKED_BY]->()" in query:
            return _FakeResult(self._responses.get("unmatched_topics", []))

        if "c.pull_subscriptions" in query:
            return _FakeResult(self._responses.get("candidates", []))

        if "MERGE (s)-[:BACKED_BY]->(c)" in query:
            subscription_name = params.get("subscription_name")
            matches = self._responses.get("subscription_backed_by", {})
            matched = 1 if subscription_name in matches else 0
            return _FakeResult([{"matched": matched}])

        if "MERGE (t)-[:HAS_SUBSCRIPTION]->(s)" in query:
            subscription_name = params.get("subscription_name")
            matches = self._responses.get("has_subscription", {})
            matched = 1 if subscription_name in matches else 0
            return _FakeResult([{"matched": matched}])

        return _FakeResult([])


class _FakeDriver:
    """Minimal driver stub matching execute_write_operation's non-neo4j
    (`with driver.session() as session: return work_fn(session)`) branch --
    get_backend_type is absent so backend detection falls back to 'neo4j',
    which this fake tolerates the same way test_wire_writer.py's does
    (_FakeSession has neither execute_write nor write_transaction, so
    execute_write_operation/execute_read_operation both fall through to
    calling work_fn(session) directly)."""

    def __init__(self, responses: Dict[str, Any]):
        self.calls: Dict[str, Any] = {}
        self._responses = responses

    def session(self, *args, **kwargs):
        return _FakeSession(self.calls, self._responses)


# ── Fixtures ────────────────────────────────────────────────────────────────


def fake_driver_with_topic_and_tf_class() -> _FakeDriver:
    """A Topic node whose name matches a Terraform module's `topic`
    attribute -- phase 1 should back it."""
    return _FakeDriver({
        "phase1_match": [{"matched": 1}],
        "unmatched_topics": [],
        "candidates": [],
    })


def fake_driver_with_subscription_and_pull_subscriptions_json() -> _FakeDriver:
    """A Terraform module whose `pull_subscriptions` JSON list carries a
    `service_account`-bearing entry naming a Subscription that already
    exists in the graph (from the application-code Pub/Sub scan) -- phase 2
    should back it and also wire (Topic)-[:HAS_SUBSCRIPTION]->(Subscription)
    for the same module row."""
    return _FakeDriver({
        "phase1_match": [{"matched": 0}],
        "unmatched_topics": [],
        "candidates": [
            {
                "uid": "class_terra_pubsub_module",
                "topic": "oms-order-create-event",
                "pull_subscriptions": (
                    '[{"name": "oms-order-create-pull-subscription", '
                    '"service_account": "svc@proj.iam.gserviceaccount.com"}]'
                ),
            }
        ],
        "subscription_backed_by": {"oms-order-create-pull-subscription"},
        "has_subscription": {"oms-order-create-pull-subscription"},
    })


# ── Tests ───────────────────────────────────────────────────────────────────


def test_stitch_topics_to_terraform_matches_by_topic_name():
    driver = fake_driver_with_topic_and_tf_class()
    stats = stitch_topics_to_terraform(driver)
    assert isinstance(stats, StitchStats)
    assert stats.topics_backed == 1


def test_stitch_subscriptions_to_terraform_matches_by_service_account():
    """The Subscription<-BACKED_BY-Terraform match itself is name-only (see
    _MATCH_SUBSCRIPTION_BACKED_BY's own comment on why), but this is the
    exact reason Decision #1 added a first-class Subscription node: once
    matched, the module's service_account -- the signal the application-code
    scan could never see -- must actually be backfilled onto the node, not
    just read and discarded. Asserting only subscriptions_backed == 1 would
    pass identically whether or not service_account were read, passed, or
    used at all -- so this asserts the SET query actually carried it."""
    driver = fake_driver_with_subscription_and_pull_subscriptions_json()
    stats = stitch_topics_to_terraform(driver)
    assert stats.subscriptions_backed == 1

    backed_by_queries = [
        c for c in driver.calls["queries"]
        if "MERGE (s)-[:BACKED_BY]->(c)" in c["query"]
    ]
    assert len(backed_by_queries) == 1
    assert "SET s.service_account" in backed_by_queries[0]["query"]
    assert (
        backed_by_queries[0]["params"]["service_account"]
        == "svc@proj.iam.gserviceaccount.com"
    )


def test_unmatched_topics_are_reported_not_silently_dropped():
    driver = _FakeDriver({
        "phase1_match": [{"matched": 0}],
        "unmatched_topics": [{"name": "orphan-topic"}],
        "candidates": [],
    })
    stats = stitch_topics_to_terraform(driver)
    assert stats.unmatched_topics == ["orphan-topic"]


def test_stitch_also_wires_topic_has_subscription_edge_for_same_module_row():
    """Scope amendment: phase 2 must also MERGE (Topic)-[:HAS_SUBSCRIPTION]->
    (Subscription) for the same Terraform module row, not just the two
    separate BACKED_BY edges -- otherwise Task 7's downstream bridging query
    (PRODUCES_TO -> Topic -> HAS_SUBSCRIPTION -> Subscription <- CONSUMES_FROM)
    never matches anything that came from Terraform-only joins."""
    driver = fake_driver_with_subscription_and_pull_subscriptions_json()
    stitch_topics_to_terraform(driver)
    has_subscription_queries = [
        c for c in driver.calls["queries"]
        if "MERGE (t)-[:HAS_SUBSCRIPTION]->(s)" in c["query"]
    ]
    assert len(has_subscription_queries) == 1
    assert has_subscription_queries[0]["params"]["topic_name"] == "oms-order-create-event"
    assert (
        has_subscription_queries[0]["params"]["subscription_name"]
        == "oms-order-create-pull-subscription"
    )
