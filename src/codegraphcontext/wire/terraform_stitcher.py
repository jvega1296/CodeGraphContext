"""Stitch wire-coupling Topic/Subscription nodes to the Terraform resources
that actually define them (Piece 3's final task, MULTI_REPO_LINKS).

Tasks 10/11 capture a Terraform HCL ``module``/``resource`` block's ``topic``
and ``pull_subscriptions`` attributes onto its ``Class`` node. Piece 1's
Pub/Sub scanner independently creates ``Topic``/``Subscription`` nodes from
application code (see :mod:`codegraphcontext.wire.pubsub_scanner` and
:mod:`codegraphcontext.wire.writer`). Neither side knows about the other at
index time -- this module joins them after both repos have been indexed,
by matching on the values each side already carries (topic name, and the
subscription names declared in a Terraform module's ``pull_subscriptions``
list).

Run via :func:`stitch_topics_to_terraform`, exposed as the
``stitch_infrastructure`` MCP tool.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class StitchStats:
    topics_backed: int = 0
    subscriptions_backed: int = 0
    unmatched_topics: List[str] = field(default_factory=list)


# ── Cypher ──────────────────────────────────────────────────────────────────

# Phase 1: Topic <-[:BACKED_BY]- Terraform module, matched by topic name.
# The comma-joined MATCH form (rather than two chained MATCH statements) is
# deliberate -- see writer.py's UNWIND/MATCH-merge comment in
# database_embedded_kuzu.py: LadybugDB's planner silently returns zero rows
# for two unrelated chained MATCH clauses, but plans the comma form correctly.
_MATCH_TOPIC_BY_TERRAFORM_MODULE = """
MATCH (t:Topic), (c:Class {node_type: "module"})
WHERE c.topic = t.name
MERGE (t)-[:BACKED_BY]->(c)
RETURN count(*) as matched
"""

# A Topic with no Terraform match is reported explicitly in
# StitchStats.unmatched_topics rather than silently left dangling with no
# indication anything was checked (the plan's anti-goal for this step).
_UNMATCHED_TOPICS = """
MATCH (t:Topic)
WHERE NOT (t)-[:BACKED_BY]->()
RETURN t.name as name
"""

# Phase 2 candidates: Terraform modules whose pull_subscriptions JSON list is
# non-trivial. Matching a Subscription.name against a JSON-encoded list's
# `name` field is awkward in pure Cypher on an embedded backend with no
# native JSON-parse function (this codebase parses JSON in Python
# everywhere else too -- see cgc_bundle.py, config_manager.py, writer.py's
# UNWIND batch handling -- there is no existing Cypher-side JSON precedent
# to follow instead), so candidates are fetched here and `pull_subscriptions`
# is `json.loads`-ed in Python, then one small MERGE is issued per matched
# (subscription_name, terraform_class_uid) pair below.
_CANDIDATE_TERRAFORM_MODULES = """
MATCH (c:Class {node_type: "module"})
WHERE c.pull_subscriptions IS NOT NULL AND c.pull_subscriptions <> "[]"
RETURN c.uid as uid, c.topic as topic, c.pull_subscriptions as pull_subscriptions
"""

# Subscription <-[:BACKED_BY]- Terraform module, matched by name alone (not
# name+project_id): the application-code Pub/Sub scan always merges
# Subscription nodes with project_id set to its own "unknown" placeholder
# (see writer.py's _UNKNOWN_PROJECT_ID), since code alone never reveals the
# real GCP project. Matching on name only here finds that same node rather
# than risking a second, differently-keyed Subscription node if a real
# project_id were used instead.
#
# Once matched, this also backfills service_account onto the Subscription
# node from the same pull_subscriptions entry -- this is new data the
# application-code scan could never have provided (see writer.py's
# _UNKNOWN_PROJECT_ID comment: "Piece 3's Terraform stitching step backfills
# ... later"), and the whole reason Decision #1 in the design spec added a
# first-class Subscription node was to let this step attach Terraform's
# "most reliable signal for who consumes this" to it. Deriving a real
# project_id from the service_account email (e.g. the project-id segment of
# name@project-id.iam.gserviceaccount.com) is deliberately NOT done here --
# that's unverified inference with no specified format, left for a later
# decision; project_id stays at its existing "unknown" placeholder.
_MATCH_SUBSCRIPTION_BACKED_BY = """
MATCH (s:Subscription {name: $subscription_name}), (c:Class {uid: $class_uid})
MERGE (s)-[:BACKED_BY]->(c)
SET s.service_account = $service_account
RETURN count(*) as matched
"""

# (Topic)-[:HAS_SUBSCRIPTION]->(Subscription) for the same Terraform module
# row that backed both nodes above -- this is the only place this edge can
# ever be created (see writer.py's _MERGE_HAS_SUBSCRIPTION comment: the
# application-code scan never knows which Topic a Subscription is bound to).
#
# This deliberately does NOT reuse writer._MERGE_HAS_SUBSCRIPTION verbatim.
# That query matches Topic on (system, name) and re-MERGEs Subscription on
# (name, project_id) -- both fields this module cannot know correctly at
# stitch time without an extra query, and guessing wrong would create a
# second, differently-keyed Topic/Subscription node pair instead of
# connecting the ones the application-code scan already made. Matching both
# sides by name alone, exactly like phase 1 and _MATCH_SUBSCRIPTION_BACKED_BY
# above, avoids that risk.
_MATCH_TOPIC_HAS_SUBSCRIPTION = """
MATCH (t:Topic {name: $topic_name}), (s:Subscription {name: $subscription_name})
MERGE (t)-[:HAS_SUBSCRIPTION]->(s)
RETURN count(*) as matched
"""


def stitch_topics_to_terraform(driver: Any, *, graph_name: Optional[str] = None) -> StitchStats:
    """Join wire-coupling ``Topic``/``Subscription`` nodes (Piece 1) to the
    Terraform ``Class`` nodes that declare them (Tasks 10/11).

    Run once after indexing both the application code repo(s) and the
    infrastructure (Terraform) repo. ``graph_name`` is accepted for parity
    with the other graph-scoped tools' driver-selection convention (the
    caller is expected to pass a ``driver`` already bound to that graph).
    """
    from codegraphcontext.tools.indexing.persistence.utils import (
        execute_read_operation,
        execute_write_operation,
        get_backend_type,
    )

    stats = StitchStats()
    backend = get_backend_type(driver)

    def _phase1(session):
        result = session.run(_MATCH_TOPIC_BY_TERRAFORM_MODULE)
        record = result.single()
        return record["matched"] if record else 0

    stats.topics_backed = execute_write_operation(driver, backend, _phase1) or 0

    def _unmatched(session):
        return [row["name"] for row in session.run(_UNMATCHED_TOPICS)]

    stats.unmatched_topics = list(execute_read_operation(driver, backend, _unmatched) or [])

    def _candidates(session):
        return [dict(row) for row in session.run(_CANDIDATE_TERRAFORM_MODULES)]

    for row in execute_read_operation(driver, backend, _candidates) or []:
        class_uid = row.get("uid")
        topic_name = row.get("topic")
        try:
            pull_subscriptions = json.loads(row.get("pull_subscriptions") or "[]")
        except (TypeError, ValueError):
            continue
        if not isinstance(pull_subscriptions, list):
            continue

        for entry in pull_subscriptions:
            subscription_name = entry.get("name") if isinstance(entry, dict) else None
            if not subscription_name:
                continue
            service_account = entry.get("service_account") if isinstance(entry, dict) else None

            def _backed_by(
                session,
                subscription_name=subscription_name,
                class_uid=class_uid,
                service_account=service_account,
            ):
                result = session.run(
                    _MATCH_SUBSCRIPTION_BACKED_BY,
                    subscription_name=subscription_name,
                    class_uid=class_uid,
                    service_account=service_account,
                )
                record = result.single()
                return record["matched"] if record else 0

            matched = execute_write_operation(driver, backend, _backed_by) or 0
            if not matched:
                continue
            stats.subscriptions_backed += 1

            if not topic_name:
                continue

            def _has_subscription(session, topic_name=topic_name, subscription_name=subscription_name):
                session.run(
                    _MATCH_TOPIC_HAS_SUBSCRIPTION,
                    topic_name=topic_name,
                    subscription_name=subscription_name,
                )

            execute_write_operation(driver, backend, _has_subscription)

    return stats


__all__ = ["StitchStats", "stitch_topics_to_terraform"]
