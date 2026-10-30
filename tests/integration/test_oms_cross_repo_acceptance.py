"""Task 22 -- literal end-to-end acceptance test (Decision #8).

Not a mocked test. Runs against the real, already-indexed shared graph in
the ``oms-acceptance-test`` named context (``cgc context create
oms-acceptance-test --database ladybugdb``), populated by indexing the four
real repos in sequence with ``MULTI_REPO_LINKS=true ENABLE_FTS5_INDEX=true``
and running ``stitch_infrastructure`` once, per the task brief's Steps 1-3.

This file assumes that indexing has already happened outside of pytest (the
brief's own Steps 1-3 are a one-time setup, not something a test re-runs on
every invocation) -- it only proves the literal acceptance criterion against
whatever is currently in that context's database. If the context or its
repos are missing, the test fails loudly rather than silently skipping, per
Decision #8: this must be run against the real repos and the real shared
database, not mocked and not skipped.
"""
import os

import pytest

from codegraphcontext.cli.config_manager import resolve_context
from codegraphcontext.core import get_database_manager
from codegraphcontext.tools.code_finder import CodeFinder


ACCEPTANCE_CONTEXT = "oms-acceptance-test"


@pytest.fixture(scope="module")
def real_ladybug_driver_from_acceptance_context():
    """The real db_manager for the oms-acceptance-test named context.

    Named ``_driver`` per the task brief's own fixture name, but actually
    returns the db_manager -- CodeFinder's constructor takes a
    DatabaseManager, not a raw driver (confirmed against CodeFinder.__init__
    at code_finder.py:523).
    """
    ctx = resolve_context(cli_context=ACCEPTANCE_CONTEXT)
    assert ctx.database == "ladybugdb", (
        f"Expected the '{ACCEPTANCE_CONTEXT}' context to use ladybugdb "
        f"(Javier's real DEFAULT_DATABASE), got '{ctx.database}' instead -- "
        "re-check 'cgc context list'."
    )

    prior_type = os.environ.get("CGC_RUNTIME_DB_TYPE")
    os.environ["CGC_RUNTIME_DB_TYPE"] = "ladybugdb"
    try:
        manager = get_database_manager(db_path=ctx.db_path)
        yield manager
    finally:
        manager.close_driver()
        if prior_type is None:
            os.environ.pop("CGC_RUNTIME_DB_TYPE", None)
        else:
            os.environ["CGC_RUNTIME_DB_TYPE"] = prior_type


@pytest.mark.integration
def test_create_order_direction_both_crosses_repos_and_reaches_terraform(
    real_ladybug_driver_from_acceptance_context,
):
    code_finder = CodeFinder(real_ladybug_driver_from_acceptance_context)
    res = code_finder.analyze_impact(target="createOrder", direction="both", max_wire_hops=2)

    node_paths = [n["path"] for n in res["nodes"]]
    assert any("special-order-services" in p for p in node_paths), (
        "the target repo itself is not represented in the merged graph -- "
        f"got paths: {node_paths}"
    )
    assert any("oms-order-consumer" in p for p in node_paths), (
        "Piece 2's downstream wire-bridging did not reach the consumer repo"
    )
    assert any("terra-projects-oms" in p for p in node_paths), (
        "Piece 3's Terraform stitching did not surface the backing module"
    )

    assert res["direction"] == "both"
    # Corrected against the real graph (Step 6 of the task brief): the brief's
    # original guess named "order-inv-inquiry-oms-leg-analysis-event" (a
    # BigQuery-sink topic) as the one expected no-consumer exception, but
    # that topic is not even reachable from createOrder within max_wire_hops=2
    # and never appears here. The topic that actually surfaces as unresolved
    # is "order-special-received-event" -- confirmed via direct grep of
    # terra-projects-oms/az-ordfl-ordrmgmt's real .tf sources that this topic
    # (and its "order-special-*"/"special-order-*" siblings) is not declared
    # in this Terraform module at all. Its real producer (special-order-services)
    # and real consumer (special-order-export-listener) both exist, but the
    # consumer is a fifth repo outside this task's literal 4-repo acceptance
    # scope, making this the same category of legitimate exception the brief
    # anticipated -- just a different topic than it guessed before the real
    # extraction gaps were fixed.
    _KNOWN_OUT_OF_SCOPE_TOPICS = {"order-special-received-event"}
    assert res["unresolved_crossings"] == [] or all(
        c.get("topic") in _KNOWN_OUT_OF_SCOPE_TOPICS
        for c in res["unresolved_crossings"]
    ), (
        "any unresolved crossing other than a known out-of-scope topic "
        f"indicates a real gap, not an expected one: {res['unresolved_crossings']}"
    )

    assert "risk_score" in res and 0.0 <= res["risk_score"] <= 1.0
