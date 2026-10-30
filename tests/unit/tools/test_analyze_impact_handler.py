"""Unit tests for the `analyze_impact` MCP handler
(tools/handlers/analysis_handlers.py).

Regression guard for the final-review Important #1 finding: the MCP schema
never exposed `max_wire_hops`/`include_churn` and the handler silently
dropped them even if a caller somehow supplied them, defeating Task 16's
`include_churn` toggle (the one knob that controls whether a `git`
subprocess is shelled out for churn scoring). `CodeFinder.analyze_impact`'s
own behavior for these two parameters is already covered by
test_impact_analysis.py; this file only proves the handler layer forwards
them correctly, following this codebase's existing fake-collaborator
convention for handler-adjacent tests (see test_search_cascade.py's
_FakeCodeFinder, which records every kwarg a wrapped method is called
with).
"""

from typing import Any, Dict, List

from codegraphcontext.tools.handlers.analysis_handlers import analyze_impact


class _FakeCodeFinder:
    """Stands in for CodeFinder: exposes `.analyze_impact` and records every
    call's kwargs, exactly as the `analyze_impact` handler consumes it."""

    def __init__(self):
        self.calls: List[Dict[str, Any]] = []

    def analyze_impact(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
        return {"target": {"name": kwargs.get("target"), "type": "Function", "path": ""}}


def test_analyze_impact_handler_requires_target():
    res = analyze_impact(_FakeCodeFinder())
    assert "error" in res


def test_analyze_impact_handler_forwards_max_wire_hops_and_include_churn_when_supplied():
    code_finder = _FakeCodeFinder()
    analyze_impact(
        code_finder,
        target="createOrder",
        max_wire_hops=5,
        include_churn=False,
    )
    call = code_finder.calls[0]
    assert call["max_wire_hops"] == 5
    assert call["include_churn"] is False


def test_analyze_impact_handler_defaults_max_wire_hops_and_include_churn_when_omitted():
    """Must match CodeFinder.analyze_impact's own defaults (max_wire_hops=2,
    include_churn=True) so a caller that never heard of these two new
    schema fields sees unchanged behavior."""
    code_finder = _FakeCodeFinder()
    analyze_impact(code_finder, target="createOrder")
    call = code_finder.calls[0]
    assert call["max_wire_hops"] == 2
    assert call["include_churn"] is True


def test_analyze_impact_handler_still_forwards_existing_params_alongside_the_new_ones():
    """Regression guard: adding the two new kwargs must not accidentally
    drop or rename any of the pre-existing forwarded parameters."""
    code_finder = _FakeCodeFinder()
    analyze_impact(
        code_finder,
        target="AuthService",
        target_type="class",
        repo_path="/repos/special-order-services",
        depth=4,
        direction="both",
    )
    call = code_finder.calls[0]
    assert call["target"] == "AuthService"
    assert call["target_type"] == "class"
    assert call["repo_path"] == "/repos/special-order-services"
    assert call["depth"] == 4
    assert call["direction"] == "both"
    # defaults for the two newly-exposed params still apply
    assert call["max_wire_hops"] == 2
    assert call["include_churn"] is True


def test_analyze_impact_handler_returns_success_envelope():
    code_finder = _FakeCodeFinder()
    res = analyze_impact(code_finder, target="createOrder")
    assert res["success"] is True
    assert res["results"]["target"]["name"] == "createOrder"
