from typing import Any, Dict, List, Optional

import pytest

from codegraphcontext.tools.code_finder import (
    CodeFinder,
    _portable_node_fields,
    _portable_rel_fields,
)

class _FakeResult:
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

class _FakeSession:
    def __init__(self, recorder: Dict[str, Any]):
        self._recorder = recorder
        self._recorder["queries"] = []

    def run(self, query: str, **kwargs: Any) -> _FakeResult:
        self._recorder["queries"].append({
            "query": query,
            "params": kwargs
        })
        
        # Mock responses based on query contents
        if "MATCH (f:File)" in query:
            if kwargs.get("target") == "auth.py":
                return _FakeResult([{"uid": "file_auth", "name": "auth.py", "path": "src/auth.py", "type": "File"}])
            return _FakeResult([])
            
        elif "MATCH (f:File)-[:CONTAINS*1..2]->(c)" in query:
            return _FakeResult([
                {"uid": "func_login", "name": "login", "path": "src/auth.py", "labels": ["Function"]},
                {"uid": "class_auth", "name": "AuthService", "path": "src/auth.py", "labels": ["Class"]}
            ])
            
        elif "MATCH (c)" in query and "c.name = $target" in query:
            if kwargs.get("target") == "AuthService":
                return _FakeResult([{"uid": "class_auth", "name": "AuthService", "path": "src/auth.py", "labels": ["Class"]}])
            if kwargs.get("target") == "createOrder":
                return _FakeResult([{"uid": "func_create_order", "name": "createOrder", "path": "src/orders.py", "labels": ["Function"]}])
            if kwargs.get("target") == "publishInventoryAudit":
                return _FakeResult([{"uid": "func_publish_inventory_audit", "name": "publishInventoryAudit", "path": "src/inventory.py", "labels": ["Function"]}])
            return _FakeResult([])
            
        elif "MATCH p = (source)-[:CALLS" in query:
            # Raw nodes()/relationships() shape, as actually returned by
            # LadybugDB/KùzuDB (label spelled flat on the dict as `_LABEL`):
            # analyze_impact shapes these in Python via _portable_node_fields/
            # _portable_rel_fields, not via an unsupported Cypher comprehension.
            return _FakeResult([
                {
                    "path_nodes": [
                        {"uid": "func_main", "name": "main", "path": "src/main.py", "_LABEL": "Function"},
                        {"uid": "func_login", "name": "login", "path": "src/auth.py", "_LABEL": "Function"}
                    ],
                    "path_rels": [
                        {"_LABEL": "CALLS", "line_number": 42}
                    ]
                }
            ])

        elif "MATCH p = (target)-[:CALLS" in query:
            return _FakeResult([
                {
                    "path_nodes": [
                        {"uid": "func_create_order", "name": "createOrder", "path": "src/orders.py", "_LABEL": "Function"},
                        {"uid": "func_validate_order", "name": "validateOrder", "path": "src/orders.py", "_LABEL": "Function"}
                    ],
                    "path_rels": [
                        {"_LABEL": "CALLS", "line_number": 10}
                    ]
                }
            ])

        elif "MATCH (target)<-[r:CALLS" in query:
            return _FakeResult([{"count": 5}])

        elif "MATCH (target)-[r:CALLS" in query:
            return _FakeResult([{"count": 3}])

        elif "NOT (t)<-[:CONSUMES_FROM]-()" in query:
            # Query D (dead-end detection): only the inventory-audit frontier
            # reaches a topic with no indexed consumer at all.
            if "func_publish_inventory_audit" in kwargs.get("frontier_uids", []):
                return _FakeResult([
                    {"topic_name": "order-inv-inquiry-oms-leg-analysis-event"}
                ])
            return _FakeResult([])

        elif "HAS_SUBSCRIPTION]->(s:Subscription)<-[:CONSUMES_FROM]" in query:
            # Query A: producer -> topic -> subscription -> consumer.
            if "func_create_order" in kwargs.get("frontier_uids", []):
                return _FakeResult([
                    {
                        "producer_uid": "func_create_order",
                        "topic_name": "oms-order-create-event",
                        "subscription_name": "oms-order-create-pull-subscription",
                        "consumer_uid": "func_on_order_create",
                        "consumer_name": "onOrderCreateEvent",
                        "consumer_path": "src/oms-order-consumer/OrderCreateListener.java",
                    }
                ])
            return _FakeResult([])

        elif "(t:Topic)<-[:CONSUMES_FROM]-(consumer_fn)" in query:
            # Query B: producer -> topic -> consumer (no Subscription node).
            return _FakeResult([])

        elif "MATCH (fn)-[:SERVES]->(ep:Endpoint)" in query:
            # Query C: SERVES/INVOKES bridge through Endpoint.
            return _FakeResult([])

        return _FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

class _FakeDriver:
    def __init__(self, recorder: Dict[str, Any]):
        self._recorder = recorder

    def session(self):
        return _FakeSession(self._recorder)

class _FakeDBManager:
    def __init__(self, recorder: Dict[str, Any]):
        self._recorder = recorder

    def get_driver(self, graph_name=None):
        return _FakeDriver(self._recorder)

    def get_backend_type(self) -> str:
        return "kuzudb"

def _make_finder() -> tuple[CodeFinder, Dict[str, Any]]:
    recorder = {}
    db_manager = _FakeDBManager(recorder)
    finder = CodeFinder(db_manager)
    return finder, recorder

def test_analyze_impact_symbol_target():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="AuthService")
    
    assert res["target"]["name"] == "AuthService"
    assert res["target"]["type"] == "Class"
    assert res["risk_level"] in ("Low", "Medium", "High")
    assert res["impact_score"] > 0.0
    assert len(res["affected_nodes"]) == 1
    assert res["affected_nodes"][0]["name"] == "main"
    assert len(res["explanations"]) == 1
    assert "main" in res["explanations"][0]["text"]
    assert "calls" in res["explanations"][0]["text"]

def test_analyze_impact_file_target():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="auth.py")
    
    # Target should resolve to file details
    assert res["target"]["name"] == "auth.py"
    assert res["target"]["type"] == "File"
    assert res["impact_score"] > 0.0
    assert len(res["affected_nodes"]) == 1
    assert res["affected_nodes"][0]["name"] == "main"

def test_analyze_impact_not_found():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="NonExistentSymbol")

    assert res["impact_score"] == 0.0
    assert res["risk_level"] == "Low"
    assert "warning" in res
    assert len(res["affected_nodes"]) == 0

def test_analyze_impact_downstream_direction_reverses_query_arrow():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="AuthService", direction="downstream")
    assert res["direction"] == "downstream"
    assert len(res["propagation_paths"]) == 1
    # the canned downstream fixture row proves the reversed-arrow query ran
    assert res["propagation_paths"][0]["nodes"][-1]["name"] == "validateOrder"
    # exercises the downstream (outgoing-edge) _criticality_query fixture
    # branch, previously wired but never asserted on (count=3 -> 3*0.2=0.6)
    assert res["risk_breakdown"]["criticality_score"] == 0.6

def test_analyze_impact_downstream_crosses_pubsub_topic_to_consumer_repo():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="createOrder", direction="downstream", max_wire_hops=2)
    node_paths = {n["path"] for n in res["affected_nodes"]}
    assert any("oms-order-consumer" in p for p in node_paths), "cross-repo consumer node missing from downstream trace"
    assert res["cross_repo_hops"][0]["topic"] == "oms-order-create-event"

def test_downstream_trace_reports_unresolved_crossing_for_dead_end_topic():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="publishInventoryAudit", direction="downstream")
    assert any(c["topic"] == "order-inv-inquiry-oms-leg-analysis-event" for c in res["unresolved_crossings"])

@pytest.mark.parametrize("backend", ["neo4j", "falkordb"])
def test_portable_field_helpers_handle_real_driver_objects(backend):
    """_portable_node_fields/_portable_rel_fields must work on the actual
    objects each backend's driver returns, not just a plain dict -- a bug
    that unit tests built only on the FakeSession's raw-dict fixtures would
    never catch. Mirrors test_class_call_queries.py's
    test_path_conversion_preserves_driver_relationship_properties, which is
    what proves the duck-typed (no isinstance-guard) style these helpers
    copy actually works for these two backends."""
    if backend == "neo4j":
        from neo4j.graph import Graph, Node

        graph = Graph()
        node = Node(graph, "n0", 0, ["Function"], {"uid": "func_entry", "name": "entry", "path": "/repo/app.py"})
        rel = graph.relationship_type("CALLS")(graph, "r0", 0, {"line_number": 7})
    else:
        from falkordb.node import Node
        from falkordb.edge import Edge

        node = Node(node_id=0, labels=["Function"], properties={"uid": "func_entry", "name": "entry", "path": "/repo/app.py"})
        other = Node(node_id=1, labels=["Function"], properties={"name": "build"})
        rel = Edge(node, "CALLS", other, properties={"line_number": 7})

    assert _portable_node_fields(node) == {
        "uid": "func_entry", "name": "entry", "path": "/repo/app.py", "type": "Function",
    }
    assert _portable_rel_fields(rel) == {"type": "CALLS", "line_number": 7}
