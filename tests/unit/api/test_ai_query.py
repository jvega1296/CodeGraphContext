import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from codegraphcontext.viz.server import app, set_db_manager

client = TestClient(app)

def test_ai_query_no_api_keys(monkeypatch):
    # Ensure no supported provider key is in env
    monkeypatch.delenv("ANTHROPIC_VERTEX_PROJECT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLASCLOUD_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("CHEAPER_INFERENCE_API_KEY", raising=False)
    monkeypatch.delenv("API_ROUTE_API_KEY", raising=False)
    
    # Mock db_manager
    mock_db = MagicMock()
    set_db_manager(mock_db)
    
    response = client.post("/api/ai_query", json={"query": "test query"})
    
    assert response.status_code == 400
    assert "AI querying requires" in response.json()["detail"]


@patch("codegraphcontext.viz.server.requests.post")
def test_call_llm_with_atlascloud(mock_post, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_VERTEX_PROJECT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("ATLASCLOUD_API_KEY", "fake-atlas-key")
    monkeypatch.setenv("ATLASCLOUD_API_BASE", "https://api.atlascloud.ai/v1/")
    monkeypatch.setenv("ATLASCLOUD_MODEL", "qwen/qwen3.5-flash")

    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        "choices": [{"message": {"content": "MATCH (n) RETURN n"}}]
    }

    from codegraphcontext.viz.server import call_llm

    result = call_llm("system prompt", "user prompt")

    assert result == "MATCH (n) RETURN n"
    mock_post.assert_called_once_with(
        "https://api.atlascloud.ai/v1/chat/completions",
        json={
            "model": "qwen/qwen3.5-flash",
            "messages": [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "user prompt"},
            ],
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer fake-atlas-key",
        },
        timeout=30,
    )


@patch("codegraphcontext.viz.server.requests.post")
def test_call_llm_with_atlas_cloud_alias_defaults(mock_post, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_VERTEX_PROJECT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLASCLOUD_API_KEY", raising=False)
    monkeypatch.delenv("ATLASCLOUD_API_BASE", raising=False)
    monkeypatch.delenv("ATLASCLOUD_MODEL", raising=False)
    monkeypatch.setenv("ATLAS_CLOUD_API_KEY", "fake-atlas-key")

    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        "choices": [{"message": {"content": "default response"}}]
    }

    from codegraphcontext.viz.server import call_llm

    assert call_llm("system", "user") == "default response"
    _, kwargs = mock_post.call_args
    assert mock_post.call_args.args[0] == "https://api.atlascloud.ai/v1/chat/completions"
    assert kwargs["json"]["model"] == "deepseek-ai/deepseek-v4-pro"


@patch("codegraphcontext.viz.server.requests.post")
def test_call_llm_with_cheaperinference(mock_post, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_VERTEX_PROJECT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLASCLOUD_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_CLOUD_API_KEY", raising=False)
    monkeypatch.setenv("CHEAPER_INFERENCE_API_KEY", "fake-ci-key")
    monkeypatch.setenv("CHEAPER_INFERENCE_API_BASE", "https://api.cheaperinference.com/v1/")
    monkeypatch.setenv("CHEAPER_INFERENCE_MODEL", "claude-sonnet-5")

    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        "choices": [{"message": {"content": "MATCH (n) RETURN n"}}]
    }

    from codegraphcontext.viz.server import call_llm

    result = call_llm("system prompt", "user prompt")

    assert result == "MATCH (n) RETURN n"
    mock_post.assert_called_once_with(
        "https://api.cheaperinference.com/v1/chat/completions",
        json={
            "model": "claude-sonnet-5",
            "messages": [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "user prompt"},
            ],
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer fake-ci-key",
        },
        timeout=30,
    )


@patch("codegraphcontext.viz.server.requests.post")
def test_call_llm_with_cheaperinference_defaults(mock_post, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_VERTEX_PROJECT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ATLASCLOUD_API_KEY", raising=False)
    monkeypatch.delenv("ATLAS_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("CHEAPER_INFERENCE_API_BASE", raising=False)
    monkeypatch.delenv("CHEAPER_INFERENCE_MODEL", raising=False)
    monkeypatch.setenv("CHEAPER_INFERENCE_API_KEY", "fake-ci-key")

    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {
        "choices": [{"message": {"content": "default response"}}]
    }

    from codegraphcontext.viz.server import call_llm

    assert call_llm("system", "user") == "default response"
    _, kwargs = mock_post.call_args
    assert mock_post.call_args.args[0] == "https://api.cheaperinference.com/v1/chat/completions"
    assert kwargs["json"]["model"] == "gpt-5.4-mini"


@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_success(mock_call_llm, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"cypher_query": "MATCH (n) RETURN n", "explanation": "translating test"}',
        "This is the explanation of the results."
    ]

    mock_db = MagicMock()
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_result = MagicMock()

    mock_record = MagicMock()
    mock_node_dict = {"_label": "Class", "name": "TestClass", "path": "/path/to/test"}
    mock_record.values.return_value = [mock_node_dict]
    mock_record.items.return_value = [("n", mock_node_dict)]
    mock_record.get.return_value = mock_node_dict

    mock_result.__iter__.return_value = [mock_record]

    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__enter__.return_value = mock_session
    mock_db.get_driver.return_value = mock_driver
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "test query"})

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    assert res_data["cypher_query"] == "MATCH (n) RETURN n"
    assert res_data["explanation"] == "This is the explanation of the results."
    assert len(res_data["nodes"]) > 0
    assert res_data["nodes"][0]["name"] == "TestClass"


@patch("codegraphcontext.tools.code_finder.CodeFinder")
@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_flow_mode_happy_path(mock_call_llm, mock_code_finder_cls, monkeypatch):
    """Flow mode routes through analyze_impact and maps nodes/links correctly."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "flow", "target": "createOrder", "direction": "both", "explanation": "tracing flow"}',
        "The createOrder process works as follows..."
    ]

    # analyze_impact returns direction="both" shape: nodes + edges
    mock_impact_result = {
        "target": {"name": "createOrder", "type": "Function", "path": "/svc/order.java"},
        "direction": "both",
        "nodes": [
            {"uid": "uid-1", "name": "createOrder", "path": "/svc/order.java", "type": "Function", "role": "target"},
            {"uid": "uid-2", "name": "OrderTopic", "path": "", "type": "Topic", "role": "downstream"},
            {"uid": "uid-3", "name": "handleOrder", "path": "/svc/consumer.java", "type": "Function", "role": "downstream"},
        ],
        "edges": [
            {"source_uid": "uid-1", "target_uid": "uid-2", "type": "PRODUCES_TO", "line_number": 42, "direction": "downstream"},
            {"source_uid": "uid-2", "target_uid": "uid-3", "type": "CONSUMES_FROM", "line_number": 0, "direction": "downstream"},
        ],
        "cross_repo_hops": [],
        "unresolved_crossings": [],
        "impact_score": 3.0,
        "risk_level": "Low",
        "risk_breakdown": {"depth_score": 1.0, "module_score": 1.0, "criticality_score": 1.0, "churn_score": 0.0},
        "risk_score": 2.5,
        "risk_factors": {"security_sensitivity": 0.0, "caller_count": 0.0, "community_crossing": 0.0, "test_coverage": 0.0, "churn": 0.0},
        "churn_status": "off",
        "explanations": [],
    }

    mock_finder_instance = MagicMock()
    mock_finder_instance.analyze_impact.return_value = mock_impact_result
    mock_code_finder_cls.return_value = mock_finder_instance

    mock_db = MagicMock()
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "how does createOrder work"})

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    assert "createOrder" in res_data["cypher_query"]
    assert "both" in res_data["cypher_query"]
    assert res_data["translation_explanation"] == "tracing flow"
    assert res_data["explanation"] == "The createOrder process works as follows..."

    # Verify analyze_impact was called with correct args
    mock_finder_instance.analyze_impact.assert_called_once_with(
        target="createOrder",
        direction="both",
        max_wire_hops=2,
    )

    # Nodes should be mapped to frontend shape
    assert len(res_data["nodes"]) == 3
    node_by_uid = {n["id"]: n for n in res_data["nodes"]}

    # Function node -> val=2
    fn_node = node_by_uid["uid-1"]
    assert fn_node["name"] == "createOrder"
    assert fn_node["type"] == "Function"
    assert fn_node["val"] == 2

    # Topic node -> val=4
    topic_node = node_by_uid["uid-2"]
    assert topic_node["name"] == "OrderTopic"
    assert topic_node["type"] == "Topic"
    assert topic_node["val"] == 4

    # Links should be mapped correctly
    assert len(res_data["links"]) == 2
    link_ids = {lnk["id"] for lnk in res_data["links"]}
    assert "uid-1->uid-2:PRODUCES_TO" in link_ids
    assert "uid-2->uid-3:CONSUMES_FROM" in link_ids
    for lnk in res_data["links"]:
        assert set(lnk.keys()) == {"id", "source", "target", "type"}


@patch("codegraphcontext.tools.code_finder.CodeFinder")
@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_flow_mode_warning(mock_call_llm, mock_code_finder_cls, monkeypatch):
    """Flow mode surfaces analyze_impact warning as explanation when target is not found."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "flow", "target": "nonExistentSymbol", "direction": "both", "explanation": "tracing flow"}',
    ]

    mock_impact_result = {
        "target": {"name": "nonExistentSymbol", "type": "Unknown", "path": ""},
        "direction": "both",
        "nodes": [],
        "edges": [],
        "warning": "No code symbol or file named 'nonExistentSymbol' was found in the graph.",
        "cross_repo_hops": [],
        "unresolved_crossings": [],
        "impact_score": 0.0,
        "risk_level": "Low",
    }

    mock_finder_instance = MagicMock()
    mock_finder_instance.analyze_impact.return_value = mock_impact_result
    mock_code_finder_cls.return_value = mock_finder_instance

    mock_db = MagicMock()
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "how does nonExistentSymbol work"})

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    # Warning becomes the explanation -- LLM explanation call is skipped
    expected_warning = "No code symbol or file named 'nonExistentSymbol' was found in the graph."
    assert res_data["explanation"] == expected_warning
    assert res_data["nodes"] == []
    assert res_data["links"] == []
    # Only one LLM call (translation); no second call for explanation when warning
    assert mock_call_llm.call_count == 1


@patch("codegraphcontext.tools.code_finder.CodeFinder")
@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_flow_mode_val_types(mock_call_llm, mock_code_finder_cls, monkeypatch):
    """Flow mode maps val=4 for Repository/Class/Interface/Trait/Topic/Subscription/Endpoint, val=2 for others."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "flow", "target": "someTarget", "direction": "both", "explanation": ""}',
        "explanation text",
    ]

    val4_types = ["Repository", "Class", "Interface", "Trait", "Topic", "Subscription", "Endpoint"]
    val2_types = ["Function", "File", "Module", "Struct", "Enum", "Other"]

    nodes = [
        {"uid": f"uid-{i}", "name": t, "path": "", "type": t, "role": "downstream"}
        for i, t in enumerate(val4_types + val2_types)
    ]

    mock_finder_instance = MagicMock()
    mock_finder_instance.analyze_impact.return_value = {
        "target": {"name": "someTarget", "type": "Function", "path": ""},
        "direction": "both",
        "nodes": nodes,
        "edges": [],
        "cross_repo_hops": [],
        "risk_level": "Low",
        "explanations": [],
    }
    mock_code_finder_cls.return_value = mock_finder_instance
    mock_db = MagicMock()
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "how does someTarget work"})
    assert response.status_code == 200
    res_nodes = {n["type"]: n["val"] for n in response.json()["nodes"]}

    for t in val4_types:
        assert res_nodes[t] == 4, f"Expected val=4 for type {t}, got {res_nodes[t]}"
    for t in val2_types:
        assert res_nodes[t] == 2, f"Expected val=2 for type {t}, got {res_nodes[t]}"


@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_cypher_mode_explicit(mock_call_llm, monkeypatch):
    """Cypher mode with explicit mode field still works exactly as before."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "cypher", "cypher_query": "MATCH (n:Class) RETURN n LIMIT 10", "explanation": "list classes"}',
        "Here are the classes."
    ]

    mock_db = MagicMock()
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.__iter__.return_value = []
    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__enter__.return_value = mock_session
    mock_db.get_driver.return_value = mock_driver
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "list all classes"})
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    assert res_data["cypher_query"] == "MATCH (n:Class) RETURN n LIMIT 10"
    assert res_data["translation_explanation"] == "list classes"
    assert res_data["explanation"] == "Here are the classes."


@patch("codegraphcontext.tools.code_finder.CodeFinder")
@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_flow_mode_upstream_shape(mock_call_llm, mock_code_finder_cls, monkeypatch):
    """direction='upstream' result uses affected_nodes/propagation_paths shape, not nodes/edges."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "flow", "target": "processPayment", "direction": "upstream", "explanation": "callers only"}',
        "These are the callers of processPayment.",
    ]

    # upstream/downstream analyze_impact result: affected_nodes + propagation_paths (no "nodes"/"edges")
    mock_impact_result = {
        "target": {"name": "processPayment", "type": "Function", "path": "/svc/payment.java"},
        "direction": "upstream",
        "affected_nodes": [
            {"uid": "caller-1", "name": "checkout", "path": "/svc/checkout.java", "type": "Function", "depth": 1},
            {"uid": "caller-2", "name": "PaymentService", "path": "/svc/PaymentService.java", "type": "Class", "depth": 2},
        ],
        "propagation_paths": [
            {
                "nodes": [
                    {"uid": "caller-1", "name": "checkout", "path": "/svc/checkout.java", "type": "Function"},
                    {"uid": "target-uid", "name": "processPayment", "path": "/svc/payment.java", "type": "Function"},
                ],
                "relationships": [
                    {"type": "CALLS", "line_number": 55},
                ],
            },
            {
                "nodes": [
                    {"uid": "caller-2", "name": "PaymentService", "path": "/svc/PaymentService.java", "type": "Class"},
                    {"uid": "caller-1", "name": "checkout", "path": "/svc/checkout.java", "type": "Function"},
                    {"uid": "target-uid", "name": "processPayment", "path": "/svc/payment.java", "type": "Function"},
                ],
                "relationships": [
                    {"type": "CALLS", "line_number": 10},
                    {"type": "CALLS", "line_number": 55},
                ],
            },
        ],
        "cross_repo_hops": [],
        "unresolved_crossings": [],
        "impact_score": 2.0,
        "risk_level": "Low",
        "risk_breakdown": {"depth_score": 1.0, "module_score": 1.0, "criticality_score": 0.0, "churn_score": 0.0},
        "risk_score": 1.5,
        "risk_factors": {},
        "churn_status": "off",
        "explanations": [],
    }

    mock_finder_instance = MagicMock()
    mock_finder_instance.analyze_impact.return_value = mock_impact_result
    mock_code_finder_cls.return_value = mock_finder_instance
    mock_db = MagicMock()
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "what calls processPayment"})

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    assert "upstream" in res_data["cypher_query"]

    mock_finder_instance.analyze_impact.assert_called_once_with(
        target="processPayment",
        direction="upstream",
        max_wire_hops=2,
    )

    # affected_nodes drives the node list
    assert len(res_data["nodes"]) == 2
    node_by_uid = {n["id"]: n for n in res_data["nodes"]}
    assert node_by_uid["caller-1"]["name"] == "checkout"
    assert node_by_uid["caller-1"]["val"] == 2  # Function
    assert node_by_uid["caller-2"]["name"] == "PaymentService"
    assert node_by_uid["caller-2"]["val"] == 4  # Class

    # propagation_paths drives the link list; deduplication: CALLS appears in both paths
    # but with unique (source, target) pairs so both edges are kept
    link_ids = {lnk["id"] for lnk in res_data["links"]}
    assert "caller-1->target-uid:CALLS" in link_ids
    assert "caller-2->caller-1:CALLS" in link_ids
    for lnk in res_data["links"]:
        assert set(lnk.keys()) == {"id", "source", "target", "type"}


@patch("codegraphcontext.tools.code_finder.CodeFinder")
@patch("codegraphcontext.viz.server.call_llm")
def test_ai_query_flow_mode_downstream_shape(mock_call_llm, mock_code_finder_cls, monkeypatch):
    """direction='downstream' result uses affected_nodes/propagation_paths, deduplicates edges."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake-gemini-key")

    mock_call_llm.side_effect = [
        '{"mode": "flow", "target": "publishEvent", "direction": "downstream", "explanation": "effects only"}',
        "Here is what publishEvent triggers.",
    ]

    # Duplicate edge in propagation_paths: same (source, target, type) appears twice
    mock_impact_result = {
        "target": {"name": "publishEvent", "type": "Function", "path": "/svc/events.java"},
        "direction": "downstream",
        "affected_nodes": [
            {"uid": "sink-1", "name": "OrderTopic", "path": "", "type": "Topic", "depth": 1},
        ],
        "propagation_paths": [
            {
                "nodes": [
                    {"uid": "src-uid", "name": "publishEvent", "path": "/svc/events.java", "type": "Function"},
                    {"uid": "sink-1", "name": "OrderTopic", "path": "", "type": "Topic"},
                ],
                "relationships": [{"type": "PRODUCES_TO", "line_number": 22}],
            },
            {
                "nodes": [
                    {"uid": "src-uid", "name": "publishEvent", "path": "/svc/events.java", "type": "Function"},
                    {"uid": "sink-1", "name": "OrderTopic", "path": "", "type": "Topic"},
                ],
                "relationships": [{"type": "PRODUCES_TO", "line_number": 22}],
            },
        ],
        "cross_repo_hops": [],
        "unresolved_crossings": [],
        "impact_score": 1.5,
        "risk_level": "Low",
        "risk_breakdown": {},
        "risk_score": 1.0,
        "risk_factors": {},
        "churn_status": "off",
        "explanations": [],
    }

    mock_finder_instance = MagicMock()
    mock_finder_instance.analyze_impact.return_value = mock_impact_result
    mock_code_finder_cls.return_value = mock_finder_instance
    mock_db = MagicMock()
    set_db_manager(mock_db)

    response = client.post("/api/ai_query", json={"query": "what does publishEvent trigger"})

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["success"] is True
    assert "downstream" in res_data["cypher_query"]

    mock_finder_instance.analyze_impact.assert_called_once_with(
        target="publishEvent",
        direction="downstream",
        max_wire_hops=2,
    )

    # Topic node -> val=4
    assert len(res_data["nodes"]) == 1
    assert res_data["nodes"][0]["id"] == "sink-1"
    assert res_data["nodes"][0]["val"] == 4

    # Duplicate path produces only one edge after deduplication
    assert len(res_data["links"]) == 1
    assert res_data["links"][0]["id"] == "src-uid->sink-1:PRODUCES_TO"
