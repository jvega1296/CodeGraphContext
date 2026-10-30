"""
Tests for the Anthropic Vertex AI provider branch in call_llm().

Env var gating: ANTHROPIC_VERTEX_PROJECT_ID activates the branch (checked first).
SDK env vars: ANTHROPIC_VERTEX_PROJECT_ID (project) + CLOUD_ML_REGION (region).
Model: claude-sonnet-4-6 (bare first-party ID, available on Vertex AI).
"""
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from codegraphcontext.viz import server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def _mock_anthropic():
    """Replace anthropic in sys.modules with a MagicMock for the duration of the block."""
    mock_module = MagicMock()
    with patch.dict(sys.modules, {"anthropic": mock_module}):
        yield mock_module


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    """Clear all provider keys before each test so branches don't bleed."""
    for key in (
        "ANTHROPIC_VERTEX_PROJECT_ID",
        "CLOUD_ML_REGION",
        "GEMINI_API_KEY",
        "OPENAI_API_KEY",
        "ATLASCLOUD_API_KEY",
        "ATLAS_CLOUD_API_KEY",
        "CHEAPER_INFERENCE_API_KEY",
        "API_ROUTE_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Vertex branch — activation and correct call
# ---------------------------------------------------------------------------

def test_vertex_branch_activates_when_project_id_set(monkeypatch):
    """With ANTHROPIC_VERTEX_PROJECT_ID set, call_llm uses AnthropicVertex."""
    monkeypatch.setenv("ANTHROPIC_VERTEX_PROJECT_ID", "my-gcp-project")

    with _mock_anthropic() as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.AnthropicVertex.return_value = mock_client
        mock_client.messages.create.return_value.content = [MagicMock(text="MATCH (n) RETURN n")]

        result = server.call_llm("system prompt", "user prompt")

    assert result == "MATCH (n) RETURN n"
    mock_anthropic.AnthropicVertex.assert_called_once()
    mock_client.messages.create.assert_called_once_with(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system="system prompt",
        messages=[{"role": "user", "content": "user prompt"}],
    )


def test_vertex_branch_returns_string(monkeypatch):
    """call_llm returns a plain str (not a response object) from the Vertex branch."""
    monkeypatch.setenv("ANTHROPIC_VERTEX_PROJECT_ID", "proj")

    with _mock_anthropic() as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.AnthropicVertex.return_value = mock_client
        mock_client.messages.create.return_value.content = [MagicMock(text="hello")]

        result = server.call_llm("sys", "usr")

    assert isinstance(result, str)
    assert result == "hello"


def test_vertex_branch_passes_system_and_user_prompts(monkeypatch):
    """System prompt and user prompt are forwarded to the correct API parameters."""
    monkeypatch.setenv("ANTHROPIC_VERTEX_PROJECT_ID", "proj")

    sys_p = "You are a helpful assistant."
    usr_p = "What is 2 + 2?"

    with _mock_anthropic() as mock_anthropic:
        mock_client = MagicMock()
        mock_anthropic.AnthropicVertex.return_value = mock_client
        mock_client.messages.create.return_value.content = [MagicMock(text="4")]

        server.call_llm(sys_p, usr_p)

    _, kwargs = mock_client.messages.create.call_args
    assert kwargs.get("system") == sys_p
    assert kwargs.get("messages") == [{"role": "user", "content": usr_p}]


# ---------------------------------------------------------------------------
# Vertex branch takes priority over other providers
# ---------------------------------------------------------------------------

def test_vertex_takes_priority_over_gemini(monkeypatch):
    """When both ANTHROPIC_VERTEX_PROJECT_ID and GEMINI_API_KEY are set, Vertex wins."""
    monkeypatch.setenv("ANTHROPIC_VERTEX_PROJECT_ID", "proj")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")

    with _mock_anthropic() as mock_anthropic, \
         patch("codegraphcontext.viz.server.requests") as mock_requests:
        mock_client = MagicMock()
        mock_anthropic.AnthropicVertex.return_value = mock_client
        mock_client.messages.create.return_value.content = [MagicMock(text="vertex response")]

        result = server.call_llm("sys", "usr")

    assert result == "vertex response"
    # Gemini REST call should NOT have been made
    mock_requests.post.assert_not_called()


def test_vertex_takes_priority_over_openai(monkeypatch):
    """When both ANTHROPIC_VERTEX_PROJECT_ID and OPENAI_API_KEY are set, Vertex wins."""
    monkeypatch.setenv("ANTHROPIC_VERTEX_PROJECT_ID", "proj")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")

    with _mock_anthropic() as mock_anthropic, \
         patch("codegraphcontext.viz.server.requests") as mock_requests:
        mock_client = MagicMock()
        mock_anthropic.AnthropicVertex.return_value = mock_client
        mock_client.messages.create.return_value.content = [MagicMock(text="vertex response")]

        result = server.call_llm("sys", "usr")

    assert result == "vertex response"
    mock_requests.post.assert_not_called()


# ---------------------------------------------------------------------------
# Existing provider chain is unchanged when ANTHROPIC_VERTEX_PROJECT_ID is absent
# ---------------------------------------------------------------------------

def test_gemini_still_used_when_vertex_not_configured(monkeypatch):
    """When ANTHROPIC_VERTEX_PROJECT_ID is NOT set, the Gemini branch still fires."""
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-key")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": "gemini output"}]}}]
    }

    with patch("codegraphcontext.viz.server.requests.post", return_value=mock_resp) as mock_post:
        result = server.call_llm("sys", "usr")

    assert result == "gemini output"
    assert "generativelanguage.googleapis.com" in mock_post.call_args.args[0]


def test_openai_still_used_when_vertex_not_configured(monkeypatch):
    """When ANTHROPIC_VERTEX_PROJECT_ID is NOT set, OpenAI branch fires normally."""
    monkeypatch.setenv("OPENAI_API_KEY", "oai-key")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "choices": [{"message": {"content": "openai output"}}]
    }

    with patch("codegraphcontext.viz.server.requests.post", return_value=mock_resp):
        result = server.call_llm("sys", "usr")

    assert result == "openai output"


def test_valueerror_still_raised_when_no_provider_configured(monkeypatch):
    """With NO provider set at all, call_llm raises ValueError (unchanged behaviour)."""
    with pytest.raises(ValueError):
        server.call_llm("sys", "usr")
