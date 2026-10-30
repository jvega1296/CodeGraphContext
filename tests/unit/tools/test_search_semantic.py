"""Unit tests for the semantic search tier (Piece 5, Task 20)."""

from typing import Any, Dict, List

import pytest

from codegraphcontext.tools.search_semantic import semantic_search

# ---------------------------------------------------------------------------
# Fake session matching this codebase's `.run(query, **kwargs)` fake-session
# convention (see tests/unit/tools/test_search_fts5.py's _FakeSession).
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def run(self, query: str, **kwargs: Any) -> _FakeResult:
        return _FakeResult(self._rows)


class _FakeEmbedder:
    """embed_batch returns a caller-supplied fixed vector for every input."""

    def __init__(self, vector: List[float]):
        self._vector = vector

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [self._vector for _ in texts]


@pytest.fixture
def fake_session_with_embedded_nodes() -> _FakeSession:
    return _FakeSession([
        # Identical direction to the query vector -> cosine similarity 1.0.
        {"uid": "uid-validate-order", "embedding": [1.0, 0.0, 0.0]},
        # Orthogonal to the query vector -> cosine similarity 0.0.
        {"uid": "uid-unrelated", "embedding": [0.0, 1.0, 0.0]},
    ])


@pytest.fixture
def fake_embedder_returning_fixed_vector() -> _FakeEmbedder:
    return _FakeEmbedder([1.0, 0.0, 0.0])


@pytest.fixture
def fake_session_with_no_embedded_nodes() -> _FakeSession:
    return _FakeSession([])


@pytest.fixture
def fake_embedder() -> _FakeEmbedder:
    return _FakeEmbedder([1.0, 0.0, 0.0])


def test_semantic_search_ranks_by_cosine_similarity(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector):
    results = semantic_search(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector, "validate order")
    assert results[0][1] >= results[-1][1]  # sorted descending


def test_semantic_search_best_match_ranks_first_by_uid(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector):
    # Stronger than the loose brief assertion above: the node whose embedding
    # points in the exact same direction as the query vector must be the
    # literal first result, not just tied-or-ahead on score.
    results = semantic_search(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector, "validate order")
    assert results[0][0] == "uid-validate-order"
    assert results[0][1] == pytest.approx(1.0)
    assert results[-1][0] == "uid-unrelated"
    assert results[-1][1] == pytest.approx(0.0)


def test_semantic_search_returns_empty_when_no_nodes_have_embeddings(fake_session_with_no_embedded_nodes, fake_embedder):
    assert semantic_search(fake_session_with_no_embedded_nodes, fake_embedder, "anything") == []


def test_semantic_search_respects_limit(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector):
    results = semantic_search(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector, "validate order", limit=1)
    assert len(results) == 1
    assert results[0][0] == "uid-validate-order"


def test_semantic_search_skips_rows_missing_uid_or_embedding():
    session = _FakeSession([
        {"uid": None, "embedding": [1.0, 0.0, 0.0]},
        {"uid": "uid-no-embedding", "embedding": None},
        {"uid": "uid-valid", "embedding": [1.0, 0.0, 0.0]},
    ])
    embedder = _FakeEmbedder([1.0, 0.0, 0.0])
    results = semantic_search(session, embedder, "anything")
    assert results == [("uid-valid", pytest.approx(1.0))]
