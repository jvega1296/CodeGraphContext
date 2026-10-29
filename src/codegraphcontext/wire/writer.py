"""Persist wire-coupling records into the graph (MULTI_REPO_LINKS).

Called only when ``MULTI_REPO_LINKS=true`` at index time. Consumes the
records produced by :mod:`codegraphcontext.wire.kafka_scanner`,
:mod:`codegraphcontext.wire.http_scanner`, and
:mod:`codegraphcontext.wire.grpc_scanner`, and writes:

* ``Topic`` nodes (merge on ``system, name``)
* ``Endpoint`` nodes (merge on ``protocol, method, path``)
* ``PRODUCES_TO`` / ``CONSUMES_FROM`` edges Function → Topic
* ``SERVES`` / ``INVOKES`` edges Function → Endpoint

Each edge carries ``match_confidence`` (from the extractor's confidence
tier), ``provenance`` (extractor debug string), ``source_file``, and
``call_line``. Producer/server edges also carry ``call_shape`` /
``framework``.

Function nodes are matched by ``(path, name)`` — line number is not used
because the extractor records the *call site* line, not the containing
method's declaration line. Overloads legitimately produce multiple edges.

Rejected records (e.g. Kafka topic literals whose confidence is
``AMBIGUOUS``) are dropped by default so noisy identifier-as-topic hits do
not clog the graph. Set ``include_ambiguous=True`` to write them anyway.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from codegraphcontext.wire.grpc_scanner import GrpcScanResult
from codegraphcontext.wire.http_scanner import HttpScanResult
from codegraphcontext.wire.kafka_scanner import KafkaScanResult
from codegraphcontext.wire.pubsub_scanner import PubsubScanResult


HIGH_CONFIDENCE_TIERS = ("DECLARED", "EXTRACTED", "INFERRED", "NORMALIZED", "SYMBOLIC")


@dataclass
class WireWriteStats:
    topics_merged: int = 0
    endpoints_merged: int = 0
    produces_edges: int = 0
    consumes_edges: int = 0
    serves_edges: int = 0
    invokes_edges: int = 0
    skipped_ambiguous: int = 0
    dropped_bad_fqn: int = 0
    subscriptions_merged: int = 0
    pubsub_produces_edges: int = 0
    pubsub_consumes_edges: int = 0


def _fqn_method_name(fqn: str) -> Optional[str]:
    """Return the trailing method name from ``pkg.Class.method`` or ``pkg.Class.inner.method``."""
    if not fqn:
        return None
    last = fqn.rsplit(".", 1)[-1]
    if not last or not last.isidentifier():
        return None
    return last


# Records with no backing Java call site (see kafka_extractor.scan_config_kafka_bindings)
# carry this fqn prefix. They anchor their edge to the config file's `:File` node
# instead of a `:Function` node, since no method exists to match on.
CONFIG_ANCHOR_PREFIX = "config:"


def _is_config_anchor(fqn: str) -> bool:
    return bool(fqn) and fqn.startswith(CONFIG_ANCHOR_PREFIX)


def _accept(confidence: str, include_ambiguous: bool) -> bool:
    if confidence in HIGH_CONFIDENCE_TIERS:
        return True
    return include_ambiguous


def _kafka_producer_rows(scan: KafkaScanResult, include_ambiguous: bool) -> tuple:
    """Return (topic_rows, function_edge_rows, file_edge_rows, dropped_ambiguous, dropped_bad_fqn)."""
    topic_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    file_edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for p in scan.producers:
        if not _accept(p.confidence, include_ambiguous):
            ambig += 1; continue
        name = p.topic_resolved or p.topic_raw
        key = ("kafka", name)
        topic_rows[key] = {"system": "kafka", "name": name}
        row = {
            "system": "kafka", "topic_name": name,
            "path": p.source_file,
            "match_confidence": p.confidence,
            "provenance": p.provenance,
            "call_shape": p.call_shape,
            "call_line": p.line,
            "topic_raw": p.topic_raw,
        }
        if _is_config_anchor(p.fqn):
            file_edge_rows.append(row)
            continue
        method = _fqn_method_name(p.fqn)
        if method is None:
            bad_fqn += 1; continue
        row["method_name"] = method
        edge_rows.append(row)
    return list(topic_rows.values()), edge_rows, file_edge_rows, ambig, bad_fqn


def _kafka_consumer_rows(scan: KafkaScanResult, include_ambiguous: bool) -> tuple:
    """Return (topic_rows, function_edge_rows, file_edge_rows, dropped_ambiguous, dropped_bad_fqn)."""
    topic_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    file_edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for c in scan.consumers:
        if not _accept(c.confidence, include_ambiguous):
            ambig += 1; continue
        name = c.topic_resolved or c.topic_raw
        key = ("kafka", name)
        topic_rows[key] = {"system": "kafka", "name": name}
        row = {
            "system": "kafka", "topic_name": name,
            "path": c.source_file,
            "match_confidence": c.confidence,
            "provenance": c.provenance,
            "is_pattern": c.is_pattern,
            "call_line": c.line,
            "topic_raw": c.topic_raw,
        }
        if _is_config_anchor(c.fqn):
            file_edge_rows.append(row)
            continue
        method = _fqn_method_name(c.fqn)
        if method is None:
            bad_fqn += 1; continue
        row["method_name"] = method
        edge_rows.append(row)
    return list(topic_rows.values()), edge_rows, file_edge_rows, ambig, bad_fqn


def _http_server_rows(scan: HttpScanResult, include_ambiguous: bool) -> tuple:
    ep_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for s in scan.servers:
        if not _accept(s.confidence, include_ambiguous):
            ambig += 1; continue
        method = _fqn_method_name(s.fqn)
        if method is None:
            bad_fqn += 1; continue
        key = ("http", s.method, s.path)
        ep_rows[key] = {"protocol": "http", "method": s.method, "path": s.path}
        edge_rows.append({
            "protocol": "http", "endpoint_method": s.method, "endpoint_path": s.path,
            "path": s.source_file, "method_name": method,
            "match_confidence": s.confidence,
            "provenance": s.provenance,
            "framework": s.framework,
            "call_line": s.line,
            "path_raw": s.path_raw,
        })
    return list(ep_rows.values()), edge_rows, ambig, bad_fqn


def _http_client_rows(scan: HttpScanResult, include_ambiguous: bool) -> tuple:
    ep_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for c in scan.clients:
        if not _accept(c.confidence, include_ambiguous):
            ambig += 1; continue
        method = _fqn_method_name(c.fqn)
        if method is None:
            bad_fqn += 1; continue
        key = ("http", c.method, c.path)
        ep_rows[key] = {"protocol": "http", "method": c.method, "path": c.path}
        edge_rows.append({
            "protocol": "http", "endpoint_method": c.method, "endpoint_path": c.path,
            "path": c.source_file, "method_name": method,
            "match_confidence": c.confidence,
            "provenance": c.provenance,
            "framework": c.framework,
            "call_line": c.line,
            "path_raw": c.path_raw,
        })
    return list(ep_rows.values()), edge_rows, ambig, bad_fqn


def _grpc_server_rows(scan: GrpcScanResult, include_ambiguous: bool) -> tuple:
    ep_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for s in scan.servers:
        if not _accept(s.confidence, include_ambiguous):
            ambig += 1; continue
        method = _fqn_method_name(s.fqn)
        if method is None:
            bad_fqn += 1; continue
        key = ("grpc", s.rpc, s.path)
        ep_rows[key] = {"protocol": "grpc", "method": s.rpc, "path": s.path}
        edge_rows.append({
            "protocol": "grpc", "endpoint_method": s.rpc, "endpoint_path": s.path,
            "path": s.source_file, "method_name": method,
            "match_confidence": s.confidence,
            "provenance": s.provenance,
            "service": s.service,
            "framework": "grpc-java",
            "call_line": s.line,
        })
    return list(ep_rows.values()), edge_rows, ambig, bad_fqn


def _grpc_client_rows(scan: GrpcScanResult, include_ambiguous: bool) -> tuple:
    ep_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for c in scan.clients:
        if not _accept(c.confidence, include_ambiguous):
            ambig += 1; continue
        method = _fqn_method_name(c.fqn)
        if method is None:
            bad_fqn += 1; continue
        key = ("grpc", c.rpc, c.path)
        ep_rows[key] = {"protocol": "grpc", "method": c.rpc, "path": c.path}
        edge_rows.append({
            "protocol": "grpc", "endpoint_method": c.rpc, "endpoint_path": c.path,
            "path": c.source_file, "method_name": method,
            "match_confidence": c.confidence,
            "provenance": c.provenance,
            "service": c.service,
            "framework": f"grpc-java:{c.stub_kind}",
            "call_line": c.line,
        })
    return list(ep_rows.values()), edge_rows, ambig, bad_fqn


# Pub/Sub Subscription nodes have no Terraform data joined at this layer
# (project_id isn't visible from application source at all), so every
# pubsub-wire-derived Subscription row uses this same placeholder. Piece 3's
# Terraform stitching step backfills the real project_id later; as long as
# every row here is consistently "unknown", the (name, project_id) merge key
# stays stable and doesn't fork into spurious duplicate nodes.
_UNKNOWN_PROJECT_ID = "unknown"


def _pubsub_producer_rows(scan: PubsubScanResult, include_ambiguous: bool) -> tuple:
    """Return (topic_rows, function_edge_rows, dropped_ambiguous, dropped_bad_fqn).

    Mirrors :func:`_kafka_producer_rows`, writing into the same ``Topic``
    shape with ``system="pubsub"``. Pub/Sub publisher records always carry a
    real enclosing-method FQN (no config-anchor case exists for publishers),
    so there is no file-anchored variant here.
    """
    topic_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for p in scan.publishers:
        if not _accept(p.confidence, include_ambiguous):
            ambig += 1; continue
        name = p.topic_resolved or p.topic_raw
        key = ("pubsub", name)
        topic_rows[key] = {"system": "pubsub", "name": name}
        method = _fqn_method_name(p.fqn)
        if method is None:
            bad_fqn += 1; continue
        edge_rows.append({
            "system": "pubsub", "topic_name": name,
            "path": p.source_file,
            "method_name": method,
            "match_confidence": p.confidence,
            "provenance": p.provenance,
            "call_shape": p.call_shape,
            "call_line": p.line,
            "topic_raw": p.topic_raw,
        })
    return list(topic_rows.values()), edge_rows, ambig, bad_fqn


def _pubsub_consumer_rows(scan: PubsubScanResult, include_ambiguous: bool) -> tuple:
    """Return (subscription_rows, function_edge_rows, dropped_ambiguous, dropped_bad_fqn).

    Subscriber records carry no topic reference — the application code has
    no way to know which topic a subscription is bound to, only its own
    subscription name (see Decision #1: that join is Terraform's job, done
    later in Piece 3). Subscriber FQNs are always a real ``@ServiceActivator``
    method FQN, so there is no config-anchor case here either.
    """
    sub_rows: Dict[tuple, dict] = {}
    edge_rows: List[dict] = []
    ambig = 0; bad_fqn = 0
    for s in scan.subscribers:
        if not _accept(s.confidence, include_ambiguous):
            ambig += 1; continue
        key = (s.subscription_name, _UNKNOWN_PROJECT_ID)
        sub_rows[key] = {"name": s.subscription_name, "project_id": _UNKNOWN_PROJECT_ID}
        method = _fqn_method_name(s.fqn)
        if method is None:
            bad_fqn += 1; continue
        edge_rows.append({
            "subscription_name": s.subscription_name,
            "project_id": _UNKNOWN_PROJECT_ID,
            "path": s.source_file,
            "method_name": method,
            "match_confidence": s.confidence,
            "provenance": s.provenance,
            "call_line": s.line,
        })
    return list(sub_rows.values()), edge_rows, ambig, bad_fqn


# ── Cypher ──────────────────────────────────────────────────────────────────

_MERGE_TOPICS = """
UNWIND $rows AS row
MERGE (t:Topic {system: row.system, name: row.name})
"""

_MERGE_ENDPOINTS = """
UNWIND $rows AS row
MERGE (e:Endpoint {protocol: row.protocol, method: row.method, path: row.path})
"""

_MERGE_PRODUCES_TO = """
UNWIND $rows AS row
MATCH (fn:Function {path: row.path, name: row.method_name})
MERGE (t:Topic {system: row.system, name: row.topic_name})
MERGE (fn)-[e:PRODUCES_TO {call_line: row.call_line}]->(t)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.call_shape       = row.call_shape,
    e.topic_raw        = row.topic_raw,
    e.source_file      = row.path
"""

_MERGE_CONSUMES_FROM = """
UNWIND $rows AS row
MATCH (fn:Function {path: row.path, name: row.method_name})
MERGE (t:Topic {system: row.system, name: row.topic_name})
MERGE (fn)-[e:CONSUMES_FROM {call_line: row.call_line}]->(t)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.is_pattern       = row.is_pattern,
    e.topic_raw        = row.topic_raw,
    e.source_file      = row.path
"""

# Config-anchored variants — no Function node exists (see CONFIG_ANCHOR_PREFIX),
# so the edge originates from the config file's own `:File` node instead.
_MERGE_PRODUCES_TO_FILE = """
UNWIND $rows AS row
MATCH (f:File {path: row.path})
MERGE (t:Topic {system: row.system, name: row.topic_name})
MERGE (f)-[e:PRODUCES_TO {call_line: row.call_line}]->(t)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.call_shape       = row.call_shape,
    e.topic_raw        = row.topic_raw,
    e.source_file      = row.path,
    e.anchor           = 'file'
"""

_MERGE_CONSUMES_FROM_FILE = """
UNWIND $rows AS row
MATCH (f:File {path: row.path})
MERGE (t:Topic {system: row.system, name: row.topic_name})
MERGE (f)-[e:CONSUMES_FROM {call_line: row.call_line}]->(t)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.is_pattern       = row.is_pattern,
    e.topic_raw        = row.topic_raw,
    e.source_file      = row.path,
    e.anchor           = 'file'
"""

_MERGE_SERVES = """
UNWIND $rows AS row
MATCH (fn:Function {path: row.path, name: row.method_name})
MERGE (ep:Endpoint {protocol: row.protocol, method: row.endpoint_method, path: row.endpoint_path})
MERGE (fn)-[e:SERVES {call_line: row.call_line}]->(ep)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.framework        = row.framework,
    e.source_file      = row.path
"""

_MERGE_INVOKES = """
UNWIND $rows AS row
MATCH (fn:Function {path: row.path, name: row.method_name})
MERGE (ep:Endpoint {protocol: row.protocol, method: row.endpoint_method, path: row.endpoint_path})
MERGE (fn)-[e:INVOKES {call_line: row.call_line}]->(ep)
SET e.match_confidence = row.match_confidence,
    e.provenance       = row.provenance,
    e.framework        = row.framework,
    e.source_file      = row.path
"""

_MERGE_SUBSCRIPTIONS = """
UNWIND $rows AS row
MERGE (s:Subscription {name: row.name, project_id: row.project_id})
"""

# Not wired into write_wire_edges below — the application-code scan never
# knows which Topic a Subscription is bound to (see Decision #1); this is
# defined here for the Terraform stitching step (Piece 3) to call directly
# once it has joined a Subscription to its real Topic.
_MERGE_HAS_SUBSCRIPTION = """
UNWIND $rows AS row
MATCH (t:Topic {system: row.system, name: row.topic_name})
MERGE (s:Subscription {name: row.subscription_name, project_id: row.project_id})
MERGE (t)-[:HAS_SUBSCRIPTION]->(s)
"""

_MERGE_CONSUMES_FROM_SUBSCRIPTION = """
UNWIND $rows AS row
MATCH (fn:Function {path: row.path, name: row.method_name})
MERGE (s:Subscription {name: row.subscription_name, project_id: row.project_id})
MERGE (fn)-[e:CONSUMES_FROM {call_line: row.call_line}]->(s)
SET e.match_confidence = row.match_confidence, e.provenance = row.provenance, e.source_file = row.path
"""


def write_wire_edges(
    writer: Any,
    *,
    kafka_scan: Optional[KafkaScanResult] = None,
    http_scan: Optional[HttpScanResult] = None,
    grpc_scan: Optional[GrpcScanResult] = None,
    pubsub_scan: Optional[PubsubScanResult] = None,
    include_ambiguous: bool = False,
    batch_size: int = 500,
) -> WireWriteStats:
    """Merge Topic/Endpoint nodes and wire edges through ``writer`` (a GraphWriter).

    The writer's ``.driver`` and ``._db_manager`` are used to run parameterised
    Cypher with proper backend awareness (Neo4j / FalkorDB / Kuzu).
    """
    from codegraphcontext.tools.indexing.persistence.utils import (
        execute_write_operation,
        get_backend_type,
    )

    stats = WireWriteStats()
    backend = get_backend_type(writer.driver, writer._db_manager)

    def _run(cypher: str, rows: List[dict]) -> None:
        if not rows:
            return
        def _work(session):
            for i in range(0, len(rows), batch_size):
                session.run(cypher, rows=rows[i:i + batch_size])
        execute_write_operation(writer.driver, backend, _work)

    # Kafka → Topic + PRODUCES_TO / CONSUMES_FROM
    if kafka_scan is not None:
        p_topics, p_edges, p_file_edges, p_amb, p_bad = _kafka_producer_rows(kafka_scan, include_ambiguous)
        c_topics, c_edges, c_file_edges, c_amb, c_bad = _kafka_consumer_rows(kafka_scan, include_ambiguous)
        all_topics = list({(r["system"], r["name"]): r for r in p_topics + c_topics}.values())
        _run(_MERGE_TOPICS, all_topics)
        _run(_MERGE_PRODUCES_TO, p_edges)
        _run(_MERGE_CONSUMES_FROM, c_edges)
        _run(_MERGE_PRODUCES_TO_FILE, p_file_edges)
        _run(_MERGE_CONSUMES_FROM_FILE, c_file_edges)
        stats.topics_merged += len(all_topics)
        stats.produces_edges += len(p_edges) + len(p_file_edges)
        stats.consumes_edges += len(c_edges) + len(c_file_edges)
        stats.skipped_ambiguous += p_amb + c_amb
        stats.dropped_bad_fqn += p_bad + c_bad

    # HTTP → Endpoint + SERVES / INVOKES
    if http_scan is not None:
        s_eps, s_edges, s_amb, s_bad = _http_server_rows(http_scan, include_ambiguous)
        c_eps, c_edges, c_amb, c_bad = _http_client_rows(http_scan, include_ambiguous)
        all_eps = list({(r["protocol"], r["method"], r["path"]): r for r in s_eps + c_eps}.values())
        _run(_MERGE_ENDPOINTS, all_eps)
        _run(_MERGE_SERVES, s_edges)
        _run(_MERGE_INVOKES, c_edges)
        stats.endpoints_merged += len(all_eps)
        stats.serves_edges += len(s_edges)
        stats.invokes_edges += len(c_edges)
        stats.skipped_ambiguous += s_amb + c_amb
        stats.dropped_bad_fqn += s_bad + c_bad

    # gRPC → Endpoint + SERVES / INVOKES
    if grpc_scan is not None:
        s_eps, s_edges, s_amb, s_bad = _grpc_server_rows(grpc_scan, include_ambiguous)
        c_eps, c_edges, c_amb, c_bad = _grpc_client_rows(grpc_scan, include_ambiguous)
        all_eps = list({(r["protocol"], r["method"], r["path"]): r for r in s_eps + c_eps}.values())
        _run(_MERGE_ENDPOINTS, all_eps)
        _run(_MERGE_SERVES, s_edges)
        _run(_MERGE_INVOKES, c_edges)
        stats.endpoints_merged += len(all_eps)
        stats.serves_edges += len(s_edges)
        stats.invokes_edges += len(c_edges)
        stats.skipped_ambiguous += s_amb + c_amb
        stats.dropped_bad_fqn += s_bad + c_bad

    # Pub/Sub → Topic (producer) + Subscription (consumer)
    if pubsub_scan is not None:
        p_topics, p_edges, p_amb, p_bad = _pubsub_producer_rows(pubsub_scan, include_ambiguous)
        s_subs, s_edges, s_amb, s_bad = _pubsub_consumer_rows(pubsub_scan, include_ambiguous)
        _run(_MERGE_TOPICS, p_topics)
        _run(_MERGE_PRODUCES_TO, p_edges)
        _run(_MERGE_SUBSCRIPTIONS, s_subs)
        _run(_MERGE_CONSUMES_FROM_SUBSCRIPTION, s_edges)
        stats.topics_merged += len(p_topics)
        stats.pubsub_produces_edges += len(p_edges)
        stats.subscriptions_merged += len(s_subs)
        stats.pubsub_consumes_edges += len(s_edges)
        stats.skipped_ambiguous += p_amb + s_amb
        stats.dropped_bad_fqn += p_bad + s_bad

    return stats


__all__ = ["WireWriteStats", "write_wire_edges"]
