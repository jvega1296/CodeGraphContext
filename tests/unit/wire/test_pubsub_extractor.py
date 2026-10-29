"""Unit tests for wire.pubsub_extractor (MULTI_REPO_LINKS, wire piece 1a)."""
from __future__ import annotations

from pathlib import Path

from codegraphcontext.wire.config_values import ConfigValueStore
from codegraphcontext.wire.pubsub_extractor import (
    extract_publishers_from_source,
    looks_like_pubsub_source,
)


# ── Trigger detection ────────────────────────────────────────────────────────

def test_looks_like_pubsub_source_detects_spring_cloud_gcp_import():
    source = "import com.google.cloud.spring.pubsub.core.PubSubTemplate;"
    assert looks_like_pubsub_source(source) is True


def test_looks_like_pubsub_source_rejects_unrelated_file():
    assert looks_like_pubsub_source("public class Foo {}") is False


# ── Producer extraction (literal topic) ─────────────────────────────────────

def test_extracts_producer_with_literal_topic():
    source = """
    package com.autozone.oms;
    import com.google.cloud.spring.pubsub.core.PubSubTemplate;
    public class MessagePublisher {
        private final PubSubTemplate pubSubTemplate;
        public void publish() {
            pubSubTemplate.publish("oms-order-create-event", payload);
        }
    }
    """
    result = extract_publishers_from_source(source, Path("/r/MessagePublisher.java"))
    assert len(result.publishers) == 1
    rec = result.publishers[0]
    assert rec.topic_resolved == "oms-order-create-event"
    assert rec.confidence == "EXTRACTED"
    assert rec.call_shape == "PubSubTemplate.publish"
    assert rec.fqn.endswith("MessagePublisher.publish")


# ── Producer extraction (unresolved identifier) ─────────────────────────────

def test_producer_with_unresolved_placeholder_topic_is_marked_unresolved():
    source = """
    public class MessagePublisher {
        public void publish() {
            pubSubTemplate.publish(topicNameProperty, payload);
        }
    }
    """
    # topicNameProperty resolves to "${az.pubsub.topic.name}" via a @Value field,
    # and no ConfigValueStore entry exists for that key.
    result = extract_publishers_from_source(source, Path("/r/MessagePublisher.java"), store=ConfigValueStore(repo_root="/r"))
    assert result.publishers[0].confidence in ("AMBIGUOUS", "SYMBOLIC")
    assert "unresolved" in result.publishers[0].provenance


# ── Anti-goal: unrecognized usage is a warning, not silence ─────────────────

def test_unrecognized_pubsub_usage_emits_warning_not_silence():
    source = """
    import com.google.cloud.pubsub.v1.Publisher;
    public class RawPublisher {
        public void send() { publisher.publish(message); }
    }
    """
    result = extract_publishers_from_source(source, Path("/r/RawPublisher.java"))
    assert result.publishers == []
    assert any("raw" in w.lower() or "unrecognized" in w.lower() or "Publisher" in w for w in result.warnings)


def test_raw_publisher_field_named_with_pubsub_is_not_misclassified():
    """A raw com.google.cloud.pubsub.v1.Publisher field happening to be named
    with "pubsub" in it (e.g. pubsubClient) must not be mistaken for a Spring
    Cloud GCP PubSubTemplate producer just because of its name — it is
    declared as `Publisher`, not `PubSubTemplate`, so the receiver check must
    fail here the same way it does for the plain `publisher` anti-goal case.
    """
    source = """
    import com.google.cloud.pubsub.v1.Publisher;
    public class RawNamedPublisher {
        private final Publisher pubsubClient;
        public void send() { pubsubClient.publish(message); }
    }
    """
    result = extract_publishers_from_source(source, Path("/r/RawNamedPublisher.java"))
    assert result.publishers == []
    assert any("raw" in w.lower() or "unrecognized" in w.lower() or "Publisher" in w for w in result.warnings)
