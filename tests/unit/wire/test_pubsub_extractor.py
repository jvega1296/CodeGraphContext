"""Unit tests for wire.pubsub_extractor (MULTI_REPO_LINKS, wire piece 1a)."""
from __future__ import annotations

from pathlib import Path

from codegraphcontext.wire.config_values import ConfigValueStore
from codegraphcontext.wire.pubsub_extractor import (
    extract_channel_adapter_beans_from_source,
    extract_publishers_from_source,
    extract_service_activators_from_source,
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


# ── Consumer extraction: channel-adapter bean ───────────────────────────────

def test_extracts_channel_adapter_bean_with_property_subscription_and_qualifier():
    source = """
    @Configuration
    public class ListenerConfig {
        @Value("${az.pubsub.subscription.name}")
        private String subscriptionName;

        @Bean
        public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier(Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL) MessageChannel omsOrderEventChannel,
            PubSubTemplate pubSubTemplate) {
          PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, subscriptionName);
          adapter.setOutputChannel(omsOrderEventChannel);
          return adapter;
        }
    }
    """
    beans, warnings = extract_channel_adapter_beans_from_source(source, Path("/r/ListenerConfig.java"))
    assert len(beans) == 1
    bean = beans[0]
    assert bean.subscription_raw == "subscriptionName"
    assert bean.output_channel_ref == "omsOrderEventChannel"
    assert bean.output_channel_qualifier == "Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL"


def test_extracts_channel_adapter_bean_with_string_literal_qualifier():
    """Grounding correction form 2 (3 repos): @Qualifier("...") bare string
    literal, verbatim including quotes — not just the Constants.X form."""
    source = """
    @Configuration
    public class MessageReceiver {
        @Value("${az.pubsub.subscription.name}")
        private String subscriptionName;

        @Bean
        public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier("inputMessageChannel") MessageChannel inputMessageChannel,
            PubSubTemplate pubSubTemplate) {
          PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, subscriptionName);
          adapter.setOutputChannel(inputMessageChannel);
          return adapter;
        }
    }
    """
    beans, warnings = extract_channel_adapter_beans_from_source(source, Path("/r/MessageReceiver.java"))
    assert len(beans) == 1
    bean = beans[0]
    assert bean.output_channel_ref == "inputMessageChannel"
    assert bean.output_channel_qualifier == '"inputMessageChannel"'


# ── Consumer extraction: @ServiceActivator ──────────────────────────────────

def test_extracts_service_activator_with_input_channel():
    source = """
    @Service
    public class ListenerService {
        @ServiceActivator(inputChannel = Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL)
        public void onOrderCreateEvent(Message<String> message) { ... }
    }
    """
    activators, warnings = extract_service_activators_from_source(source, Path("/r/ListenerService.java"))
    assert len(activators) == 1
    assert activators[0].input_channel_raw == "Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL"
    assert activators[0].fqn.endswith("ListenerService.onOrderCreateEvent")


def test_service_activator_with_symbolic_input_channel_is_not_locally_resolved():
    """A `Constants.X` reference cannot be resolved from this file alone (the
    Constants class may live elsewhere) — resolving it is Task 3's join."""
    source = """
    @Service
    public class ListenerService {
        @ServiceActivator(inputChannel = Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL)
        public void onOrderCreateEvent(Message<String> message) { ... }
    }
    """
    activators, warnings = extract_service_activators_from_source(source, Path("/r/ListenerService.java"))
    assert activators[0].input_channel_resolved is None


def test_extracts_service_activator_with_string_literal_input_channel():
    """Grounding correction form 2 (3 repos): inputChannel = "..." bare string
    literal, verbatim including quotes — not just the Constants.X form."""
    source = """
    @Component
    public class MessageReceiver {
        @ServiceActivator(inputChannel = "inputMessageChannel")
        public void onMessage(Message<String> message) { ... }
    }
    """
    activators, warnings = extract_service_activators_from_source(source, Path("/r/MessageReceiver.java"))
    assert len(activators) == 1
    assert activators[0].input_channel_raw == '"inputMessageChannel"'
    assert activators[0].input_channel_resolved == "inputMessageChannel"
    assert activators[0].fqn.endswith("MessageReceiver.onMessage")


# ── Consumer extraction: unresolved subscription placeholder ───────────────

def test_channel_adapter_with_unresolved_subscription_placeholder_is_marked():
    source = """
    @Configuration
    public class ListenerConfig {
        @Value("${az.pubsub.subscription.name}")
        private String subscriptionName;
        @Bean
        public PubSubInboundChannelAdapter adapter(PubSubTemplate t) {
          PubSubInboundChannelAdapter a = new PubSubInboundChannelAdapter(t, subscriptionName);
          a.setOutputChannel(someChannel);
          return a;
        }
    }
    """
    # No ConfigValueStore entry for az.pubsub.subscription.name
    beans, warnings = extract_channel_adapter_beans_from_source(source, Path("/r/ListenerConfig.java"), store=ConfigValueStore(repo_root="/r"))
    assert beans[0].confidence in ("AMBIGUOUS", "SYMBOLIC")
    assert "unresolved" in beans[0].provenance


# ── Anti-goal: near-miss shapes warn, never silently vanish ─────────────────

def test_service_activator_with_unrecognized_attribute_emits_warning_not_silence():
    """`@ServiceActivator` is a 100%-confident consumer-wiring signal; a typo'd
    or missing `inputChannel` attribute must warn, not silently disappear."""
    source = """
    @Service
    public class ListenerService {
        @ServiceActivator(channel = Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL)
        public void onOrderCreateEvent(Message<String> message) { ... }
    }
    """
    activators, warnings = extract_service_activators_from_source(source, Path("/r/ListenerService.java"))
    assert activators == []
    assert any("onOrderCreateEvent" in w and "ServiceActivator" in w for w in warnings)


def test_channel_adapter_bean_missing_set_output_channel_emits_warning_not_silence():
    """A @Bean method that constructs PubSubInboundChannelAdapter but never
    calls setOutputChannel(...) must warn, not silently disappear."""
    source = """
    @Configuration
    public class ListenerConfig {
        @Value("${az.pubsub.subscription.name}")
        private String subscriptionName;
        @Bean
        public PubSubInboundChannelAdapter adapter(PubSubTemplate t) {
          PubSubInboundChannelAdapter a = new PubSubInboundChannelAdapter(t, subscriptionName);
          return a;
        }
    }
    """
    beans, warnings = extract_channel_adapter_beans_from_source(source, Path("/r/ListenerConfig.java"))
    assert beans == []
    assert any("adapter" in w and "PubSubInboundChannelAdapter" in w for w in warnings)
