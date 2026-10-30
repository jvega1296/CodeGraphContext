"""Unit tests for wire.pubsub_extractor (MULTI_REPO_LINKS, wire piece 1a)."""
from __future__ import annotations

from pathlib import Path

from codegraphcontext.wire.config_values import ConfigValue, ConfigValueStore
from codegraphcontext.wire.pubsub_extractor import (
    extract_channel_adapter_beans_from_source,
    extract_publishers_from_source,
    extract_service_activators_from_source,
    looks_like_pubsub_source,
    resolve_pending_publishers_via_callers,
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


# ── Producer extraction (GCP full-resource-path normalization) ─────────────

def test_producer_literal_full_gcp_topic_path_is_normalized_to_bare_name():
    """A literal (or resolved) value in `projects/<id>/topics/<name>` form
    (the shape a real Cloud Run env var carries) must normalize to the bare
    `<name>` so it can match Terraform's bare-name topic/subscription
    declarations -- otherwise a fully-resolved real value could still never
    stitch to its own Terraform module."""
    source = """
    public class MessagePublisher {
        private final PubSubTemplate pubSubTemplate;
        public void publish() {
            pubSubTemplate.publish("projects/azdv1-ordfl-ordrmgmt/topics/oms-order-create-event", payload);
        }
    }
    """
    result = extract_publishers_from_source(source, Path("/r/MessagePublisher.java"))
    rec = result.publishers[0]
    assert rec.topic_raw == "projects/azdv1-ordfl-ordrmgmt/topics/oms-order-create-event"
    assert rec.topic_resolved == "oms-order-create-event"
    assert rec.confidence == "EXTRACTED"


def test_producer_at_value_field_resolves_same_as_string_constant():
    """Parity with the consumer side's _classify_subscription: an @Value
    field referenced directly at the publish call site (same class) must
    resolve exactly like a plain String constant does."""
    source = """
    public class MessagePublisher {
        private final PubSubTemplate pubSubTemplate;
        @Value("${topic.order.oms}")
        private String omsTopic;
        public void publish() {
            pubSubTemplate.publish(omsTopic, payload);
        }
    }
    """
    store = ConfigValueStore(repo_root="/r")
    store.add(ConfigValue(
        repo_root="/r", key="topic.order.oms", value="oms-order-create-event",
        source_file="/r/application.yml", source_kind="yaml",
    ))
    result = extract_publishers_from_source(source, Path("/r/MessagePublisher.java"), store=store)
    rec = result.publishers[0]
    assert rec.topic_resolved == "oms-order-create-event"
    assert rec.confidence == "INFERRED"


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


# ── Cross-method caller-argument resolution ─────────────────────────────────
#
# Real-world shape (special-order-services/PublishMessagePubSub.java): an
# abstract wrapper method's topic parameter is only ever given a real value
# by its caller, several call-frames away, via an @Value-injected field in a
# completely different class.

_WRAPPER_SOURCE = """
package com.acme.oms.pubsub;
public abstract class MessagePublisher {
    public abstract void publishToOMSTopic(final String topic, final String orderId);
}
"""

_IMPL_SOURCE = """
package com.acme.oms.pubsub;
public class PublishMessagePubSub extends MessagePublisher {
    private final PubSubTemplate pubSubTemplate;
    @Override
    public void publishToOMSTopic(final String topic, final String orderId) {
        pubSubTemplate.publish(topic, buildMessage(orderId));
    }
}
"""

_CALLER_SOURCE = """
package com.acme.oms.service;
public class OrderServiceImpl {
    @Value("${topic.order.oms}")
    private String omsTopic;
    private final MessagePublisher publishMessage;
    public void createOrder() {
        publishMessage.publishToOMSTopic(omsTopic, "order-1");
    }
}
"""


def test_pending_publisher_resolves_via_caller_at_value_field():
    pub_result = extract_publishers_from_source(_IMPL_SOURCE, Path("/r/PublishMessagePubSub.java"))
    assert len(pub_result.publishers) == 1
    pending = pub_result.publishers[0]
    assert pending.confidence == "AMBIGUOUS"
    assert pending.pending_caller_method == "publishToOMSTopic"
    assert pending.pending_caller_param_index == 0

    store = ConfigValueStore(repo_root="/r")
    store.add(ConfigValue(
        repo_root="/r", key="topic.order.oms", value="oms-order-create-event",
        source_file="/r/application.yml", source_kind="yaml",
    ))
    file_texts = {
        "/r/PublishMessagePubSub.java": _IMPL_SOURCE,
        "/r/OrderServiceImpl.java": _CALLER_SOURCE,
    }
    resolved, warnings = resolve_pending_publishers_via_callers(
        pub_result.publishers, file_texts, store=store,
    )
    assert warnings == []
    assert len(resolved) == 1
    rec = resolved[0]
    assert rec.topic_resolved == "oms-order-create-event"
    assert rec.confidence == "INFERRED"
    assert rec.pending_caller_method is None
    # The wrapper method's own FQN is preserved -- the PRODUCES_TO edge
    # still originates from the wrapper Function node, not the caller.
    assert rec.fqn.endswith("PublishMessagePubSub.publishToOMSTopic")


def test_pending_publisher_fans_out_to_multiple_distinct_callers():
    """One generic wrapper method used by several callers to publish to
    several genuinely distinct real topics must produce one resolved record
    per distinct topic, not a single merged/ambiguous one."""
    wrapper_source = """
    package com.acme.oms.pubsub;
    public class PublishMessagePubSub {
        private final PubSubTemplate pubSubTemplate;
        public boolean publishMessageOnPUBSUB(final String topic, final Object request) {
            return pubSubTemplate.publish(topic, request) != null;
        }
    }
    """
    caller_a = """
    package com.acme.oms;
    public class A {
        @Value("${topic.a}")
        private String topicA;
        void m() { publishMessage.publishMessageOnPUBSUB(topicA, req); }
    }
    """
    caller_b = """
    package com.acme.oms;
    public class B {
        @Value("${topic.b}")
        private String topicB;
        void m() { publishMessage.publishMessageOnPUBSUB(topicB, req); }
    }
    """
    pub_result = extract_publishers_from_source(wrapper_source, Path("/r/PublishMessagePubSub.java"))
    store = ConfigValueStore(repo_root="/r")
    store.add(ConfigValue(repo_root="/r", key="topic.a", value="topic-a-real", source_file="/r/a.yml", source_kind="yaml"))
    store.add(ConfigValue(repo_root="/r", key="topic.b", value="topic-b-real", source_file="/r/a.yml", source_kind="yaml"))
    file_texts = {
        "/r/PublishMessagePubSub.java": wrapper_source,
        "/r/A.java": caller_a,
        "/r/B.java": caller_b,
    }
    resolved, warnings = resolve_pending_publishers_via_callers(pub_result.publishers, file_texts, store=store)
    assert warnings == []
    resolved_topics = sorted(r.topic_resolved for r in resolved)
    assert resolved_topics == ["topic-a-real", "topic-b-real"]
    assert all(r.fqn.endswith("PublishMessagePubSub.publishMessageOnPUBSUB") for r in resolved)


def test_pending_publisher_with_no_resolvable_caller_is_dropped_with_warning():
    pub_result = extract_publishers_from_source(_IMPL_SOURCE, Path("/r/PublishMessagePubSub.java"))
    file_texts = {
        "/r/PublishMessagePubSub.java": _IMPL_SOURCE,
        # No caller anywhere in the scanned repo.
        "/r/Unrelated.java": "package x; public class Unrelated {}",
    }
    resolved, warnings = resolve_pending_publishers_via_callers(pub_result.publishers, file_texts)
    assert resolved == []
    assert len(warnings) == 1
    assert "publishToOMSTopic" in warnings[0]


def test_non_pending_publishers_pass_through_resolve_pending_unchanged():
    source = """
    public class MessagePublisher {
        private final PubSubTemplate pubSubTemplate;
        public void publish() {
            pubSubTemplate.publish("literal-topic", payload);
        }
    }
    """
    pub_result = extract_publishers_from_source(source, Path("/r/MessagePublisher.java"))
    resolved, warnings = resolve_pending_publishers_via_callers(
        pub_result.publishers, {"/r/MessagePublisher.java": source},
    )
    assert warnings == []
    assert resolved == pub_result.publishers
