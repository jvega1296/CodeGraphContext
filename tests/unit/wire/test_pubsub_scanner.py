"""Unit tests for wire.pubsub_scanner (MULTI_REPO_LINKS, wire piece 1c)."""
from __future__ import annotations

from pathlib import Path

from codegraphcontext.wire import scan_repo_config
from codegraphcontext.wire.pubsub_scanner import (
    PubsubScanResult,
    PubsubSubscriberRecord,
    scan_repo_pubsub,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_scan_repo_joins_bean_and_activator_across_two_files(tmp_path):
    _write(tmp_path / "src/main/java/ListenerConfig.java", """
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
    """)
    _write(tmp_path / "src/main/java/ListenerService.java", """
    @Service
    public class ListenerService {
        @ServiceActivator(inputChannel = Constants.OMS_ORDER_EVENT_MESSAGE_CHANNEL)
        public void onOrderCreateEvent(Message<String> message) { }
    }
    """)
    _write(tmp_path / "src/main/resources/application.yml", "az:\n  pubsub:\n    subscription:\n      name: oms-order-create-pull-subscription\n")

    result = scan_repo_pubsub(tmp_path, store=scan_repo_config(tmp_path))
    assert len(result.subscribers) == 1
    sub = result.subscribers[0]
    assert sub.subscription_name == "oms-order-create-pull-subscription"
    assert sub.fqn.endswith("ListenerService.onOrderCreateEvent")


def test_caller_with_a_file_over_the_extraction_size_cap_still_resolves_pending_publisher(tmp_path):
    """A caller file too large for full per-file extraction (max_file_kb)
    must still be readable for the much cheaper caller-argument-resolution
    text scan -- confirmed real against special-order-services's actual
    587 KB OrderServiceImpl.java, which is exactly where a Pub/Sub wrapper
    method's real @Value-injected topic argument lives."""
    _write(tmp_path / "src/main/java/PublishMessagePubSub.java", """
    public class PublishMessagePubSub {
        private final PubSubTemplate pubSubTemplate;
        public void publishToOMSTopic(final String topic, final String orderId) {
            pubSubTemplate.publish(topic, orderId);
        }
    }
    """)
    # Padded well past a tiny max_file_kb cap with an inert comment block,
    # so the file is unambiguously over the extraction-cost cap while still
    # containing the one real call site that matters.
    padding = "// filler\n" * 20000
    caller_source = (
        padding
        + """
    public class OrderServiceImpl {
        @Value("${topic.order.oms}")
        private String omsTopic;
        private final PublishMessagePubSub publishMessage;
        public void createOrder() {
            publishMessage.publishToOMSTopic(omsTopic, "order-1");
        }
    }
    """
    )
    _write(tmp_path / "src/main/java/OrderServiceImpl.java", caller_source)
    _write(
        tmp_path / "src/main/resources/application.yml",
        "topic:\n  order:\n    oms: oms-order-create-event\n",
    )
    assert len(caller_source.encode("utf-8")) > 100 * 1024  # comfortably over a 100 KB cap

    result = scan_repo_pubsub(tmp_path, store=scan_repo_config(tmp_path), max_file_kb=100)
    assert any("OrderServiceImpl.java" in w and "KB cap" in w for w in result.warnings)
    assert len(result.publishers) == 1
    rec = result.publishers[0]
    assert rec.topic_resolved == "oms-order-create-event"
    assert rec.pending_caller_method is None


def test_unmatched_channel_adapter_bean_emits_warning(tmp_path):
    _write(tmp_path / "src/main/java/ListenerConfig.java", """
    @Configuration
    public class ListenerConfig {
        @Bean
        public PubSubInboundChannelAdapter adapter(
            @Qualifier("someChannel") MessageChannel someChannel, PubSubTemplate t) {
          PubSubInboundChannelAdapter a = new PubSubInboundChannelAdapter(t, "literal-sub-name");
          a.setOutputChannel(someChannel);
          return a;
        }
    }
    """)
    # No file anywhere declares @ServiceActivator(inputChannel = "someChannel")
    result = scan_repo_pubsub(tmp_path)
    assert result.subscribers == []
    assert any("someChannel" in w for w in result.warnings)


def test_unmatched_activator_warning_is_suppressed_when_repo_has_no_pubsub_beans(tmp_path):
    """@ServiceActivator is generic Spring Integration chaining machinery used far
    beyond Pub/Sub consumption. A repo with zero Pub/Sub channel-adapter beans has
    nothing to join against, so flagging every unrelated @ServiceActivator there
    would just be noise — see the plan-mandated ruling on this finding."""
    _write(tmp_path / "src/main/java/Pipeline.java", """
    @Component
    public class Pipeline {
        @ServiceActivator(inputChannel = "nonPubsubChannel")
        public void transform(Message<String> message) { }
    }
    """)
    result = scan_repo_pubsub(tmp_path)
    assert result.subscribers == []
    assert not any("nonPubsubChannel" in w for w in result.warnings)


def test_unmatched_activator_still_warns_when_repo_has_other_pubsub_beans(tmp_path):
    """The suppression above must not swallow a genuinely unmatched activator
    sitting alongside an unrelated, successfully matched bean/activator pair in
    the same repo."""
    _write(tmp_path / "src/main/java/ListenerConfig.java", """
    @Configuration
    public class ListenerConfig {
        @Bean
        public PubSubInboundChannelAdapter adapter(
            @Qualifier("channelA") MessageChannel channelA, PubSubTemplate t) {
          PubSubInboundChannelAdapter a = new PubSubInboundChannelAdapter(t, "sub-a");
          a.setOutputChannel(channelA);
          return a;
        }
    }
    """)
    _write(tmp_path / "src/main/java/ListenerService.java", """
    @Service
    public class ListenerService {
        @ServiceActivator(inputChannel = "channelA")
        public void onA(Message<String> message) { }
    }
    """)
    _write(tmp_path / "src/main/java/Pipeline.java", """
    @Component
    public class Pipeline {
        @ServiceActivator(inputChannel = "channelX")
        public void onX(Message<String> message) { }
    }
    """)
    result = scan_repo_pubsub(tmp_path)
    assert len(result.subscribers) == 1
    assert result.subscribers[0].fqn.endswith("ListenerService.onA")
    assert any("channelX" in w for w in result.warnings)
    assert not any("channelA" in w and "no channel-adapter bean" in w for w in result.warnings)
