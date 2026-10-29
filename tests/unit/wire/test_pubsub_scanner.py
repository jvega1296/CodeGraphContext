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
