# OMS Cross-Repo Code + Infra Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CodeGraphContext (this fork) trace a business flow end-to-end across 60+ Java/Spring repos and the GCP/Terraform infrastructure connecting them, with risk-scored blast-radius analysis and a fast hybrid search cascade, replacing neo-rag and manual Terraform/K8s reading.

**Architecture:** Five independent-but-sequenced pieces on top of the existing wire-coupling (PR #1728) and blast-radius (PR #1298) foundations already merged into `main`: (1) a GCP Pub/Sub wire extractor adding a first-class `Subscription` node, (2) a `direction` parameter on `analyze_impact` with a BFS that crosses `PRODUCES_TO`/`CONSUMES_FROM`/`SERVES`/`INVOKES` wire edges in addition to the existing `CALLS`-style chain, (3) a Terraform-stitching step that attaches `Topic` nodes to the real HCL resource behind them, (4) a ported risk-scoring engine from `code-review-graph` (security keywords, churn, community-crossing and test-coverage proxies, diff-based change-set orchestration), and (5) a 3-tier search cascade (exact Cypher → SQLite FTS5 lexical → local fastembed semantic) that only pays for a slower tier when the faster one returns nothing.

**Tech Stack:** Python 3.12, LadybugDB (embedded, Cypher-compatible) via the existing `GraphWriter`/driver abstraction, stdlib `sqlite3` (FTS5 is a compile-time stdlib feature, no new dependency), `fastembed` (already a lazy optional import, add to `pyproject.toml` as a named extra), `subprocess`+`git` for churn/diff (already a pattern in `code_finder.py`), pytest (existing hand-rolled fake-driver convention, no mocking library).

**Spec:** `docs/superpowers/specs/2026-10-29-oms-cross-repo-graph-design.md`

## Global Constraints

- Windows 11, AutoZone corporate laptop, no admin rights, no Docker/WSL2 — every step must run with plain `pip`/`pytest`/`git` in the existing venv.
- Graph backend is LadybugDB (embedded, Cypher-compatible) — any new Cypher clause not already proven to run against this backend elsewhere in the codebase must be verified against the real installed LadybugDB before being relied on; do not assume Neo4j dialect parity.
- Python 3.12 venv at `C:\Users\jvega3\.codegraphcontext-venv` — do not change the Python version.
- Corporate TLS proxy workarounds already in place (`strict-ssl false`, `GIT_SSL_NO_VERIFY=true`, `SSL_CERT_FILE`) — reuse, do not rediscover.
- Must not break the LadybugDB `sitecustomize.py` DLL fix or the MCP registration in `~/.claude.json`.
- A traversal that silently stops at a repo or infra boundary without saying so is a hard failure (anti-goal) — every new cross-boundary query must report what it could not cross, not just return fewer results.
- No added latency on the fast/exact search path when a slower tier is never needed (anti-goal) — the search cascade and the direction/wire-bridging additions to `analyze_impact` must not run expensive work unconditionally.
- A wire extractor must never silently produce nothing for a valid-but-unrecognized usage pattern (anti-goal) — unrecognized patterns get a warning, not silence.
- Acceptance criterion (Decision #8, literal): re-index `special-order-services`, `oms-order-consumer`, `oms-order-update-consumer`, and `terra-projects-oms/az-ordfl-ordrmgmt` for real, then `analyze_impact("createOrder"-or-"MessagePublisher.publish", direction="both")` must return nodes from the consumer repos **and** the Terraform module behind the topic, as one merged connected trace.

## Review Focus

- **Unrecognized GCP Pub/Sub usage pattern in a real file** (e.g. raw `com.google.cloud.pubsub.v1.Publisher` instead of Spring Cloud GCP, or a consumer wired some other way than the confirmed bean pattern): the extractor must emit a warning naming the file, not silently return zero records. (Task 1, Task 2)
- **A wire-bridge or Terraform-stitch crossing that cannot complete** (topic has no indexed `Subscription`, subscription has no indexed consumer function, topic has no matching Terraform resource): the result must say so explicitly in an `unresolved_crossings`/similar field, not silently truncate the trace and look like "no further impact." (Task 7, Task 9, Task 12)
- **LadybugDB rejects or silently mis-executes the new bridging Cypher** (dialect variance from Neo4j — e.g. `OPTIONAL MATCH` behavior, multi-label pattern matching): a dialect problem must surface as a loud query error in logs/tests, never as an empty-but-plausible "no impact" result that looks like a true negative. (Task 7)
- **An unresolved `${...}` subscription-name placeholder** (no matching `application*.yml`/`.properties` entry found by `config_scanner.py`): the consumer record must carry an explicit unresolved/low-confidence marker (mirroring Kafka's existing `AMBIGUOUS`/`unresolved:` provenance convention), not be silently dropped or silently treated as if the literal placeholder string were a real subscription name. (Task 2)
- **Churn/diff scoring run against a directory with no `.git`/`.svn`** (a vendored or copied-out subtree): must degrade to `churn_status="unavailable"`/`"off"` cleanly and let the rest of the risk score and the overall tool call still return successfully, not raise and fail the whole request. (Task 14, Task 18)

---

## File Structure

| File | Piece | Responsibility |
|---|---|---|
| `src/codegraphcontext/wire/pubsub_extractor.py` (new) | 1 | Per-file GCP Pub/Sub producer/consumer extraction, mirrors `kafka_extractor.py` |
| `src/codegraphcontext/wire/pubsub_scanner.py` (new) | 1 | Repo-wide scan orchestration, mirrors `kafka_scanner.py`; owns the cross-file bean→`@ServiceActivator` join |
| `src/codegraphcontext/wire/writer.py` (modify) | 1 | Add `Subscription` node, `HAS_SUBSCRIPTION`/consumer-side `CONSUMES_FROM` Cypher, `pubsub_scan` param on `write_wire_edges` |
| `src/codegraphcontext/wire/__init__.py` (modify) | 1 | Export new pubsub names |
| `src/codegraphcontext/tools/indexing/pipeline.py` (modify) | 1, 5 | Phase 6: add pubsub scan call; new Phase 7: FTS5 sidecar population |
| `src/codegraphcontext/tools/code_finder.py` (modify) | 2, 4 | `analyze_impact`: add `direction`/`max_wire_hops` params, wire-bridging BFS, ported risk-score fields |
| `src/codegraphcontext/tool_definitions.py` (modify) | 2, 3, 4, 5 | New/changed tool schemas: `analyze_impact` (direction), `stitch_infrastructure`, `analyze_change_risk`, `search_code` |
| `src/codegraphcontext/tools/handlers/analysis_handlers.py` (modify) | 2, 3, 4, 5 | New/changed handler functions for the above tools |
| `src/codegraphcontext/server.py` (modify) | 2, 3, 4, 5 | `tool_map` wiring for new tools |
| `src/codegraphcontext/tools/languages/hcl.py` (modify) | 3 | Generic attribute capture on `pull_subscriptions` list (topic string first, then object-list walk for `service_account`) |
| `src/codegraphcontext/wire/terraform_stitcher.py` (new) | 3 | Post-index Cypher stitching `Topic`→Terraform `Class` node via `BACKED_BY` |
| `src/codegraphcontext/tools/risk_scoring.py` (new) | 4 | Ported security keywords, caller-count, churn infra, community-crossing proxy, test-coverage proxy, diff parsing, change-set orchestration |
| `src/codegraphcontext/tools/search_fts5.py` (new) | 5 | SQLite FTS5 sidecar: schema, indexing, BM25 query, `_split_identifier` port |
| `src/codegraphcontext/tools/search_semantic.py` (new) | 5 | Cosine-similarity semantic tier over existing node embedding properties |
| `src/codegraphcontext/tools/search_cascade.py` (new) | 5 | 3-tier orchestration: exact → FTS5 → semantic, each tier gated on the prior returning zero |
| `pyproject.toml` (modify) | 5 | Add `fastembed` as a named optional-dependency extra (matches existing `[project.optional-dependencies]` convention) |

Tests mirror each new/modified file 1:1 under `tests/unit/wire/`, `tests/unit/tools/`, `tests/unit/parsers/`, plus one new `tests/integration/test_oms_cross_repo_acceptance.py` for the final end-to-end criterion.

---

# Piece 1 — GCP Pub/Sub Wire Extractor + Subscription Node

Parallelizable with Piece 2. Ground truth (from the design doc, already verified against real AZ code): producer = `PubSubTemplate.publish(...)` behind a wrapper class; consumer = a `@Bean`-method `PubSubInboundChannelAdapter` wiring a subscription name to a `MessageChannel`, consumed by a separate class's `@ServiceActivator`-annotated method — three hops, same repo.

### Task 1: Pub/Sub producer extraction

**Files:**
- Create: `src/codegraphcontext/wire/pubsub_extractor.py`
- Test: `tests/unit/wire/test_pubsub_extractor.py`

**Interfaces:**
- Consumes: `codegraphcontext.wire.config_values.ConfigValueStore`, `.resolve_placeholders()`, `BASE_PROFILE` (existing, read-only reuse — no changes needed, confirmed already solves `${...}` resolution).
- Produces (consumed by Task 3 and Task 4):
  ```python
  @dataclass(frozen=True)
  class PubsubPublisherRecord:
      fqn: str
      topic_raw: str
      topic_resolved: str
      confidence: str          # "EXTRACTED" | "INFERRED" | "SYMBOLIC" | "AMBIGUOUS"
      provenance: str
      source_file: str
      line: int
      call_shape: str          # e.g. "PubSubTemplate.publish"

  @dataclass
  class PubsubExtractionResult:
      publishers: List[PubsubPublisherRecord] = field(default_factory=list)
      warnings: List[str] = field(default_factory=list)
      def is_empty(self) -> bool: ...

  def looks_like_pubsub_source(source: str) -> bool: ...

  def extract_publishers_from_source(
      source: str,
      path: Path,
      *,
      store: Optional[ConfigValueStore] = None,
      active_profile: str = BASE_PROFILE,
  ) -> PubsubExtractionResult: ...
  ```

- [ ] **Step 1: Write the failing test for trigger detection**

```python
def test_looks_like_pubsub_source_detects_spring_cloud_gcp_import():
    source = "import com.google.cloud.spring.pubsub.core.PubSubTemplate;"
    assert looks_like_pubsub_source(source) is True

def test_looks_like_pubsub_source_rejects_unrelated_file():
    assert looks_like_pubsub_source("public class Foo {}") is False
```

- [ ] **Step 2: Run test to verify it fails** — `pytest tests/unit/wire/test_pubsub_extractor.py -v`, expect `ImportError`/`NameError: looks_like_pubsub_source`.

- [ ] **Step 3: Implement `looks_like_pubsub_source(source: str) -> bool`**

Match on `com.google.cloud.spring.pubsub` substring presence (the confirmed Spring Cloud GCP package), mirroring `kafka_extractor.py`'s `looks_like_kafka_source` shape (cheap substring pre-filter, not a full parse).

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for literal-topic producer extraction**

```python
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
```

- [ ] **Step 6: Run test to verify it fails.**

- [ ] **Step 7: Implement `extract_publishers_from_source`**

Regex/line-scan for `<identifier>.publish(` calls on a variable whose declared type is `PubSubTemplate` (mirror the identifier-to-declared-type resolution already used in `kafka_extractor.py` for `KafkaTemplate`/`@KafkaListener`-style variable tracking — read that file's approach to resolving a call receiver's declared type before writing this). First string-literal argument is `topic_raw`; `_classify_literal`-equivalent logic (string literal → `EXTRACTED`; a field/constant reference → attempt `store.resolve_placeholders` only if it's a `${...}`-shaped property reference, else mark `confidence="SYMBOLIC"`, `provenance=f"identifier:{name}"` same as Kafka's untraced-identifier convention). Wrap the whole method body in the enclosing class's FQN the same way `kafka_extractor.py` does for its FQN construction — reuse that helper if it's a standalone function in that file (check before duplicating).

- [ ] **Step 8: Run test to verify it passes.**

- [ ] **Step 9: Write the failing test for an unresolved config-placeholder topic**

```python
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
```

- [ ] **Step 10: Run test to verify it fails, then implement the unresolved-placeholder path** (reuse `store.resolve_placeholders` and its `unresolved` list exactly as `kafka_extractor.py` already does for config-driven topic names — read that call site before writing this one; do not re-derive the placeholder regex independently).

- [ ] **Step 11: Run test to verify it passes.**

- [ ] **Step 12: Write the failing test for the anti-goal — unrecognized pattern produces a warning, not silence**

```python
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
```

This pins the anti-goal from Review Focus item 1: `looks_like_pubsub_source` should still return `True` for this file (it imports a GCP Pub/Sub package) but the Spring-Cloud-GCP-specific extraction regex won't match the raw-client `Publisher.publish(Message)` call shape — the extractor must notice it looked like Pub/Sub source and found nothing, and say so.

- [ ] **Step 13: Run test to verify it fails, then implement**: when `looks_like_pubsub_source(source)` is `True` but zero publisher records were found, append a warning naming the file and noting only the Spring Cloud GCP `PubSubTemplate` shape is currently recognized.

- [ ] **Step 14: Run test to verify it passes.**

- [ ] **Step 15: Commit**

```bash
git add src/codegraphcontext/wire/pubsub_extractor.py tests/unit/wire/test_pubsub_extractor.py
git commit -m "feat: GCP Pub/Sub publisher extraction (wire piece 1a)"
```

---

### Task 2: Pub/Sub consumer extraction — bean/channel/`@ServiceActivator` join

**Before writing code**, read `ListenerConfig.java`'s full pattern again (already captured in the design doc's Ground Truth) and grep the other 3 consumer repos' config classes for the equivalent bean (`oms-order-update-consumer`, and whichever two of `vendor-oms-update-consumer`/`special-order-shipping-hub-listener`/`oms-hub-consumer`/`oms-shipment-label-consumer` are reachable in this environment) to confirm the join key (bean method name vs. `@Qualifier` constant vs. literal string) is consistent before committing to one regex — the design doc explicitly flags this as unverified across all four repos.

**Files:**
- Modify: `src/codegraphcontext/wire/pubsub_extractor.py`
- Test: `tests/unit/wire/test_pubsub_extractor.py` (extend)

**Interfaces:**
- Produces (consumed by Task 3):
  ```python
  @dataclass(frozen=True)
  class PubsubChannelAdapterBean:
      fqn: str                      # bean method FQN
      subscription_raw: str
      subscription_resolved: str
      confidence: str
      provenance: str
      output_channel_ref: str       # variable name passed to setOutputChannel(...)
      output_channel_qualifier: Optional[str]  # resolved @Qualifier value on that parameter, if present
      source_file: str
      line: int

  @dataclass(frozen=True)
  class PubsubServiceActivator:
      fqn: str                      # method FQN
      input_channel_raw: str
      input_channel_resolved: Optional[str]  # resolved if input_channel_raw was a constant reference
      source_file: str
      line: int

  def extract_channel_adapter_beans_from_source(
      source: str, path: Path, *, store: Optional[ConfigValueStore] = None, active_profile: str = BASE_PROFILE,
  ) -> Tuple[List[PubsubChannelAdapterBean], List[str]]:  # (beans, warnings)

  def extract_service_activators_from_source(
      source: str, path: Path,
  ) -> Tuple[List[PubsubServiceActivator], List[str]]:  # (activators, warnings)
  ```
  Both extraction functions are per-file and side-effect-free; the cross-file join that turns these into `PubsubSubscriberRecord`s happens in Task 3 (`pubsub_scanner.py`), because the bean and the activator can live in different files within the same repo.

- [ ] **Step 1: Write the failing test for channel-adapter bean extraction**

```python
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
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `extract_channel_adapter_beans_from_source`**

Find `@Bean` methods whose body constructs `new PubSubInboundChannelAdapter(<pubSubTemplateRef>, <subscriptionNameRef>)` followed by `.setOutputChannel(<channelRef>)` on the resulting local variable. `subscription_raw` is the second constructor argument's identifier; if that identifier is a field in the same class annotated `@Value("${...}")`, resolve through `store` exactly as Task 1's placeholder handling does (reuse, don't reimplement). `output_channel_ref` is the `setOutputChannel` argument's identifier; if it's a method parameter annotated `@Qualifier(<value>)`, capture `<value>` verbatim (may be a string literal or a constant field reference like `Constants.X` — store as written, resolution of `Constants.X`'s actual value happens in Task 3's join, not here, since that requires seeing the `Constants` class which may be a different file).

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for `@ServiceActivator` extraction**

```python
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
```

- [ ] **Step 6: Run test to verify it fails.**

- [ ] **Step 7: Implement `extract_service_activators_from_source`** — regex for `@ServiceActivator(inputChannel = <value>)` immediately preceding a method declaration; `<value>` captured verbatim (string literal or constant reference), same convention as Step 3.

- [ ] **Step 8: Run test to verify it passes.**

- [ ] **Step 9: Write the failing test for the Review Focus item — unresolved subscription placeholder**

```python
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
```

- [ ] **Step 10: Run test to verify it fails, then implement** (reuse the same `store.resolve_placeholders` + `AMBIGUOUS`/`unresolved:` convention as Task 1 Step 10 — do not invent a second convention for the same concept).

- [ ] **Step 11: Run test to verify it passes.**

- [ ] **Step 12: Commit**

```bash
git add src/codegraphcontext/wire/pubsub_extractor.py tests/unit/wire/test_pubsub_extractor.py
git commit -m "feat: GCP Pub/Sub consumer bean + ServiceActivator extraction (wire piece 1b)"
```

---

### Task 3: Repo scanner — cross-file join and `Subscription` record assembly

**Files:**
- Create: `src/codegraphcontext/wire/pubsub_scanner.py`
- Test: `tests/unit/wire/test_pubsub_scanner.py`

**Interfaces:**
- Consumes: Task 1's `extract_publishers_from_source`/`PubsubPublisherRecord`, Task 2's `extract_channel_adapter_beans_from_source`/`extract_service_activators_from_source`/`PubsubChannelAdapterBean`/`PubsubServiceActivator`. `kafka_scanner.py`'s `DEFAULT_JAVA_SOURCE_DIRS`, `DEFAULT_JAVA_EXCLUDE_PATH_PARTS` (reuse directly, do not redefine).
- Produces (consumed by Task 4):
  ```python
  @dataclass(frozen=True)
  class PubsubSubscriberRecord:
      fqn: str                 # the @ServiceActivator method's FQN — this is the Function node the CONSUMES_FROM edge attaches to
      subscription_name: str
      confidence: str
      provenance: str
      source_file: str
      line: int

  @dataclass
  class PubsubScanResult:
      publishers: List[PubsubPublisherRecord] = field(default_factory=list)
      subscribers: List[PubsubSubscriberRecord] = field(default_factory=list)
      warnings: List[str] = field(default_factory=list)
      files_scanned: int = 0
      files_skipped: int = 0

  def scan_repo_pubsub(
      repo_root: Path, *, store: Optional[ConfigValueStore] = None, active_profile: str = BASE_PROFILE,
      include_dirs: Sequence[str] = DEFAULT_JAVA_SOURCE_DIRS,
      exclude_path_parts: Sequence[str] = DEFAULT_JAVA_EXCLUDE_PATH_PARTS,
      max_file_kb: int = 512,
  ) -> PubsubScanResult: ...
  ```

- [ ] **Step 1: Write the failing test for same-repo bean→activator join by qualifier constant**

```python
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
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `scan_repo_pubsub`**

Walk the repo (mirror `scan_repo_kafka`'s directory-walk/exclude/dedupe/size-cap logic exactly — reuse its loop shape, do not rewrite it) collecting per-file `PubsubExtractionResult.publishers`, `PubsubChannelAdapterBean`s, and `PubsubServiceActivator`s into repo-wide lists. After the walk: build a `qualifier_value → bean` and `qualifier_value → [activators]` join using `output_channel_qualifier`/`input_channel_raw` as the join key (both carry the raw `Constants.X` text verbatim per Task 2's design, so a direct string-equality join works without needing to resolve `Constants.X`'s literal value — confirm this is sufficient by checking whether any of the 4 real consumer repos use a bare string literal qualifier instead of a constant, during the ground-truth check at the top of Task 2; if some repos use literals and others constants, the join key must normalize both to the same comparable form before matching). For each matched pair, resolve the bean's `subscription_raw` through `store` (same as Task 1/2) to get the final `subscription_name`, and emit one `PubsubSubscriberRecord` per matched activator. Unmatched beans or activators (no join partner found) are **not** silently dropped — append a warning naming the file(s) and the unmatched qualifier/channel value, per the anti-goal.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the anti-goal — unmatched bean or activator warns**

```python
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
```

- [ ] **Step 6: Run test to verify it fails, then implement the warning path from Step 3.**

- [ ] **Step 7: Run test to verify it passes.**

- [ ] **Step 8: Commit**

```bash
git add src/codegraphcontext/wire/pubsub_scanner.py tests/unit/wire/test_pubsub_scanner.py
git commit -m "feat: GCP Pub/Sub repo-wide scan with bean/activator join (wire piece 1c)"
```

---

### Task 4: Writer — `Subscription` node schema and `write_wire_edges` extension

**Files:**
- Modify: `src/codegraphcontext/wire/writer.py`
- Modify: `src/codegraphcontext/wire/__init__.py`
- Test: `tests/unit/wire/test_wire_writer.py` (extend)

**Interfaces:**
- Consumes: Task 3's `PubsubScanResult` (`.publishers: List[PubsubPublisherRecord]`, `.subscribers: List[PubsubSubscriberRecord]`).
- Produces (consumed by Task 7, Task 12): new `write_wire_edges(..., pubsub_scan: Optional[PubsubScanResult] = None, ...)` keyword param; new `WireWriteStats` fields `subscriptions_merged: int`, `pubsub_produces_edges: int`, `pubsub_consumes_edges: int` (kept separate from the existing `produces_edges`/`consumes_edges` counters so Kafka and Pub/Sub volumes stay independently visible in logs).

Per Decision #1 in the design doc: `Topic {system, name}` unchanged (`system="pubsub"` reuses existing `_MERGE_TOPICS`); new node `Subscription {name, project_id}`; new edges `(Topic)-[:HAS_SUBSCRIPTION]->(Subscription)` and `(Function)-[:CONSUMES_FROM]->(Subscription)` (not `Topic`, for the consumer side — this is the schema decision that makes Terraform's `service_account`-per-subscription signal attachable later in Piece 3). Producer side stays `(Function)-[:PRODUCES_TO]->(Topic)`, unchanged shape.

- [ ] **Step 1: Write the failing test for `Subscription` node + edge merge Cypher shape**

```python
def test_write_wire_edges_merges_subscription_node_and_edges(fake_writer):
    pubsub_scan = PubsubScanResult(
        publishers=[PubsubPublisherRecord(fqn="com.az.MessagePublisher.publish", topic_raw="oms-order-create-event",
                                           topic_resolved="oms-order-create-event", confidence="EXTRACTED",
                                           provenance="literal", source_file="/r/MessagePublisher.java", line=10,
                                           call_shape="PubSubTemplate.publish")],
        subscribers=[PubsubSubscriberRecord(fqn="com.az.ListenerService.onOrderCreateEvent",
                                             subscription_name="oms-order-create-pull-subscription",
                                             confidence="EXTRACTED", provenance="config:az.pubsub.subscription.name",
                                             source_file="/r/ListenerService.java", line=20)],
    )
    stats = write_wire_edges(fake_writer, pubsub_scan=pubsub_scan)
    assert stats.subscriptions_merged == 1
    assert stats.pubsub_produces_edges == 1
    assert stats.pubsub_consumes_edges == 1
```

Use whatever `fake_writer` fixture/pattern `test_wire_writer.py` already uses (read that file first — it was flagged but not read in full during grounding; follow its existing fake-driver convention rather than introducing a new one).

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement.**

Add Cypher constants mirroring the existing style exactly:
```python
_MERGE_SUBSCRIPTIONS = """
UNWIND $rows AS row
MERGE (s:Subscription {name: row.name, project_id: row.project_id})
"""
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
```
`project_id` is not yet known at this layer (no Terraform data joined here) — default to the literal string `"unknown"` until Piece 3's stitching step can backfill it from the real Terraform resource; document this default inline as a comment, since a `Subscription` merge key of `(name, project_id)` with everything on `"unknown"` is still correct as long as every pubsub-wire-derived `Subscription` uses the same placeholder consistently (avoids spurious duplicate nodes). Add the `pubsub_scan` branch to `write_wire_edges` mirroring the existing `if kafka_scan is not None:` block shape (row-building, `_run()` calls, stats increments), reusing `_MERGE_TOPICS`/`_MERGE_PRODUCES_TO` as-is with `system="pubsub"` for the producer side.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Update `wire/__init__.py`** — add the new Task 1/2/3/4 public names to the import block and `__all__`, following the exact existing alias convention (`extract_from_source as extract_pubsub_from_source`, etc., as scoped in the earlier grounding).

- [ ] **Step 6: Run the full existing wire test suite to confirm no regression** — `pytest tests/unit/wire/ -v`, expect all pre-existing tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/codegraphcontext/wire/writer.py src/codegraphcontext/wire/__init__.py tests/unit/wire/test_wire_writer.py
git commit -m "feat: Subscription node schema + pubsub_scan param on write_wire_edges (wire piece 1d)"
```

---

### Task 5: Pipeline wiring — Phase 6 extension

**Files:**
- Modify: `src/codegraphcontext/tools/indexing/pipeline.py` (the Phase 6 block, lines ~500–554)
- Test: none new (covered by the existing integration smoke-test pattern in Task 22's acceptance test; this task is wiring only)

**Interfaces:**
- Consumes: Task 3's `scan_repo_pubsub`, Task 4's extended `write_wire_edges`.

- [ ] **Step 1: Modify the Phase 6 `try` block**

Add `from ...wire import scan_repo_pubsub` to the existing lazy import line (alongside `scan_repo_config, scan_repo_grpc, scan_repo_http, scan_repo_kafka, write_wire_edges`). Add `pubsub_scan = scan_repo_pubsub(repo_root, store=store)` immediately after the existing `grpc_scan = scan_repo_grpc(...)` line. Extend the `warning_logger(f"[WIRE] Scan complete — ...")` f-string with a `pubsub p/s={len(pubsub_scan.publishers)}/{len(pubsub_scan.subscribers)}` segment. Pass `pubsub_scan=pubsub_scan` into the existing `write_wire_edges(...)` call. Extend the `warning_logger(f"[WIRE] Persist complete — ...")` f-string with `subscriptions={wire_stats.subscriptions_merged}` and the new `pubsub_produces_edges`/`pubsub_consumes_edges` counters.

- [ ] **Step 2: Manually verify with a real indexing run**

Re-index `special-order-services` with `MULTI_REPO_LINKS=true` and confirm the `[WIRE]` log lines show non-zero `pubsub p/s` counts, matching the known producer (`MessagePublisher`/`PublishMessagePubSub`) and the 3 known topics from `AgentProject.md`. This is a manual verification step, not a pytest assertion — record the exact log output in the commit message or a scratch note for cross-reference during Task 22.

- [ ] **Step 3: Commit**

```bash
git add src/codegraphcontext/tools/indexing/pipeline.py
git commit -m "feat: wire GCP Pub/Sub scan into Phase 6 pipeline (wire piece 1e)"
```

---

# Piece 2 — `analyze_impact` Direction Parameter + Wire-Edge Bridging

Parallelizable with Piece 1 (can be built/tested against the existing Kafka wire edges first, per the design doc's own sequencing note — the direction-flip and bridging logic is identical regardless of which extractor populated the `Topic`/`Subscription` nodes).

**Design decision made here** (resolving the two "builder's call" items the design doc left open): wire-hop crossings get their **own** budget, `max_wire_hops` (default 2), separate from the existing `depth` parameter — conflating them would make `depth` behave inconsistently for single-repo-only callers who never cross a wire edge. The upstream/downstream merge for `direction=both` is done as **two separate Cypher queries merged in Python** (not a single `UNION` clause) — this is the safest option against LadybugDB Cypher-dialect variance per the Constraints section, and keeps each query independently testable.

### Task 6: `direction` parameter — reversed CALLS-chain query (no wire bridging yet)

**Files:**
- Modify: `src/codegraphcontext/tools/code_finder.py` (`analyze_impact` method, lines 1839–2109)
- Modify: `src/codegraphcontext/tool_definitions.py` (lines ~554–581)
- Modify: `src/codegraphcontext/tools/handlers/analysis_handlers.py` (lines 352–376)
- Test: `tests/unit/tools/test_impact_analysis.py`

**Interfaces:**
- Produces (consumed by Task 7, Task 9):
  ```python
  def analyze_impact(
      self,
      target: str,
      target_type: Optional[str] = None,
      repo_path: Optional[str] = None,
      depth: int = 3,
      direction: str = "upstream",     # "upstream" | "downstream" | "both"
      max_wire_hops: int = 2,          # unused until Task 7; accepted now so the signature is stable
  ) -> Dict[str, Any]:
  ```

- [ ] **Step 1: Write the failing test for `direction="downstream"`**

Extend `_FakeSession.run` in `test_impact_analysis.py` with a new `elif` branch matching the reversed-arrow Cypher text (exact text chosen in Step 3 below), returning a canned downstream row (e.g. `createOrder` → `validateOrder`, `CALLS`). Per the grounding note on this file's `recorder["queries"]` reset-per-session behavior, assert on the returned `res["propagation_paths"]` shape rather than on `recorder["queries"]` across multiple `with self.driver.session()` blocks.

```python
def test_analyze_impact_downstream_direction_reverses_query_arrow():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="AuthService", direction="downstream")
    assert res["direction"] == "downstream"
    assert len(res["propagation_paths"]) == 1
    # the canned downstream fixture row proves the reversed-arrow query ran
    assert res["propagation_paths"][0]["nodes"][-1]["name"] == "validateOrder"
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement**

Extract the existing upstream query string into a small helper:
```python
def _propagation_query(direction: str, depth: int) -> str:
    rel_types = "CALLS|INHERITS|IMPLEMENTS|INJECTS|DECORATED_BY|IMPORTS"
    if direction == "downstream":
        return f"""
            MATCH p = (target)-[:{rel_types}*1..{depth}]->(sink)
            WHERE target.uid IN $target_uids
            RETURN [n in nodes(p) | {{uid: n.uid, name: n.name, path: n.path, labels: labels(n)}}] as path_nodes,
                   [rel in relationships(p) | {{type: type(rel), line_number: coalesce(rel.line_number, 0)}}] as path_rels
        """
    return f"""
        MATCH p = (source)-[:{rel_types}*1..{depth}]->(target)
        WHERE target.uid IN $target_uids
        RETURN [n in nodes(p) | {{uid: n.uid, name: n.name, path: n.path, labels: labels(n)}}] as path_nodes,
               [rel in relationships(p) | {{type: type(rel), line_number: coalesce(rel.line_number, 0)}}] as path_rels
    """
```
Also add a `direction`-aware `_criticality_query` helper (reversed arrow for downstream: `MATCH (target)-[r:...]->()`, out-degree instead of in-degree). Validate `direction` against the 3 allowed literal values at the top of `analyze_impact`, raising/returning the same "not found"-style error shape used for a missing target if an invalid value is passed (do not let an unrecognized `direction` string silently fall through to the upstream default). For `direction in ("upstream", "downstream")`, run exactly one of the two queries (today's behavior, generalized). Add `"direction": direction` to the returned dict. Leave `direction="both"` unimplemented for now (raise `NotImplementedError` or route to Task 9 once it exists — mark with a `# TODO(Task 9)` comment referencing this plan) so this task's tests stay scoped to the two-single-direction case.

- [ ] **Step 4: Run test to verify it passes, and confirm the existing 3 tests in this file (symbol target, file target, not-found) still pass unmodified** — `pytest tests/unit/tools/test_impact_analysis.py -v`.

- [ ] **Step 5: Update the MCP tool schema and handler**

In `tool_definitions.py`, add to `analyze_impact`'s `inputSchema.properties`:
```python
"direction": {
    "type": "string",
    "enum": ["upstream", "downstream", "both"],
    "description": "Optional: traversal direction. 'upstream' (default) finds what depends on/calls into the target. 'downstream' finds what the target's output drives. 'both' returns one merged connected trace covering both.",
    "default": "upstream"
},
```
In `analysis_handlers.py::analyze_impact`, add `direction = args.get("direction", "upstream")` and thread it through to `code_finder.analyze_impact(..., direction=direction)`.

- [ ] **Step 6: Run the full unit test suite for this area once more** — `pytest tests/unit/tools/test_impact_analysis.py tests/unit/test_tool_definitions.py -v`.

- [ ] **Step 7: Commit**

```bash
git add src/codegraphcontext/tools/code_finder.py src/codegraphcontext/tool_definitions.py src/codegraphcontext/tools/handlers/analysis_handlers.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: direction param on analyze_impact (upstream/downstream), both deferred to next task"
```

---

### Task 7: Downstream wire-bridging BFS

**Files:**
- Modify: `src/codegraphcontext/tools/code_finder.py`
- Test: `tests/unit/tools/test_impact_analysis.py`

**Interfaces:**
- Consumes: Task 6's `_propagation_query`. Piece 1's `Topic`/`Subscription`/`PRODUCES_TO`/`CONSUMES_FROM` schema (also already-existing `Endpoint`/`SERVES`/`INVOKES` from PR #1728, same bridging shape).
- Produces (consumed by Task 9):
  ```python
  def _bridge_downstream(
      self, session, frontier_uids: List[str],
  ) -> Tuple[List[dict], List[str], List[dict]]:
      # returns (new_path_fragments, newly_discovered_function_uids, unresolved_crossings)
  ```

- [ ] **Step 1: Verify LadybugDB supports the needed query shape before writing new Cypher**

Run the *existing* (already-merged, already-in-production-use) upstream `propagation_query` by hand via `cgc` against the real `special-order-services` index, confirming the variable-length multi-relationship-type pattern (`*1..{depth}` over a `|`-joined relationship list) executes correctly against the actually-installed LadybugDB — this is already proven by Task 6 passing its tests against the fake driver, but the fake driver does not prove LadybugDB dialect compatibility, only that the Python code paths are correct. Run one real `cgc` query and record the result. If it fails or behaves unexpectedly, stop and resolve the dialect issue before proceeding — do not build the new bridging query on an unverified foundation.

- [ ] **Step 2: Write the failing test for a single wire-hop crossing a Topic via a Subscription**

Add a new canned branch to `_FakeSession.run` matching the bridging query text chosen in Step 4, returning one crossing: `createOrder` (frontier) → `PRODUCES_TO` → `Topic{oms-order-create-event}` → `HAS_SUBSCRIPTION` → `Subscription{oms-order-create-pull-subscription}` → `CONSUMES_FROM` (reversed) → `onOrderCreateEvent` (a function in a different `path`, simulating `oms-order-consumer`).

```python
def test_analyze_impact_downstream_crosses_pubsub_topic_to_consumer_repo():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="createOrder", direction="downstream", max_wire_hops=2)
    node_paths = {n["path"] for n in res["affected_nodes"]}
    assert any("oms-order-consumer" in p for p in node_paths), "cross-repo consumer node missing from downstream trace"
    assert res["cross_repo_hops"][0]["topic"] == "oms-order-create-event"
```

- [ ] **Step 3: Run test to verify it fails.**

- [ ] **Step 4: Implement `_bridge_downstream`**

Exact Cypher (two separate `MATCH` clauses run as two queries, per this piece's stated design decision — do not combine into one query with `OPTIONAL MATCH` chains without first confirming LadybugDB handles multi-stage `OPTIONAL MATCH` the same as Neo4j):
```cypher
-- Query A: producer -> topic -> subscription -> consumer (new-style, Subscription-aware)
MATCH (fn)-[:PRODUCES_TO]->(t:Topic)-[:HAS_SUBSCRIPTION]->(s:Subscription)<-[:CONSUMES_FROM]-(consumer_fn)
WHERE fn.uid IN $frontier_uids
RETURN fn.uid as producer_uid, t.name as topic_name, s.name as subscription_name,
       consumer_fn.uid as consumer_uid, consumer_fn.name as consumer_name, consumer_fn.path as consumer_path

-- Query B: producer -> topic -> consumer (legacy/Kafka-style, no Subscription node)
MATCH (fn)-[:PRODUCES_TO]->(t:Topic)<-[:CONSUMES_FROM]-(consumer_fn)
WHERE fn.uid IN $frontier_uids AND NOT (t)-[:HAS_SUBSCRIPTION]->()
RETURN fn.uid as producer_uid, t.name as topic_name, null as subscription_name,
       consumer_fn.uid as consumer_uid, consumer_fn.name as consumer_name, consumer_fn.path as consumer_path

-- Query C: SERVES/INVOKES bridge through Endpoint, same direction logic
MATCH (fn)-[:SERVES]->(ep:Endpoint)<-[:INVOKES]-(caller_fn)
WHERE fn.uid IN $frontier_uids
RETURN fn.uid as producer_uid, ep.path as topic_name, null as subscription_name,
       caller_fn.uid as consumer_uid, caller_fn.name as consumer_name, caller_fn.path as consumer_path
```
Note Query C's naming reuses the same column names as A/B for a uniform row shape in Python, even though "producer"/"consumer" mean "endpoint owner"/"endpoint caller" there — document this with an inline comment since it's a deliberate shape-reuse, not a copy-paste error. The BFS loop in `analyze_impact`: after computing the standard downstream `propagation_paths` for the current frontier (Task 6's query), collect the set of leaf-node uids from those paths as the new frontier, run the three bridge queries against it, and for every crossing found, add a `cross_repo_hops` entry (`{"topic": topic_name, "subscription": subscription_name, "from_uid":..., "to_uid":..., "to_name":..., "to_path":...}`) and add `consumer_uid` to the function-uid set carried into the next loop iteration. Repeat for `max_wire_hops` iterations or until a frontier produces no new crossings, whichever comes first. After each wire hop, re-run the standard CALLS-chain downstream query from the newly-crossed function(s) too, so that code *inside* the consumer continues to be traced, not just the entry point — this is the detail that makes the trace genuinely "end-to-end" rather than stopping exactly at the consumer's receiving method.

Per the anti-goal and Review Focus item 2: if a `Topic` is reached with zero `HAS_SUBSCRIPTION` edges and zero direct `CONSUMES_FROM` edges (a true dead end, matching the known real case of `order-inv-inquiry-oms-leg-analysis-event` feeding only a BigQuery subscription with no indexed consumer function), record this explicitly: `unresolved_crossings.append({"topic": topic_name, "reason": "no indexed consumer function found for this topic/subscription"})` rather than just stopping silently.

- [ ] **Step 5: Run test to verify it passes.**

- [ ] **Step 6: Write the failing test for the Review Focus item — a topic with no consumer is reported, not silent**

```python
def test_downstream_trace_reports_unresolved_crossing_for_dead_end_topic():
    finder, recorder = _make_finder()
    # fixture: a topic reached with no Subscription and no direct CONSUMES_FROM edge at all
    res = finder.analyze_impact(target="publishInventoryAudit", direction="downstream")
    assert any(c["topic"] == "order-inv-inquiry-oms-leg-analysis-event" for c in res["unresolved_crossings"])
```

- [ ] **Step 7: Run test to verify it fails, then wire the fake-session branch + confirm Step 4's unresolved-crossing logic covers it.**

- [ ] **Step 8: Run test to verify it passes.**

- [ ] **Step 9: Commit**

```bash
git add src/codegraphcontext/tools/code_finder.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: downstream wire-bridging BFS across PRODUCES_TO/CONSUMES_FROM/SERVES/INVOKES"
```

---

### Task 8: Upstream wire-bridging BFS

**Files:**
- Modify: `src/codegraphcontext/tools/code_finder.py`
- Test: `tests/unit/tools/test_impact_analysis.py`

**Interfaces:**
- Consumes: Task 7's bridge-query pattern (mirrored, reversed).
- Produces (consumed by Task 9): `_bridge_upstream(self, session, frontier_uids: List[str]) -> Tuple[List[dict], List[str], List[dict]]` — same return shape as `_bridge_downstream`.

- [ ] **Step 1: Write the failing test for upstream crossing — "what feeds into this consumer"**

```python
def test_analyze_impact_upstream_crosses_pubsub_subscription_to_producer_repo():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="onOrderCreateEvent", direction="upstream", max_wire_hops=2)
    node_paths = {n["path"] for n in res["affected_nodes"]}
    assert any("special-order-services" in p for p in node_paths), "producer repo missing from upstream trace"
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `_bridge_upstream`** — same three-query shape as Task 7's `_bridge_downstream` with arrows reversed (`(consumer_fn)-[:CONSUMES_FROM]->(s:Subscription)<-[:HAS_SUBSCRIPTION]-(t:Topic)<-[:PRODUCES_TO]-(fn)`, matched against the frontier on the **consumer** side this time), feeding into the same BFS loop structure used in Task 7 but called from the `direction == "upstream"` branch of `analyze_impact`. Reuse the same `unresolved_crossings` convention for a `Subscription`/`Topic` reached with no producer indexed.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Run the full test file to confirm no regressions** — `pytest tests/unit/tools/test_impact_analysis.py -v`.

- [ ] **Step 6: Commit**

```bash
git add src/codegraphcontext/tools/code_finder.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: upstream wire-bridging BFS (mirrors downstream, Task 7)"
```

---

### Task 9: `direction="both"` — merged single connected trace

**Files:**
- Modify: `src/codegraphcontext/tools/code_finder.py`
- Test: `tests/unit/tools/test_impact_analysis.py`

**Interfaces:**
- Consumes: Task 6's single-direction query, Task 7's `_bridge_downstream`, Task 8's `_bridge_upstream`.
- Produces: the final `analyze_impact` return shape for `direction="both"`:
  ```python
  {
      "target": {...},
      "direction": "both",
      "nodes": List[dict],            # deduplicated by uid; each has a "role": "target" | "upstream" | "downstream"
      "edges": List[dict],            # deduplicated by (source_uid, target_uid, type); each has a "direction" tag
      "cross_repo_hops": List[dict],  # merged from both BFS directions
      "unresolved_crossings": List[dict],
      "impact_score": float,
      "risk_level": str,
      "risk_breakdown": dict,
      "explanations": List[dict],
  }
  ```
  This is the return shape Task 13/16 (Piece 4) will add `risk_score`/`risk_factors` fields to.

- [ ] **Step 1: Write the failing test for the merged-trace requirement itself**

This is the test that pins Decision #75-81 of the design doc — the literal requirement that `both` is one connected trace, not two lists the caller has to stitch:

```python
def test_analyze_impact_both_returns_one_merged_connected_trace_not_two_lists():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="validateOrder", direction="both")
    assert res["direction"] == "both"
    assert "nodes" in res and "edges" in res
    assert "propagation_paths" not in res, "both must not just concatenate two separate upstream/downstream result sets"
    target_node = next(n for n in res["nodes"] if n["role"] == "target")
    assert target_node["name"] == "validateOrder"
    roles = {n["role"] for n in res["nodes"]}
    assert "upstream" in roles and "downstream" in roles, "both must include nodes from both directions in the same node list"
    # Every edge must connect two uids present in the same `nodes` list -- this is what makes it "connected"
    node_uids = {n["uid"] for n in res["nodes"]}
    for e in res["edges"]:
        assert e["source_uid"] in node_uids and e["target_uid"] in node_uids
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement the `direction == "both"` branch**

Run the upstream query+BFS (Task 6/8's logic) and the downstream query+BFS (Task 6/7's logic) against the same `target_uids`, independently. Merge: build `nodes` as a dict keyed by `uid`, seeding the target node(s) with `role="target"`, then every node appearing only in the upstream result gets `role="upstream"`, only in downstream gets `role="downstream"`; a node appearing in both (possible if a path loops back) keeps `role="target"` priority, else `"both"`. Build `edges` as a dict keyed by `(source_uid, target_uid, type)` from both result sets' relationship lists, each tagged `"direction": "upstream"|"downstream"`. Concatenate and deduplicate (by `(topic, subscription)` pair) the two directions' `cross_repo_hops` and `unresolved_crossings` lists. Compute `impact_score`/`risk_level`/`risk_breakdown` once, over the **union** of affected nodes from both directions (not computed twice and averaged/summed) — reuse the existing scoring helpers against the merged node set.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the literal acceptance-criterion shape (small-scale version)**

This previews Task 22's full acceptance test at the fake-driver/unit level, to catch a structural mistake before the expensive real-repo run:

```python
def test_analyze_impact_both_on_create_order_includes_consumer_repo_and_terraform_node():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="createOrder", direction="both", max_wire_hops=2)
    node_types_by_repo = {n["path"]: n["type"] for n in res["nodes"]}
    assert any("oms-order-consumer" in p for p in node_types_by_repo), "consumer repo missing"
    assert any(n["type"] == "Class" and "terra-projects-oms" in n["path"] for n in res["nodes"]), "Terraform node missing (requires Piece 3's BACKED_BY stitching to be reachable too -- extend the fake fixture once Task 12 lands)"
```

Mark this test `@pytest.mark.skip(reason="requires Task 12's BACKED_BY edge in the bridging query — un-skip when Piece 3 lands")` for now; un-skip and extend the fake fixture with a `BACKED_BY` edge once Task 12 is done (that task's own steps reference this).

- [ ] **Step 6: Run the full test file once more to confirm no regressions** — `pytest tests/unit/tools/test_impact_analysis.py -v`.

- [ ] **Step 7: Commit**

```bash
git add src/codegraphcontext/tools/code_finder.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: direction=both returns one merged connected trace (closes Piece 2)"
```

---

# Piece 3 — Terraform Stitching

Depends on Piece 1 (needs the `Subscription`/`Topic` schema to exist). Two phases per Decision in the design doc: topic-name-only stitching ships first (works with existing HCL primitives), `service_account` cross-check is a fast-follow once object-list parsing exists.

### Task 10: HCL generic attribute capture — `topic` on `pull_subscriptions` string elements

**Files:**
- Modify: `src/codegraphcontext/tools/languages/hcl.py`
- Test: `tests/unit/parsers/test_hcl_parser.py`

**Interfaces:**
- Produces (consumed by Task 12): the `_block_node()` dict gains a new key whenever the block's own body has a literal `topic = "..."` attribute, e.g. `node["topic"] = self._literal(attributes.get("topic"))` (or `None` if absent) — lands as a plain `Class.topic` property via the existing generic `SET n += row` writer mechanism, no `writer.py` changes needed (confirmed during grounding).

- [ ] **Step 1: Write the failing test**

```python
def test_module_block_with_topic_attribute_captures_it_as_a_property(tmp_path):
    source = """
    module "oms_order_event_pubsub_config" {
      source = "../../modules/pubsub"
      topic  = "oms-order-create-event"
    }
    """
    result = parse(parser, tmp_path, "pubsub.tf", source)
    module_node = next(c for c in result["classes"] if c["name"].endswith("oms_order_event_pubsub_config"))
    assert module_node["topic"] == "oms-order-create-event"
```

- [ ] **Step 2: Run test to verify it fails** — `pytest tests/unit/parsers/test_hcl_parser.py -v`, expect `KeyError: 'topic'`.

- [ ] **Step 3: Implement** — in `_block_node()`, add `"topic": self._literal(attributes.get("topic"))` alongside the existing `"docstring": self._literal(attributes.get("description"))` line, using the exact same `_literal()` helper (already proven to return `None` cleanly for an absent/non-string attribute — no new null-handling needed).

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Add the integration-level proof, extending `test_hcl_indexing_smoke.py`'s pattern**

```python
# in a new or extended integration test, following test_hcl_indexing_smoke.py's exact real-KuzuDB pattern
def test_hcl_pubsub_module_topic_attribute_lands_as_queryable_property(tmp_path, ...):
    # index a small fixture containing the pull_subscriptions module shape from the design doc's ground truth
    ...
    with manager.get_driver().session() as session:
        rows = session.run("MATCH (c:Class) WHERE c.topic = 'oms-order-create-event' RETURN c.name AS name").data()
        assert len(rows) == 1
```

- [ ] **Step 6: Run test to verify it passes.**

- [ ] **Step 7: Commit**

```bash
git add src/codegraphcontext/tools/languages/hcl.py tests/unit/parsers/test_hcl_parser.py tests/integration/test_hcl_indexing_smoke.py
git commit -m "feat: capture HCL module 'topic' attribute as a Class node property"
```

---

### Task 11: HCL object-list parsing — `service_account` per `pull_subscriptions` entry (fast-follow)

**Files:**
- Modify: `src/codegraphcontext/tools/languages/hcl.py`
- Test: `tests/unit/parsers/test_hcl_parser.py`

**Interfaces:**
- Produces (consumed by Task 12, optionally): a new helper
  ```python
  def _object_list(self, attribute) -> List[Dict[str, Optional[str]]]:
      """Each element of a list-of-objects attribute as a dict of its string-valued keys."""
  ```
  and a new `_block_node()` key, e.g. `node["pull_subscriptions"] = json.dumps(self._object_list(attributes.get("pull_subscriptions")))` — pre-serialized to a JSON string per the grounding note that the writer's batch type-coercion coerces list-typed properties to JSON strings; storing it pre-serialized here avoids relying on that inference running the way this task expects.

- [ ] **Step 1: Write the failing test**

```python
def test_pull_subscriptions_object_list_captures_name_and_service_account(tmp_path):
    source = """
    module "oms_order_event_pubsub_config" {
      topic = "oms-order-create-event"
      pull_subscriptions = [
        { name = "oms_order_create_pull_subscription", service_account = "oms-order-consumer-sa@azdv1-ordfl-ordrmgmt.iam.gserviceaccount.com" }
      ]
    }
    """
    result = parse(parser, tmp_path, "pubsub.tf", source)
    module_node = next(c for c in result["classes"] if c["name"].endswith("oms_order_event_pubsub_config"))
    subs = json.loads(module_node["pull_subscriptions"])
    assert subs == [{"name": "oms_order_create_pull_subscription",
                      "service_account": "oms-order-consumer-sa@azdv1-ordfl-ordrmgmt.iam.gserviceaccount.com"}]
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `_object_list`**

Walk the same `collection_value` → `tuple` structure `_literals()` already walks (reuse that traversal, do not reimplement it independently), but instead of skipping non-string `expression` children, detect when an element's `expression` wraps an `object`/`collection_value` node representing key-value pairs (an HCL object literal: `{ name = "x", service_account = "y" }`) and recurse into it using `self._attributes()`-equivalent logic (an object literal's internal structure is attribute-like pairs, not a nested block — confirm the exact tree-sitter node shape for an HCL object literal by inspecting the parse tree for the test fixture above before writing the walk, since this is new, unproven parsing territory per the design doc's own "no comparable pattern exists" callout). For each key found, call `self._string()` on its value expression (same as `_literals()` already does per-element) to get a plain string; build one `Dict[str, Optional[str]]` per list element.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test confirming the existing string-list case (`paths = ["../vpc", "../db"]`) is unaffected**

```python
def test_string_element_list_still_works_after_object_list_support_added(tmp_path):
    # re-run the existing TestTerragruntDependencies fixture/assertion unchanged
    ...
```

This guards against the natural risk of touching shared traversal code: confirm `_literals()` itself is untouched/still correct, only a new sibling helper was added.

- [ ] **Step 6: Run test to verify it passes, then run the full HCL test file** — `pytest tests/unit/parsers/test_hcl_parser.py -v`.

- [ ] **Step 7: Commit**

```bash
git add src/codegraphcontext/tools/languages/hcl.py tests/unit/parsers/test_hcl_parser.py
git commit -m "feat: parse pull_subscriptions object-list for service_account (HCL fast-follow)"
```

---

### Task 12: Terraform stitcher + `stitch_infrastructure` MCP tool

**Files:**
- Create: `src/codegraphcontext/wire/terraform_stitcher.py`
- Modify: `src/codegraphcontext/tool_definitions.py`
- Modify: `src/codegraphcontext/tools/handlers/analysis_handlers.py` (or a new `infra_handlers.py` if that file doesn't already group naturally — check the existing `handlers/` package's grouping convention first and follow it)
- Modify: `src/codegraphcontext/server.py`
- Test: `tests/unit/wire/test_terraform_stitcher.py`

**Interfaces:**
- Consumes: Task 10's `Class.topic` property, Task 11's `Class.pull_subscriptions` property, Piece 1's `Topic`/`Subscription` schema.
- Produces:
  ```python
  @dataclass
  class StitchStats:
      topics_backed: int = 0
      subscriptions_backed: int = 0
      unmatched_topics: List[str] = field(default_factory=list)

  def stitch_topics_to_terraform(driver: Any, *, graph_name: Optional[str] = None) -> StitchStats: ...
  ```
  New edge: `(Topic)-[:BACKED_BY]->(terraform_class_node)`, and `(Subscription)-[:BACKED_BY]->(terraform_class_node)` when the `service_account`-level match is available.

- [ ] **Step 1: Write the failing test for topic-name stitching**

```python
def test_stitch_topics_to_terraform_matches_by_topic_name(fake_driver_with_topic_and_tf_class):
    stats = stitch_topics_to_terraform(fake_driver_with_topic_and_tf_class)
    assert stats.topics_backed == 1
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `stitch_topics_to_terraform` — phase 1 (topic name match)**

```cypher
MATCH (t:Topic), (c:Class {node_type: "module"})
WHERE c.topic = t.name
MERGE (t)-[:BACKED_BY]->(c)
RETURN count(*) as matched
```
Run via the same `execute_write_operation(driver, backend, _work)` pattern `writer.py` uses (lazy import, same as Task 4's reuse). Then a second pass for unmatched topics (`MATCH (t:Topic) WHERE NOT (t)-[:BACKED_BY]->() RETURN t.name`) populated into `StitchStats.unmatched_topics` — per the anti-goal, a `Topic` with no Terraform match is reported explicitly, not silently left dangling with no indication anything was checked.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the `service_account` cross-check (phase 2, using Task 11's data)**

```python
def test_stitch_subscriptions_to_terraform_matches_by_service_account(fake_driver_with_subscription_and_pull_subscriptions_json):
    stats = stitch_topics_to_terraform(fake_driver_with_subscription_and_pull_subscriptions_json)
    assert stats.subscriptions_backed == 1
```

- [ ] **Step 6: Run test to verify it fails.**

- [ ] **Step 7: Implement phase 2** — a second Cypher pass (or Python-side JSON parsing of each Terraform `Class.pull_subscriptions` property, since matching a `Subscription.name` against a JSON-encoded list's `name` field is awkward in pure Cypher on an embedded backend without a native JSON function — verify whether LadybugDB's Cypher has a JSON-parse function before deciding; if not, fetch candidate `(Class {pull_subscriptions: NOT NULL})` rows into Python, `json.loads` each, and issue a second small `MERGE` per matched `(subscription_name, terraform_class_uid)` pair instead of a single bulk Cypher statement).

- [ ] **Step 8: Run test to verify it passes.**

- [ ] **Step 9: Register `stitch_infrastructure` as an MCP tool**

`tool_definitions.py`: `"stitch_infrastructure": {"name": "stitch_infrastructure", "description": "Connect wire-coupling Topic/Subscription nodes to the real Terraform resources that define them. Run once after indexing both the code repos and the infrastructure (Terraform) repo.", "inputSchema": {"type": "object", "properties": {"graph_name": _GRAPH_NAME_PROP}}}`. Handler calls `stitch_topics_to_terraform(self.driver, graph_name=...)` and returns `{"success": True, "stats": {...}}`. Wire into `server.py`'s `tool_map` and a `stitch_infrastructure_tool(self, **args)` wrapper, following the exact 4-edit convention already used for `analyze_impact`.

- [ ] **Step 10: Un-skip Task 9's skipped test**

Return to `tests/unit/tools/test_impact_analysis.py::test_analyze_impact_both_on_create_order_includes_consumer_repo_and_terraform_node`, remove the `@pytest.mark.skip`, extend the fake fixture with a `BACKED_BY` edge in the bridge-query fake-session branch, and confirm it now passes.

- [ ] **Step 11: Run the full wire + tools test suites** — `pytest tests/unit/wire/ tests/unit/tools/ -v`.

- [ ] **Step 12: Commit**

```bash
git add src/codegraphcontext/wire/terraform_stitcher.py src/codegraphcontext/tool_definitions.py src/codegraphcontext/tools/handlers/ src/codegraphcontext/server.py tests/unit/wire/test_terraform_stitcher.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: stitch_infrastructure tool — Topic/Subscription BACKED_BY Terraform (closes Piece 3)"
```

---

# Piece 4 — Risk-Scoring Port

Largely independent of Pieces 1–3 (extends `analyze_impact`'s scoring, and adds a new diff-level tool). Per Decision #5/#6: security keywords, caller-count, churn (with full infra), and all diff-parsing/orchestration logic are ported to **full fidelity** — these are pure-Python/Cypher, no new schema needed. Per Decision #4: community-crossing and test-coverage are **lightweight live-query proxies**, computed at call time rather than via persisted new node/edge types — this keeps Piece 4 fully independent of any new indexing phase (YAGNI: no re-indexing of already-indexed repos required to get these scores).

### Task 13: `risk_scoring.py` — security keywords, caller-count, community-crossing proxy

**Files:**
- Create: `src/codegraphcontext/tools/risk_scoring.py`
- Test: `tests/unit/tools/test_risk_scoring.py`

**Interfaces:**
- Produces (consumed by Task 16):
  ```python
  SECURITY_KEYWORDS: frozenset[str] = frozenset({
      "auth", "login", "password", "token", "session", "crypt", "secret",
      "credential", "permission", "sql", "query", "execute", "connect",
      "socket", "request", "http", "sanitize", "validate", "encrypt",
      "decrypt", "hash", "sign", "verify", "admin", "privilege",
  })

  def security_sensitivity_score(name: str, qualified_name: str) -> float:
      """0.20 if name or qualified_name contains a security keyword (case-insensitive substring), else 0.0."""

  def caller_count_score(caller_count: int) -> float:
      """min(caller_count / 20.0, 0.10) -- exact port of code-review-graph's formula."""

  def community_crossing_score(target_path: str, caller_paths: List[str]) -> float:
      """0.05 per caller whose top-level module/package directory differs from the target's, capped at 0.15.
      Proxy for code-review-graph's Leiden-community-crossing factor -- see Decision #4."""
  ```

- [ ] **Step 1: Write the failing tests for each pure function** (one test per function, exact values from the port):

```python
def test_security_sensitivity_score_matches_keyword():
    assert security_sensitivity_score("validateOrder", "com.az.oms.validateOrder") == 0.20

def test_security_sensitivity_score_no_match():
    assert security_sensitivity_score("createOrder", "com.az.oms.createOrder") == 0.0

def test_caller_count_score_caps_at_point_one():
    assert caller_count_score(1000) == 0.10
    assert caller_count_score(10) == 0.5 * 0.10  # 10/20 = 0.5, * nothing -- exact: 10/20.0 = 0.5... wait cap is 0.10 so recompute: min(10/20.0, 0.10)
```

(Implementer note embedded directly: `caller_count_score(10)` = `min(10/20.0, 0.10)` = `min(0.5, 0.10)` = `0.10` — write the actual assertion as `== 0.10`, the above draft miscalculates; use `caller_count_score(1) == 0.05` as the non-saturated example instead: `min(1/20.0, 0.10) == 0.05`.)

```python
def test_caller_count_score_below_cap():
    assert caller_count_score(1) == 0.05
    assert caller_count_score(0) == 0.0

def test_community_crossing_score_counts_cross_module_callers_capped_at_point_fifteen():
    target_path = "src/main/java/com/az/oms/order/OrderValidator.java"
    caller_paths = [
        "src/main/java/com/az/oms/order/OrderService.java",   # same top-level module (order) -- doesn't count
        "src/main/java/com/az/oms/shipping/ShippingService.java",  # different module -- counts
        "src/main/java/com/az/oms/payment/PaymentService.java",    # different module -- counts
        "src/main/java/com/az/oms/inventory/InventoryService.java",# different module -- counts
        "src/main/java/com/az/oms/vendor/VendorService.java",      # 4th crossing -- still counts, pre-cap
    ]
    # 4 crossings * 0.05 = 0.20, capped at 0.15
    assert community_crossing_score(target_path, caller_paths) == 0.15
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement the three functions** per the exact formulas above. For `community_crossing_score`, the module/package-boundary heuristic (per the "what's left to the builder" item) is: split both paths on `/`, take everything between the language-root marker (`src/main/java` or `src/main/kotlin`, matching the existing `DEFAULT_JAVA_SOURCE_DIRS` convention already used elsewhere in this codebase — reuse that constant rather than hardcoding it again) and the filename, then compare the **first remaining segment** (the top-level package directory under the source root) — two paths "cross" if this first segment differs.

- [ ] **Step 4: Run tests to verify they pass.**

- [ ] **Step 5: Commit**

```bash
git add src/codegraphcontext/tools/risk_scoring.py tests/unit/tools/test_risk_scoring.py
git commit -m "feat: port security-keyword, caller-count, community-crossing-proxy risk factors"
```

---

### Task 14: `risk_scoring.py` — churn infrastructure (full port)

**Files:**
- Modify: `src/codegraphcontext/tools/risk_scoring.py`
- Test: `tests/unit/tools/test_risk_scoring.py`

**Interfaces:**
- Produces (consumed by Task 16, Task 18):
  ```python
  CHURN_OK = "ok"
  CHURN_UNAVAILABLE = "unavailable"
  CHURN_OFF = "off"

  def clear_churn_cache() -> None: ...
  def compute_file_churn_with_status(repo_root: str, window_days: Optional[int] = None) -> Tuple[Dict[str, int], str]: ...
  def compute_file_churn(repo_root: str, window_days: Optional[int] = None) -> Dict[str, int]: ...
  def churn_score(commit_count: int) -> float:
      """min(commit_count / _CHURN_SATURATION, 1.0) * _CHURN_WEIGHT"""
  ```

This is a near-verbatim port of code-review-graph's `changes.py` churn section (already captured in full during grounding) — exact constants: `_CHURN_SATURATION = 10.0`, `_CHURN_WEIGHT = 0.15`, `_CHURN_TIMEOUT` default `5.0` (env `CGC_CHURN_TIMEOUT` — **rename the env var prefix from `CRG_` to `CGC_`**, since this is now CGC's own config surface, not code-review-graph's; keep every other name identical), `_CHURN_MAX_COMMITS` default `2000` (env `CGC_CHURN_MAX_COMMITS`), `_CHURN_CACHE_MAX_ENTRIES = 32`, default trailing window `90` days (env `CGC_CHURN_WINDOW_DAYS`).

- [ ] **Step 1: Write the failing test for the cache-key shape**

```python
def test_churn_cache_key_is_repo_head_window(tmp_path):
    # tmp_path is a real git repo (initialize with subprocess git init + one commit in the test setup)
    counts1, status1 = compute_file_churn_with_status(str(tmp_path), window_days=90)
    assert status1 == CHURN_OK
    # second call within the same process must hit cache, not re-invoke git -- verify via monkeypatching
    # subprocess.run to raise if called a second time with the same cache key
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Port the churn cache + `_parse_numstat` + `compute_file_churn_with_status` + `compute_file_churn`** verbatim from the exact code captured during grounding (the full function bodies were already read in full — copy them, renaming only the `CRG_`→`CGC_` env var prefixes and the module-level docstring references to `code-review-graph`).

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the sticky-unavailable cache (Review Focus item 5)**

```python
def test_churn_degrades_to_unavailable_for_non_git_directory(tmp_path):
    counts, status = compute_file_churn_with_status(str(tmp_path), window_days=90)  # tmp_path has no .git
    assert status == CHURN_UNAVAILABLE
    assert counts == {}
    # second call for the same (repo, window) must short-circuit without touching git again (sticky)
    counts2, status2 = compute_file_churn_with_status(str(tmp_path), window_days=90)
    assert status2 == CHURN_UNAVAILABLE
```

- [ ] **Step 6: Run test to verify it fails, then confirm Step 3's port already satisfies it** (the sticky-unavailable logic is part of the ported function — this step should pass once Step 3 is correctly done; if it doesn't, the port is incomplete).

- [ ] **Step 7: Run test to verify it passes. Call `clear_churn_cache()` in a test teardown/fixture to avoid cross-test cache pollution** (add a `@pytest.fixture(autouse=True)` in this test file that calls `clear_churn_cache()` before each test).

- [ ] **Step 8: Implement `churn_score(commit_count: int) -> float`** — one-line port of the formula, trivial once the constants exist.

- [ ] **Step 9: Run the full risk-scoring test file** — `pytest tests/unit/tools/test_risk_scoring.py -v`.

- [ ] **Step 10: Commit**

```bash
git add src/codegraphcontext/tools/risk_scoring.py tests/unit/tools/test_risk_scoring.py
git commit -m "feat: port churn infrastructure (cache, sticky-unavailable, trailing window, max-commits cap)"
```

---

### Task 15: `risk_scoring.py` — test-coverage proxy (live Cypher heuristic)

**Files:**
- Modify: `src/codegraphcontext/tools/risk_scoring.py`
- Test: `tests/unit/tools/test_risk_scoring.py`

**Interfaces:**
- Produces (consumed by Task 16):
  ```python
  def find_test_coverage_count(session, function_name: str, function_path: str) -> int:
      """Count of Function nodes matching a Java/JUnit test-naming convention
      (test<Name> / <name>Test / <Name>Test) that are co-located under the
      mirrored src/test tree for function_path's package. Lightweight proxy
      for a real TESTED_BY edge -- see Decision #4."""

  def test_coverage_score(test_count: int) -> float:
      """0.30 - (min(test_count / 5.0, 1.0) * 0.25) -- exact port of the formula."""
  ```

- [ ] **Step 1: Write the failing test for the naming/colocation heuristic**

```python
def test_find_test_coverage_count_matches_colocated_junit_naming(fake_session_with_main_and_test_functions):
    count = find_test_coverage_count(fake_session_with_main_and_test_functions, "validateOrder", "src/main/java/com/az/oms/OrderValidator.java")
    assert count == 2  # fixture has testValidateOrder and validateOrderTest, both under src/test/java/com/az/oms/
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `find_test_coverage_count`**

Cypher: compute the expected test-tree path by replacing `src/main/java` with `src/test/java` (string substitution on `function_path`, reusing `DEFAULT_JAVA_SOURCE_DIRS`'s known directory convention) and the expected test-name variants (`f"test{name[0].upper()}{name[1:]}"`, `f"{name}Test"`, `f"{name[0].upper()}{name[1:]}Test"`), then query:
```cypher
MATCH (fn:Function)
WHERE fn.path STARTS WITH $test_tree_prefix AND fn.name IN $candidate_names
RETURN count(fn) as count
```
Scope this explicitly to Java/JUnit naming only, per Decision #4 — do not attempt to generalize to other languages in this task.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Implement `test_coverage_score`** — one-line port of the exact formula.

- [ ] **Step 6: Write the failing test for the formula's two end-points**

```python
def test_test_coverage_score_zero_tests_is_point_three():
    assert test_coverage_score(0) == 0.30

def test_test_coverage_score_five_or_more_tests_floors_at_point_zero_five():
    assert test_coverage_score(5) == 0.05
    assert test_coverage_score(100) == 0.05
```

- [ ] **Step 7: Run tests to verify they pass** (should already pass if Step 5 is correct — this step is confirmation, not new implementation).

- [ ] **Step 8: Commit**

```bash
git add src/codegraphcontext/tools/risk_scoring.py tests/unit/tools/test_risk_scoring.py
git commit -m "feat: live-query test-coverage proxy (Java/JUnit naming+colocation heuristic)"
```

---

### Task 16: Wire `compute_risk_score` into `analyze_impact`

**Files:**
- Modify: `src/codegraphcontext/tools/code_finder.py`
- Test: `tests/unit/tools/test_impact_analysis.py`

**Interfaces:**
- Consumes: Task 13's `security_sensitivity_score`/`caller_count_score`/`community_crossing_score`, Task 14's `compute_file_churn_with_status`/`churn_score`, Task 15's `find_test_coverage_count`/`test_coverage_score`.
- Produces: new `analyze_impact` return fields `risk_score: float` (0.0–1.0, code-review-graph-style weighted sum) and `risk_factors: Dict[str, float]` (per-factor breakdown), added **alongside** the existing `impact_score`/`risk_level`/`risk_breakdown` fields (both scoring systems coexist — see design rationale below), plus `churn_status: str` surfaced at the top level.

**Design decision made here**: the existing `impact_score`/`risk_level`/`risk_breakdown` (depth/module/criticality/churn, 1–10 scale) is **kept as-is** — it's already shipped, tested, and working from PR #1298. The new `compute_risk_score` (0.0–1.0 scale, code-review-graph's weighted-factor formula) is **added** as new fields on the same response, not a replacement. This avoids regressing the existing tool's contract while delivering the ported scoring Javier asked for.

- [ ] **Step 1: Write the failing test**

```python
def test_analyze_impact_includes_ported_risk_score_and_factors():
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="AuthService")
    assert "risk_score" in res and 0.0 <= res["risk_score"] <= 1.0
    assert set(res["risk_factors"].keys()) >= {
        "security_sensitivity", "caller_count", "community_crossing", "test_coverage", "churn"
    }
    assert "churn_status" in res
    # the existing fields must be unchanged
    assert "impact_score" in res and "risk_level" in res and "risk_breakdown" in res
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement**

In `analyze_impact`, after the existing scoring block, compute the five `risk_scoring.py` factors for the primary target node (caller count already available via the existing `criticality_query`'s `in_degree`; community-crossing needs the caller node paths, fetchable via `MATCH (c)-[:CALLS|...]->(target) WHERE target.uid IN $target_uids RETURN c.path` — a cheap addition to the existing criticality query rather than a new one); test coverage via Task 15's function against `self.driver.session()`; churn via Task 14's function against `repo_root` (already resolved earlier in the method) with `include_churn` gated by a new optional `include_churn: bool = True` parameter (default on, since Decision #5 says no reason to cut corners, but kept togglable since churn is the one factor that shells out to `git` and could be slow/unavailable per Review Focus item 5). Sum the five factors, clamp `[0.0, 1.0]`, round to 4 decimals — exact port of `compute_risk_score`'s final line.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the Review Focus item — churn unavailable doesn't fail the whole call**

```python
def test_analyze_impact_degrades_gracefully_when_repo_has_no_git_history(tmp_path, monkeypatch):
    # repo_path points at a real tmp_path with no .git
    finder, recorder = _make_finder()
    res = finder.analyze_impact(target="AuthService", repo_path=str(tmp_path))
    assert res["churn_status"] == "unavailable"
    assert res["risk_score"] >= 0.0  # call still succeeds and returns a usable score, just without the churn term
```

- [ ] **Step 6: Run test to verify it fails, then confirm the churn call is wrapped so its unavailability doesn't propagate as an exception** (it already shouldn't, per Task 14's port — this step proves the integration point didn't accidentally re-introduce a raise).

- [ ] **Step 7: Run test to verify it passes, then run the full impact-analysis test file** — `pytest tests/unit/tools/test_impact_analysis.py -v`.

- [ ] **Step 8: Commit**

```bash
git add src/codegraphcontext/tools/code_finder.py tests/unit/tools/test_impact_analysis.py
git commit -m "feat: wire ported risk-score factors into analyze_impact output"
```

---

### Task 17: `risk_scoring.py` — diff parsing (C-quoted paths, SVN, ref-injection safety, line-range mapping)

**Files:**
- Modify: `src/codegraphcontext/tools/risk_scoring.py`
- Test: `tests/unit/tools/test_risk_scoring.py`

**Interfaces:**
- Produces (consumed by Task 18):
  ```python
  def parse_diff_ranges(repo_root: str, base: str = "HEAD~1", *, require_vcs: bool = False) -> Dict[str, List[Tuple[int, int]]]: ...
  def parse_git_diff_ranges(repo_root: str, base: str, *, require_vcs: bool = False) -> Dict[str, List[Tuple[int, int]]]: ...
  def parse_svn_diff_ranges(repo_root: str, rev_range: Optional[str], *, require_vcs: bool = False) -> Dict[str, List[Tuple[int, int]]]: ...
  def map_changes_to_nodes(session, changed_ranges: Dict[str, List[Tuple[int, int]]]) -> List[dict]:
      """Returns Function/Class node dicts whose [line_number, end_line] overlaps a changed range."""
  ```

This is a near-verbatim port of the exact functions already captured in full during grounding (`_unquote_c_path`, `_diff_header_path`, `_POST_IMAGE_HEADER`, `_parse_unified_diff`, `_SAFE_GIT_REF`, `_SAFE_SVN_REV`, `parse_diff_ranges`'s `.svn`-directory-detection dispatch) — port them with identical logic, adapting only `map_changes_to_nodes` to query CGC's graph (via `session`) instead of code-review-graph's `GraphStore` (which doesn't exist here): the overlap test (`node.line_start <= end and node.line_end >= start`) stays identical, but the node-fetch becomes a Cypher `MATCH (n) WHERE n.path = $file_path AND (n:Function OR n:Class) RETURN n.uid, n.name, n.path, n.line_number, n.end_line` per changed file, replacing `store.get_nodes_by_file`.

- [ ] **Step 1: Write the failing test for C-quoted path decoding**

```python
def test_unquote_c_path_decodes_octal_escapes():
    # port the exact code-review-graph test fixture value for a non-ASCII filename
    assert _unquote_c_path('"caf\\303\\251.py"') == "café.py"
```

- [ ] **Step 2: Run test to verify it fails, then port `_unquote_c_path` verbatim.**

- [ ] **Step 3: Run test to verify it passes.**

- [ ] **Step 4: Write the failing test for space-containing paths via the TAB-separator rule**

```python
def test_diff_header_path_handles_space_via_tab_separator():
    assert _diff_header_path("b/src/my file.py\t") == "src/my file.py"
```

- [ ] **Step 5: Run test to verify it fails, then port `_diff_header_path` and `_POST_IMAGE_HEADER` verbatim.**

- [ ] **Step 6: Run test to verify it passes.**

- [ ] **Step 7: Write the failing test for the ref-injection-safety guard**

```python
def test_parse_git_diff_ranges_rejects_unsafe_ref(tmp_path):
    ranges = parse_git_diff_ranges(str(tmp_path), base="HEAD; rm -rf /")
    assert ranges == {}
```

- [ ] **Step 8: Run test to verify it fails, then port `_SAFE_GIT_REF`, `_SAFE_SVN_REV`, and the guard clause in `parse_git_diff_ranges`/`parse_svn_diff_ranges` verbatim.**

- [ ] **Step 9: Run test to verify it passes.**

- [ ] **Step 10: Write the failing test for SVN auto-detection**

```python
def test_parse_diff_ranges_detects_svn_via_dot_svn_directory(tmp_path):
    (tmp_path / ".svn").mkdir()
    # monkeypatch parse_svn_diff_ranges to a sentinel and assert it was called, not parse_git_diff_ranges
```

- [ ] **Step 11: Run test to verify it fails, then port `parse_diff_ranges`'s dispatch logic verbatim.**

- [ ] **Step 12: Run test to verify it passes.**

- [ ] **Step 13: Write the failing test for `map_changes_to_nodes`'s overlap logic against a real fake session**

```python
def test_map_changes_to_nodes_overlap_check(fake_session_with_one_function_lines_10_to_30):
    nodes = map_changes_to_nodes(fake_session_with_one_function_lines_10_to_30, {"src/main/java/Foo.java": [(25, 35)]})
    assert len(nodes) == 1  # 25-35 overlaps 10-30
```

- [ ] **Step 14: Run test to verify it fails, then implement `map_changes_to_nodes`.**

- [ ] **Step 15: Run test to verify it passes, then run the full risk-scoring test file** — `pytest tests/unit/tools/test_risk_scoring.py -v`.

- [ ] **Step 16: Commit**

```bash
git add src/codegraphcontext/tools/risk_scoring.py tests/unit/tools/test_risk_scoring.py
git commit -m "feat: port line-level diff parsing (C-quoted paths, SVN, ref-injection safety)"
```

---

### Task 18: New tool `analyze_change_risk` — change-set orchestration

**Files:**
- Modify: `src/codegraphcontext/tools/risk_scoring.py` (orchestration function)
- Modify: `src/codegraphcontext/tool_definitions.py`
- Modify: `src/codegraphcontext/tools/handlers/analysis_handlers.py`
- Modify: `src/codegraphcontext/server.py`
- Test: `tests/unit/tools/test_analyze_change_risk.py`

**Interfaces:**
- Consumes: Task 13–17's full `risk_scoring.py` surface.
- Produces: `CodeFinder.analyze_change_risk(self, repo_path: str, base: str = "HEAD~1", include_churn: bool = False, require_vcs: bool = False) -> Dict[str, Any]`, exposed as MCP tool `analyze_change_risk`.

Exact ported constants/logic (already captured in full during grounding — port verbatim, renaming only `CRG_`→`CGC_` env prefixes):
```python
_TEST_GAP_EXEMPT_NAMES = frozenset({
    "setUp", "tearDown", "setUpBeforeClass", "tearDownAfterClass",
    "setup_method", "teardown_method", "setUpClass", "tearDownClass",
    "__construct", "__init__", "__destruct",
})
# CGC_MAX_CHANGED_FUNCS, default 500
# review_priorities = sorted(node_risks, key=lambda x: x["risk_score"], reverse=True)[:10]
```

- [ ] **Step 1: Write the failing test for the full orchestration happy path**

```python
def test_analyze_change_risk_returns_summary_and_top_ten_priorities(fake_finder_with_git_repo_and_changed_files):
    finder = fake_finder_with_git_repo_and_changed_files
    res = finder.analyze_change_risk(repo_path="/r", base="HEAD~1")
    assert "summary" in res
    assert "review_priorities" in res and len(res["review_priorities"]) <= 10
    assert "test_gaps" in res and "test_gaps_total" in res
    assert "functions_truncated" in res
    assert "churn_status" in res
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `analyze_change_risk`**

Orchestration sequence (mirrors `analyze_changes` from code-review-graph, already captured in full during grounding — port the control flow verbatim, replacing `GraphStore` calls with the equivalent Cypher via `self.driver.session()`): `parse_diff_ranges(repo_path, base, require_vcs=require_vcs)` → `map_changes_to_nodes(session, ranges)` → filter to `kind in ("Function", "Class")` (CGC's label system, not code-review-graph's `kind` field — adapt by checking `labels(n)` the same way `analyze_impact` already does) → apply the `CGC_MAX_CHANGED_FUNCS` cap (default 500) with `functions_truncated` flag → for each changed node, compute `risk_scoring.py`'s five-factor score (reusing Task 16's per-node scoring logic — extract it into a shared helper during this task if it's currently inlined only in `analyze_impact`, since both call sites need it now; this is the DRY trigger the plan anticipates) → gap-candidate filtering (test-file exclusion via a ported `is_test_file`-equivalent path-pattern check, lifecycle-name exemption via `_TEST_GAP_EXEMPT_NAMES`, non-runtime-construct exclusion — CGC has no TypeScript-interface-equivalent concept surfaced yet, so this filter is a no-op for Java-only scope, documented inline as such) → test-gap classification (`"none"` vs `"indirect"`, using Task 15's `find_test_coverage_count` for direct coverage and a simple one-hop-caller-is-tested check for the indirect case, since full `get_caller_test_routes` multi-hop walking is out of scope per Decision #4's lightweight-proxy framing) → `review_priorities = sorted(...)[:10]` → assemble the final dict with the exact key set already captured during grounding (`summary`, `risk_score` (max across changed nodes), `changed_functions`, `affected_flows` (empty list — flow modeling is explicitly out of scope per Non-goals), `test_gaps`, `test_gaps_total`, `test_gaps_uncovered`, `test_gaps_indirect`, `review_priorities`, `functions_truncated`, `diff_ranges_unavailable`, `churn_status`).

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for the max-functions cap and truncation flag**

```python
def test_analyze_change_risk_caps_functions_and_sets_truncated_flag(fake_finder_with_600_changed_functions, monkeypatch):
    monkeypatch.setenv("CGC_MAX_CHANGED_FUNCS", "500")
    res = fake_finder_with_600_changed_functions.analyze_change_risk(repo_path="/r")
    assert res["functions_truncated"] is True
    assert len(res["changed_functions"]) == 500
```

- [ ] **Step 6: Run test to verify it fails, then confirm Step 3's cap logic is correctly wired to the env var.**

- [ ] **Step 7: Run test to verify it passes.**

- [ ] **Step 8: Write the failing test for lifecycle-method exemption and test-file exclusion**

```python
def test_analyze_change_risk_excludes_lifecycle_methods_and_test_files_from_gaps(fake_finder_with_setup_method_and_a_test_file_change):
    res = fake_finder_with_setup_method_and_a_test_file_change.analyze_change_risk(repo_path="/r")
    gap_names = {g["name"] for g in res["test_gaps"]}
    assert "setUp" not in gap_names
    assert not any("Test.java" in g["file"] for g in res["test_gaps"])
```

- [ ] **Step 9: Run test to verify it fails, then implement the gap-candidate filter from Step 3 fully.**

- [ ] **Step 10: Run test to verify it passes.**

- [ ] **Step 11: Register `analyze_change_risk` as an MCP tool** — same 4-edit convention (`tool_definitions.py` schema with `repo_path`/`base`/`include_churn`/`require_vcs` properties, handler in `analysis_handlers.py`, wrapper + `tool_map` entry in `server.py`).

- [ ] **Step 12: Run the full tools test suite** — `pytest tests/unit/tools/ -v`.

- [ ] **Step 13: Commit**

```bash
git add src/codegraphcontext/tools/risk_scoring.py src/codegraphcontext/tool_definitions.py src/codegraphcontext/tools/handlers/analysis_handlers.py src/codegraphcontext/server.py tests/unit/tools/test_analyze_change_risk.py
git commit -m "feat: analyze_change_risk tool — change-set orchestration (closes Piece 4)"
```

---

# Piece 5 — 3-Tier Hybrid Search Cascade

Independent of Pieces 1–4. Per Decision #2/#3: exact Cypher → SQLite FTS5 lexical → local `fastembed`/BGE-small semantic, each tier only runs if the prior tier returned zero results (not an RRF blend — confirmed as the deliberate speed-prioritizing choice already made in conversation with Javier).

### Task 19: FTS5 sidecar — schema, indexing, BM25 query

**Files:**
- Create: `src/codegraphcontext/tools/search_fts5.py`
- Modify: `src/codegraphcontext/tools/indexing/pipeline.py` (new Phase 7, gated by new flag `ENABLE_FTS5_INDEX`)
- Modify: `src/codegraphcontext/cli/config_manager.py` (register `ENABLE_FTS5_INDEX` in `DEFAULT_CONFIG`/`CONFIG_DESCRIPTIONS`/`CONFIG_VALIDATORS`, mirroring the existing `MULTI_REPO_LINKS` entries exactly)
- Test: `tests/unit/tools/test_search_fts5.py`

**Interfaces:**
- Produces (consumed by Task 21):
  ```python
  _FTS_COLUMN_WEIGHTS: Dict[str, float] = {
      "name": 10.0, "qualified_name": 6.0, "file_path": 1.0,
      "signature": 2.0, "docstring": 1.5, "name_tokens": 4.0,
  }

  def split_identifier(name: str) -> str:
      """snake_case/camelCase/PascalCase/dotted -> space-separated words. Exact port of code-review-graph's _split_identifier."""

  def fts5_db_path(context) -> Path:
      """Sidecar file location, sibling to the active context's db/<backend>/ directory (see config_manager.py conventions)."""

  def ensure_fts5_schema(conn: sqlite3.Connection) -> None: ...
  def rebuild_fts5_index(conn: sqlite3.Connection, session) -> int:
      """Full rebuild from the current graph's Function/Class/Variable nodes. Returns row count indexed."""
  def fts5_search(conn: sqlite3.Connection, query: str, limit: int = 20) -> List[Tuple[str, float]]:
      """Returns (node_uid, bm25_score) pairs, best first."""
  ```

- [ ] **Step 1: Write the failing test for `split_identifier`** (exact port, same examples already captured):

```python
def test_split_identifier_snake_camel_pascal_dotted():
    assert split_identifier("get_route_handler") == "get route handler"
    assert split_identifier("APIRoute") == "API Route"
    assert split_identifier("dispatch_request") == "dispatch request"
    assert split_identifier("full_dispatch_request") == "full dispatch request"
```

- [ ] **Step 2: Run test to verify it fails, then port the exact two-step regex logic captured during grounding.**

- [ ] **Step 3: Run test to verify it passes.**

- [ ] **Step 4: Write the failing test for FTS5 availability as a stdlib capability check**

```python
def test_sqlite_fts5_is_available():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(x)")  # raises sqlite3.OperationalError if FTS5 isn't compiled in
```

This is a capability-probe test, not a unit of new code — if it fails in this environment, stop and report it before building anything on top (per "no asumas nada sin verificar primero"); do not silently fall back to LIKE-only search without telling Javier FTS5 wasn't available here.

- [ ] **Step 5: Run the probe. If it passes, proceed; if it fails, stop and escalate — this changes Piece 5's scope.**

- [ ] **Step 6: Write the failing test for schema creation and indexing**

```python
def test_rebuild_fts5_index_populates_from_fake_session(tmp_path, fake_session_with_functions_and_classes):
    conn = sqlite3.connect(str(tmp_path / "test.sqlite"))
    ensure_fts5_schema(conn)
    count = rebuild_fts5_index(conn, fake_session_with_functions_and_classes)
    assert count > 0
```

- [ ] **Step 7: Run test to verify it fails.**

- [ ] **Step 8: Implement `ensure_fts5_schema`** — `CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(uid UNINDEXED, name, qualified_name, file_path, signature, docstring, name_tokens)`. Implement `rebuild_fts5_index` — query all `Function`/`Class`/`Variable` nodes via Cypher (`MATCH (n) WHERE n:Function OR n:Class OR n:Variable RETURN n.uid, n.name, n.path, ...`), compute `name_tokens = split_identifier(n.name)` per row, bulk-insert into `nodes_fts` (clear-and-reinsert full rebuild — no incremental delta logic this pass, per the YAGNI note in this piece's intro).

- [ ] **Step 9: Run test to verify it passes.**

- [ ] **Step 10: Write the failing test for `fts5_search`'s BM25 weighting**

```python
def test_fts5_search_ranks_name_match_above_docstring_match(populated_fts5_conn):
    results = fts5_search(populated_fts5_conn, "createOrder")
    # a node literally named createOrder should outrank one that only mentions it in a docstring
    assert results[0][0] == "uid-of-createorder-function"
```

- [ ] **Step 11: Run test to verify it fails, then implement `fts5_search`** using the exact weighted `bm25(nodes_fts, 10.0, 6.0, 1.0, 2.0, 1.5, 4.0)` call shape captured during grounding (column order must match the `CREATE VIRTUAL TABLE` column order exactly — verify this alignment explicitly, since a silent column-order mismatch would misweight every query without raising an error).

- [ ] **Step 12: Run test to verify it passes.**

- [ ] **Step 13: Wire Phase 7 into `pipeline.py`**, gated by `(_gcv("ENABLE_FTS5_INDEX") or "false").lower() == "true"`, mirroring Phase 6's try/except/warning_logger convention exactly: on success, call `rebuild_fts5_index`; on any exception, log via `error_logger` with `traceback.format_exc()` and do not abort indexing — same anti-fragility contract as wire-coupling.

- [ ] **Step 14: Add the `ENABLE_FTS5_INDEX` config entry** to `config_manager.py`'s `DEFAULT_CONFIG` (`"false"`), `CONFIG_DESCRIPTIONS` (same WHEN TO ENABLE/PREREQUISITES/COST structure as `MULTI_REPO_LINKS`'s entry), `CONFIG_VALIDATORS` (`["true", "false"]`).

- [ ] **Step 15: Run the full test suite for this area** — `pytest tests/unit/tools/test_search_fts5.py -v`.

- [ ] **Step 16: Commit**

```bash
git add src/codegraphcontext/tools/search_fts5.py src/codegraphcontext/tools/indexing/pipeline.py src/codegraphcontext/cli/config_manager.py tests/unit/tools/test_search_fts5.py
git commit -m "feat: SQLite FTS5 lexical search sidecar (search cascade tier 2)"
```

---

### Task 20: Semantic tier — cosine similarity over existing embeddings

**Files:**
- Create: `src/codegraphcontext/tools/search_semantic.py`
- Modify: `pyproject.toml` (add `fastembed` as a named optional-dependency extra)
- Test: `tests/unit/tools/test_search_semantic.py`

**Interfaces:**
- Consumes: the existing `_get_embedder()`/`probe_embedding_backend()` from `embeddings.py` (unchanged, reused as-is), and the node embedding properties already written by Phase 4 when `ENABLE_VECTOR_RESOLVE=true`.
- Produces (consumed by Task 21):
  ```python
  def semantic_search(session, embedder, query: str, limit: int = 20) -> List[Tuple[str, float]]:
      """Embed the query, fetch candidate node embeddings via Cypher, rank by cosine similarity in Python.
      Returns (node_uid, similarity) pairs, best first."""
  ```

- [ ] **Step 1: Write the failing test for cosine-similarity ranking**

```python
def test_semantic_search_ranks_by_cosine_similarity(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector):
    results = semantic_search(fake_session_with_embedded_nodes, fake_embedder_returning_fixed_vector, "validate order")
    assert results[0][1] >= results[-1][1]  # sorted descending
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement**

Fetch candidate nodes with a non-null embedding property via Cypher (`MATCH (n) WHERE n.embedding IS NOT NULL RETURN n.uid, n.embedding LIMIT $candidate_cap` — confirm the exact property name Phase 4 writes embeddings under by grepping `embeddings.py`'s indexing-write path before assuming `"embedding"` is correct), compute `embedder.embed_batch([query])[0]` for the query vector, then rank candidates by cosine similarity in plain Python (`numpy` if already a dependency, else a manual dot-product/magnitude loop — check `pyproject.toml` for an existing `numpy` dependency before adding a new one). This Python-side ranking (rather than a Cypher vector function) is the deliberate choice from the design doc's constraint not to assume LadybugDB Cypher-dialect parity for anything unproven.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test confirming graceful behavior when no embeddings exist yet** (Review-Focus-adjacent: a repo indexed without `ENABLE_VECTOR_RESOLVE=true` should not crash the semantic tier, just return nothing so the cascade's "no tier found anything" path is reachable):

```python
def test_semantic_search_returns_empty_when_no_nodes_have_embeddings(fake_session_with_no_embedded_nodes, fake_embedder):
    assert semantic_search(fake_session_with_no_embedded_nodes, fake_embedder, "anything") == []
```

- [ ] **Step 6: Run test to verify it fails, then confirm Step 3's implementation already handles an empty candidate set cleanly (should need no new code if written correctly — this step is a regression guard).**

- [ ] **Step 7: Run test to verify it passes.**

- [ ] **Step 8: Add `fastembed` to `pyproject.toml`'s `[project.optional-dependencies]`**, mirroring the existing `datasources`/`kuzu`/`ladybug` extras exactly (named group, e.g. `semantic-search = ["fastembed>=...` — pin to whatever version is already proven working in the venv per `AgentProject.md`'s install notes, do not pick an arbitrary version).

- [ ] **Step 9: Commit**

```bash
git add src/codegraphcontext/tools/search_semantic.py pyproject.toml tests/unit/tools/test_search_semantic.py
git commit -m "feat: semantic search tier (local fastembed/BGE-small cosine similarity)"
```

---

### Task 21: Cascade orchestrator + `search_code` MCP tool

**Files:**
- Create: `src/codegraphcontext/tools/search_cascade.py`
- Modify: `src/codegraphcontext/tool_definitions.py`
- Modify: `src/codegraphcontext/tools/handlers/analysis_handlers.py`
- Modify: `src/codegraphcontext/server.py`
- Test: `tests/unit/tools/test_search_cascade.py`

**Interfaces:**
- Consumes: `CodeFinder`'s existing exact-match helpers (`find_by_function_name`, `find_by_class_name`, `find_by_variable_name` — reused, not reimplemented), Task 19's `fts5_search`, Task 20's `semantic_search`.
- Produces: `def search_code_cascade(code_finder, query: str, limit: int = 20) -> Dict[str, Any]` returning `{"results": [...], "tier_used": "exact"|"fts5"|"semantic"|"none", "query": query}`. New MCP tool `search_code`.

- [ ] **Step 1: Write the failing test for the exact-tier-first, no-fallthrough-when-found behavior**

This is the test that pins the Anti-goal ("no added latency on the fast/exact search path") and the explicit design conversation with Javier (cascade is availability-gated, not threshold-blended):

```python
def test_search_code_cascade_stops_at_exact_tier_when_found(mocker, fake_code_finder_with_exact_match):
    fts5_spy = mocker.patch("codegraphcontext.tools.search_cascade.fts5_search")
    semantic_spy = mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")
    res = search_code_cascade(fake_code_finder_with_exact_match, "createOrder")
    assert res["tier_used"] == "exact"
    fts5_spy.assert_not_called()
    semantic_spy.assert_not_called()
```

(This test file is the one place in this codebase where introducing `pytest-mock`'s `mocker` fixture — already a listed `dev` dependency per the grounding — is justified over the hand-rolled fake-driver convention, specifically because proving a function was **not called** is awkward with pure hand-written stubs; keep every other test in this plan on the existing hand-rolled convention and use `mocker` only for this specific "didn't fall through" assertion class.)

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `search_code_cascade`**

```python
def search_code_cascade(code_finder, query: str, limit: int = 20) -> Dict[str, Any]:
    exact = code_finder.find_related_code(query, fuzzy_search=False, repo_path=None)  # reuse existing exact path
    if exact:
        return {"results": exact[:limit], "tier_used": "exact", "query": query}
    with code_finder.driver.session() as session:
        fts = fts5_search(_fts5_conn(code_finder), query, limit=limit)
        if fts:
            return {"results": _hydrate(session, fts), "tier_used": "fts5", "query": query}
        embedder = _get_embedder()  # reuse existing embeddings.py entry point
        semantic = semantic_search(session, embedder, query, limit=limit)
        if semantic:
            return {"results": _hydrate(session, semantic), "tier_used": "semantic", "query": query}
    return {"results": [], "tier_used": "none", "query": query}
```
`_hydrate` fetches the full node dict (name/path/type/etc.) for a list of `(uid, score)` pairs via one Cypher `MATCH (n) WHERE n.uid IN $uids RETURN n` call, so FTS5/semantic results carry the same shape as `find_related_code`'s existing results. Wrap the `embedder = _get_embedder()` call in a `try/except ImportError` returning `tier_used="none"` with a warning if `fastembed` isn't installed in this environment — matching the existing `probe_embedding_backend` pattern rather than letting an `ImportError` bubble up as a tool-call failure.

- [ ] **Step 4: Run test to verify it passes.**

- [ ] **Step 5: Write the failing test for fallthrough when the exact tier finds nothing**

```python
def test_search_code_cascade_falls_through_to_fts5_when_exact_finds_nothing(mocker, fake_code_finder_with_no_exact_match):
    mocker.patch("codegraphcontext.tools.search_cascade.fts5_search", return_value=[("uid-1", 2.5)])
    semantic_spy = mocker.patch("codegraphcontext.tools.search_cascade.semantic_search")
    res = search_code_cascade(fake_code_finder_with_no_exact_match, "create order flow")
    assert res["tier_used"] == "fts5"
    semantic_spy.assert_not_called()
```

- [ ] **Step 6: Run test to verify it fails, then confirm Step 3 already satisfies it.**

- [ ] **Step 7: Run test to verify it passes.**

- [ ] **Step 8: Register `search_code` as an MCP tool** — schema with `query`/`limit`/`repo_path` properties in `tool_definitions.py`, handler in `analysis_handlers.py` calling `search_code_cascade(self.code_finder, ...)`, wrapper + `tool_map` entry in `server.py`. Leave the existing `find_code` tool completely untouched (per the anti-goal — this is a new, separate tool, not a modification of the existing fast path).

- [ ] **Step 9: Run the full tools test suite** — `pytest tests/unit/tools/ -v`.

- [ ] **Step 10: Commit**

```bash
git add src/codegraphcontext/tools/search_cascade.py src/codegraphcontext/tool_definitions.py src/codegraphcontext/tools/handlers/analysis_handlers.py src/codegraphcontext/server.py tests/unit/tools/test_search_cascade.py
git commit -m "feat: search_code tool -- 3-tier cascade, exact/fts5/semantic (closes Piece 5)"
```

---

# Final — End-to-End Acceptance Test

### Task 22: Real re-index + literal acceptance criterion

**Files:**
- Create: `tests/integration/test_oms_cross_repo_acceptance.py`

**Interfaces:**
- Consumes: everything from Pieces 1–4 (Piece 5 is not part of the literal acceptance criterion per Decision #8, but should be spot-checked manually in the same session since it's cheap to do).

This is explicitly **not** a mocked test — per Decision #8 and this plan's Global Constraints, it must run against the real repos and the real shared LadybugDB.

- [ ] **Step 1: Clear or use a fresh context** so this run isn't polluted by stale data from earlier manual testing during Pieces 1–4 — follow `config_manager.py`'s named-context convention (`cgc context create oms-acceptance-test` or equivalent CLI invocation) to isolate this run's database from the day-to-day default context.

- [ ] **Step 2: Re-index all four repos for real, in sequence** (LadybugDB is single-writer — confirmed concurrency limitation, do not parallelize):
```bash
cd special-order-services && MULTI_REPO_LINKS=true ENABLE_FTS5_INDEX=true cgc index .
cd oms-order-consumer && MULTI_REPO_LINKS=true ENABLE_FTS5_INDEX=true cgc index .
cd oms-order-update-consumer && MULTI_REPO_LINKS=true ENABLE_FTS5_INDEX=true cgc index .
cd terra-projects-oms/az-ordfl-ordrmgmt && cgc index .
```

- [ ] **Step 3: Run `stitch_infrastructure`** (Task 12's new tool) once, via `cgc` or directly through the MCP tool call, against this now-fully-indexed shared graph.

- [ ] **Step 4: Write the acceptance test, asserting against the real graph**

```python
@pytest.mark.integration
def test_create_order_direction_both_crosses_repos_and_reaches_terraform(real_ladybug_driver_from_acceptance_context):
    code_finder = CodeFinder(real_ladybug_driver_from_acceptance_context)
    res = code_finder.analyze_impact(target="createOrder", direction="both", max_wire_hops=2)

    node_paths = [n["path"] for n in res["nodes"]]
    assert any("special-order-services" in p for p in node_paths)
    assert any("oms-order-consumer" in p for p in node_paths), "Piece 2's downstream wire-bridging did not reach the consumer repo"
    assert any("terra-projects-oms" in p for p in node_paths), "Piece 3's Terraform stitching did not surface the backing module"

    assert res["direction"] == "both"
    assert res["unresolved_crossings"] == [] or all(
        "order-inv-inquiry-oms-leg-analysis-event" in c.get("topic", "") for c in res["unresolved_crossings"]
    ), "any unresolved crossing other than the known no-consumer BigQuery topic indicates a real gap, not an expected one"

    assert "risk_score" in res and 0.0 <= res["risk_score"] <= 1.0
```

- [ ] **Step 5: Run the test against the real indexed data** — `pytest tests/integration/test_oms_cross_repo_acceptance.py -v -s` (not mocked, not skipped — this is the literal Decision #8 check).

- [ ] **Step 6: If it fails, diagnose against the real graph directly via Cypher** (`MATCH (t:Topic) RETURN t`, `MATCH (s:Subscription) RETURN s`, `MATCH ()-[:BACKED_BY]->() RETURN *`) before changing any code, to confirm whether the gap is in extraction (Piece 1), traversal (Piece 2), or stitching (Piece 3) — do not guess.

- [ ] **Step 7: Once passing, manually spot-check Piece 5** (not part of the literal acceptance criterion, but cheap to verify in the same session): call `search_code` with a query like `"create order"` and confirm `tier_used` is `"exact"` or `"fts5"` as expected, and call it with something that should only semantic-match (e.g. a natural-language description with no matching identifier) to confirm `tier_used` reaches `"semantic"`.

- [ ] **Step 8: Update `AgentProject.md` in the Obsidian vault** with the final confirmed result (repos re-indexed, tool names, the real `createOrder` trace outcome) — this is documentation, not code, but belongs in this task since it's the natural place to record that the project's stated goal was actually met against real data.

- [ ] **Step 9: Commit**

```bash
git add tests/integration/test_oms_cross_repo_acceptance.py
git commit -m "test: end-to-end acceptance -- createOrder direction=both crosses repos + reaches Terraform"
```

---

## Self-Review

**1. Spec coverage** — every Goal (1–5) has an owning piece: Goal 1 → Piece 2 (Tasks 6–9); Goal 2 → Piece 3 (Tasks 10–12); Goal 3 → Piece 4 (Tasks 13–18); Goal 4 → Piece 5 (Tasks 19–21); Goal 5 → Task 22. Every numbered Decision (1–9) and the Direction section is reflected in a specific task's Interfaces or Step text (Decision #1 → Task 4; #2/#3 → Piece 5 intro + Task 19/20; #4 → Task 13/15/18 design notes; #5/#6 → Task 13/14/17 "full fidelity" framing; #7 → not applicable as a task (operational choice, left to execution); #8 → Task 22; #9 → no task needed, already done upstream). Every "What's left to the builder" bullet from the design doc is resolved either as an explicit plan decision (direction-budget split, two-query merge, topic-first/service_account-fast-follow phasing — all stated in Piece 2/3 intros) or as a grounding step inside the owning task (Task 2's cross-repo join-key verification, Task 11's object-literal tree-sitter-shape check, Task 19's FTS5-location-by-convention).

**2. Step scan** — pass; every code step now carries an exact signature, file, and the specific values the spec pins (weights, caps, env var names, cache sizes); algorithm bodies are given as code only where they're exact ports (churn, diff parsing, security keywords, `_split_identifier`) per the skill's own allowance, and as signature+description where they're genuinely new design (wire-bridging BFS, bean/activator join, HCL object-list walk).

**3. Type consistency** — checked `direction`'s literal values (`"upstream"|"downstream"|"both"`) are identical across Task 6/7/8/9's signatures and tests; `WireWriteStats`'s new field names (`subscriptions_merged`, `pubsub_produces_edges`, `pubsub_consumes_edges`) are used consistently between Task 4's implementation and Task 5's logging; `PubsubSubscriberRecord.fqn` (Task 3) matches the `Function {name, path}` match-key convention `_MERGE_CONSUMES_FROM_SUBSCRIPTION` (Task 4) expects; `risk_scoring.py`'s function names are referenced identically between Task 13–17 (definition) and Task 16/18 (consumption).

**4. Review Focus** — all five items (unrecognized Pub/Sub pattern, silent boundary-stop, LadybugDB dialect risk, unresolved placeholder, non-VCS churn degradation) have an owning task and an explicit test, listed above.

**5. Proportion** — this plan is long because the spec itself describes five substantial, largely-independent subsystems and explicitly waives the usual size ceiling ("ignora los checkpoints... no te preocupes por los tokens"); each piece is independently shippable and testable on its own, which is the right shape for a project this size rather than a sign the plan should have stayed small. Code blocks are reserved for exact ports (the spec's own "full fidelity, no corners cut" items) and schema/Cypher the implementer cannot otherwise derive; all genuinely new algorithmic work (BFS bridging, bean/activator join, object-list HCL walk) is given as signature + description, not a written-out implementation.
