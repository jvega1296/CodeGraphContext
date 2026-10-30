"""Walk a repo's Java sources and join Pub/Sub channel-adapter beans to
``@ServiceActivator`` methods (MULTI_REPO_LINKS, wire piece 1c).

This module is the cross-file join counterpart to :mod:`wire.pubsub_extractor`.
It walks one or more source roots under a repo (default
``DEFAULT_JAVA_SOURCE_DIRS``, shared with :mod:`wire.kafka_scanner`), extracts
publishers, channel-adapter beans, and service activators from every file,
then joins each bean's ``output_channel_qualifier`` to an activator's
``input_channel_raw`` by verbatim string equality — same-file or cross-file,
no normalization of either side (see the Task 2 grounding correction: both
``Constants.X`` references and quoted string literals are captured verbatim
by the extractor, so a plain equality join is sufficient for both real forms).

A bean or activator with no join partner is not silently dropped — a warning
is appended naming the file(s) involved and the unmatched qualifier/channel
text, per the anti-goal shared with the rest of the wire extractors.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from codegraphcontext.wire.config_values import BASE_PROFILE, ConfigValueStore
from codegraphcontext.wire.kafka_scanner import (
    DEFAULT_JAVA_EXCLUDE_PATH_PARTS,
    DEFAULT_JAVA_SOURCE_DIRS,
    _is_excluded_java_path,
)
from codegraphcontext.wire.pubsub_extractor import (
    PubsubChannelAdapterBean,
    PubsubPublisherRecord,
    PubsubServiceActivator,
    extract_channel_adapter_beans_from_source,
    extract_publishers_from_source,
    extract_service_activators_from_source,
    resolve_pending_publishers_via_callers,
)


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
    repo_root: Path,
    *,
    store: Optional[ConfigValueStore] = None,
    active_profile: str = BASE_PROFILE,
    include_dirs: Sequence[str] = DEFAULT_JAVA_SOURCE_DIRS,
    exclude_path_parts: Sequence[str] = DEFAULT_JAVA_EXCLUDE_PATH_PARTS,
    max_file_kb: int = 512,
    caller_resolution_max_file_kb: int = 5120,
) -> PubsubScanResult:
    """Walk ``repo_root`` and extract GCP Pub/Sub publisher/subscriber records.

    Mirrors :func:`codegraphcontext.wire.kafka_scanner.scan_repo_kafka`'s
    directory-walk/exclude/dedupe/size-cap loop; the only addition is
    collecting channel-adapter beans and service activators repo-wide so
    they can be joined after the walk, regardless of which file each side
    came from.

    ``caller_resolution_max_file_kb`` (default 5 MiB, much larger than
    ``max_file_kb``'s 512 KiB) bounds a SEPARATE read used only to populate
    the caller-argument-tracing cache (``resolve_pending_publishers_via_callers``),
    not full extraction. A file over ``max_file_kb`` is still skipped for the
    (comparatively expensive) per-file extraction passes, but real services
    legitimately have one or two single-class "God objects" well past 512 KB
    (confirmed real: a 587 KB OrderServiceImpl.java is exactly where the
    caller of an abstract Pub/Sub wrapper method's real @Value-injected topic
    argument lives) -- excluding such a file from the caller-resolution text
    cache entirely silently defeats cross-method resolution for every
    wrapper method it happens to call into, with no diagnostic signal beyond
    the generic "no caller anywhere" warning. Reading a few extra megabytes
    of text once per repo scan is cheap; re-running full extraction on it is
    not, which is why the two caps stay independent.
    """
    root = Path(repo_root).resolve()
    result = PubsubScanResult()

    seen: set = set()
    max_bytes = max_file_kb * 1024
    caller_resolution_max_bytes = caller_resolution_max_file_kb * 1024

    beans: List[PubsubChannelAdapterBean] = []
    activators: List[PubsubServiceActivator] = []
    # Retained so pending publisher records (a wrapper method's topic
    # argument is a bare parameter, real value supplied by a caller
    # elsewhere in the repo -- see extract_publishers_from_source) can be
    # resolved against every other scanned file's text after the walk.
    file_texts: Dict[str, str] = {}

    for rel in include_dirs:
        base = (root / rel).resolve()
        if not base.is_dir():
            continue
        for path in base.rglob("*.java"):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            if _is_excluded_java_path(resolved, exclude_path_parts):
                result.files_skipped += 1
                continue
            try:
                size = path.stat().st_size
            except OSError:
                result.files_skipped += 1
                continue
            if size > max_bytes:
                result.files_skipped += 1
                result.warnings.append(
                    f"{path}: skipped ({size / 1024:.1f} KB > {max_file_kb} KB cap)"
                )
                # Still worth reading for caller-argument resolution (a
                # cheap text scan, not full extraction) unless it is
                # implausibly huge even for that.
                if size <= caller_resolution_max_bytes:
                    try:
                        file_texts[str(path)] = path.read_text(encoding="utf-8", errors="replace")
                    except OSError:
                        pass
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError as e:
                result.files_skipped += 1
                result.warnings.append(f"{path}: read error: {e}")
                continue

            result.files_scanned += 1
            file_texts[str(path)] = text

            pub_result = extract_publishers_from_source(
                text, path, store=store, active_profile=active_profile,
            )
            result.publishers.extend(pub_result.publishers)
            result.warnings.extend(pub_result.warnings)

            file_beans, bean_warnings = extract_channel_adapter_beans_from_source(
                text, path, store=store, active_profile=active_profile,
            )
            beans.extend(file_beans)
            result.warnings.extend(bean_warnings)

            file_activators, activator_warnings = extract_service_activators_from_source(
                text, path,
            )
            activators.extend(file_activators)
            result.warnings.extend(activator_warnings)

    activators_by_channel: Dict[str, List[PubsubServiceActivator]] = defaultdict(list)
    for activator in activators:
        activators_by_channel[activator.input_channel_raw].append(activator)

    matched_activator_ids: set = set()

    for bean in beans:
        qualifier = bean.output_channel_qualifier
        matches = activators_by_channel.get(qualifier) if qualifier is not None else None
        if not matches:
            result.warnings.append(
                f"{bean.source_file}: {bean.fqn} declares output channel qualifier "
                f"{qualifier!r} but no @ServiceActivator(inputChannel = ...) anywhere "
                "in the repo matches it; this bean is not joined to a consumer"
            )
            continue
        for activator in matches:
            matched_activator_ids.add(id(activator))
            result.subscribers.append(PubsubSubscriberRecord(
                fqn=activator.fqn,
                subscription_name=bean.subscription_resolved,
                confidence=bean.confidence,
                provenance=bean.provenance,
                source_file=activator.source_file,
                line=activator.line,
            ))

    # @ServiceActivator is generic Spring Integration chaining machinery used far
    # beyond Pub/Sub consumption (transformer/router/service-activator pipelines,
    # etc.) — extract_service_activators_from_source captures every one in the
    # repo unconditionally, with no way to know in advance which are Pub/Sub
    # related. In a repo with zero Pub/Sub channel-adapter beans, there is
    # nothing to join against and essentially every activator there is unrelated
    # internal chaining, so the unmatched-activator warning is scoped to repos
    # that demonstrably use at least one Pub/Sub bean somewhere. The
    # unmatched-BEAN warning above has no equivalent noise problem — a @Bean
    # constructing PubSubInboundChannelAdapter is unambiguous Pub/Sub machinery
    # by construction — and stays unconditional.
    if beans:
        for activator in activators:
            if id(activator) in matched_activator_ids:
                continue
            result.warnings.append(
                f"{activator.source_file}: {activator.fqn} declares "
                f"@ServiceActivator(inputChannel = {activator.input_channel_raw!r}) but "
                "no channel-adapter bean anywhere in the repo matches it; this "
                "activator's consumer-side wiring is not joined to a Pub/Sub subscription"
            )

    # Resolve any publisher whose topic argument is a bare parameter of an
    # abstract/wrapper method (see extract_publishers_from_source) by
    # searching every real call site of that method across the whole repo
    # just scanned -- this is the repo-wide join this module already exists
    # to perform for the consumer side (bean <-> activator), applied to the
    # producer side's own cross-method case.
    resolved_publishers, pending_warnings = resolve_pending_publishers_via_callers(
        result.publishers, file_texts, store=store, active_profile=active_profile,
    )
    result.publishers = resolved_publishers
    result.warnings.extend(pending_warnings)

    return result


__all__ = [
    "PubsubScanResult",
    "PubsubSubscriberRecord",
    "scan_repo_pubsub",
]
