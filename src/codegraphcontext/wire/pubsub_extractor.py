"""GCP Pub/Sub publisher extraction from Java source (MULTI_REPO_LINKS).

This module walks Java source text and reports every place where a GCP
Pub/Sub topic is published to, together with the FQN of the enclosing
method. A downstream writer (added in a later PR, gated on
``MULTI_REPO_LINKS=true``) will turn these records into ``PRODUCES_TO``
edges between ``Function`` and ``Topic`` nodes.

This is the producer-side half of the Pub/Sub extractor; a later PR
extends this same module with the consumer-side (bean /
``@ServiceActivator``-style) extraction.

Like :mod:`codegraphcontext.wire.kafka_extractor`, the extractor is
deliberately regex- + brace-tracking-based rather than tree-sitter-driven,
and is kept self-contained (duplicating the small set of structural
helpers it needs) so it stays trivial to unit-test with inline fixtures.

The only producer call shape currently recognised is Spring Cloud GCP's
``PubSubTemplate.publish(topic, payload)``; the raw ``com.google.cloud.pubsub``
client's ``Publisher.publish(Message)`` shape is a known gap (see the
anti-goal test) and surfaces as a warning rather than silence.

Confidence tiers (mirrors :mod:`schema_contract.WIRE_CONFIDENCE_TIERS`):

* ``EXTRACTED`` — raw string literal, no placeholder
* ``INFERRED``  — ``${...}`` placeholder that ``ConfigValueStore`` resolved
* ``SYMBOLIC``  — ``${...}`` placeholder, or an identifier, that could not
  be resolved
* ``AMBIGUOUS`` — an expression we do not attempt to trace (method call,
  ternary, cast, string concat, ...)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from codegraphcontext.wire.config_values import (
    BASE_PROFILE,
    ConfigValueStore,
    PlaceholderResolution,
)

# ── Public dataclasses ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class PubsubPublisherRecord:
    fqn: str                # e.g. "com.acme.oms.MessagePublisher.publish"
    topic_raw: str          # exact string arg from source (may contain ${...})
    topic_resolved: str     # after ConfigValueStore resolution (== raw if literal)
    confidence: str         # EXTRACTED | INFERRED | SYMBOLIC | AMBIGUOUS
    provenance: str         # short human tag: literal / config:<key> / unresolved:<key> / identifier:<name>
    source_file: str
    line: int               # 1-based
    call_shape: str         # "PubSubTemplate.publish"


@dataclass
class PubsubExtractionResult:
    publishers: List[PubsubPublisherRecord] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.publishers


# ── Trigger detection ───────────────────────────────────────────────────────

# Both the Spring Cloud GCP wrapper (the only shape this module currently
# extracts) and the raw GCP client library package are recognised, mirroring
# kafka_extractor's dual Spring/raw-client marker set. This lets a file that
# only uses the raw client still reach the "recognized nothing" warning path
# in extract_publishers_from_source instead of being silently skipped.
_PUBSUB_IMPORT_MARKERS = (
    "com.google.cloud.spring.pubsub",
    "com.google.cloud.pubsub",
)


def looks_like_pubsub_source(source: str) -> bool:
    """Cheap pre-filter — skip files that clearly have no Pub/Sub involvement.

    Mirrors :func:`codegraphcontext.wire.kafka_extractor.looks_like_kafka_source`:
    a cheap substring pre-filter, not a full parse.
    """
    for marker in _PUBSUB_IMPORT_MARKERS:
        if marker in source:
            return True
    return False


# ── Regexes for structural extraction ───────────────────────────────────────

_PACKAGE_RE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.MULTILINE)

# Producer call: `<recv>.publish(<arg1>, ...)` — receiver is any identifier.
# The receiver is filtered down to PubSubTemplate-typed variables below.
_PUBLISH_CALL_RE = re.compile(
    r"(?<![\w.])([A-Za-z_][\w]*)\s*\.\s*publish\s*\(",
)

# `PubSubTemplate <name>;` / `PubSubTemplate <name> = ...;` field or local
# declaration — used to confirm a call receiver's declared type.
_PUBSUB_TEMPLATE_FIELD_RE = re.compile(
    r"(?:^|[\s;{])(?:public|private|protected|static|final|\s)*\s*PubSubTemplate\s+([A-Za-z_]\w*)\s*[;=]"
)

# Simple `String NAME = "...";` scan for identifier tracing.
_STRING_CONST_RE = re.compile(
    r"(?:^|[\s;{])(?:public|private|protected|static|final|\s)*\s*String\s+([A-Za-z_]\w*)\s*=\s*\"([^\"]*)\"\s*;",
)


# ── Structural helpers (duplicated from kafka_extractor.py's approach to
#    keep this module self-contained, like every other wire extractor) ─────

@dataclass
class _ClassSpan:
    name: str
    start_offset: int   # position of the `{` after the class header
    end_offset: int     # position of the matching `}`
    depth: int          # nesting depth (outermost = 0)


def _find_matching_brace(text: str, open_pos: int) -> int:
    """Return the offset of the `}` that closes the `{` at ``open_pos``.

    Handles nested braces, // line comments, /* block comments */, ``"strings"``,
    ``'chars'``, and escape sequences inside strings. Returns ``-1`` if no
    matching close is found (malformed input).
    """
    depth = 0
    i = open_pos
    n = len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            nl = text.find("\n", i)
            i = n if nl == -1 else nl
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            end = text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        if c == '"':
            i += 1
            while i < n:
                if text[i] == "\\":
                    i += 2
                    continue
                if text[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "'":
            i += 1
            while i < n:
                if text[i] == "\\":
                    i += 2
                    continue
                if text[i] == "'":
                    i += 1
                    break
                i += 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _extract_class_spans(source: str) -> List[_ClassSpan]:
    """Return spans of every ``class`` / ``interface`` / ``enum`` / ``@interface`` body.

    Nested types are included. Anonymous inner classes are ignored (they have
    no source-level name we could put into an FQN).
    """
    spans: List[_ClassSpan] = []
    header_re = re.compile(
        r"\b(?:class|interface|enum|@interface)\s+([A-Za-z_]\w*)"
        r"(?:\s*<[^>]*>)?"
        r"(?:\s+extends\s+[\w.<>,\s]+?)?"
        r"(?:\s+implements\s+[\w.<>,\s]+?)?"
        r"(?:\s+permits\s+[\w.<>,\s]+?)?"
        r"\s*\{",
        re.MULTILINE,
    )
    for m in header_re.finditer(source):
        name = m.group(1)
        open_brace = source.rfind("{", m.start(), m.end())
        if open_brace == -1:
            continue
        close_brace = _find_matching_brace(source, open_brace)
        if close_brace == -1:
            continue
        spans.append(_ClassSpan(name=name, start_offset=open_brace,
                                end_offset=close_brace, depth=0))
    for s in spans:
        s.depth = sum(
            1 for other in spans
            if other is not s
            and other.start_offset < s.start_offset < other.end_offset
        )
    return spans


def _enclosing_class(spans: Sequence[_ClassSpan], offset: int) -> Optional[_ClassSpan]:
    """Return the innermost class span that contains ``offset``."""
    candidates = [s for s in spans if s.start_offset < offset < s.end_offset]
    if not candidates:
        return None
    return max(candidates, key=lambda s: s.depth)


def _offset_to_line(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def _extract_package(source: str) -> Optional[str]:
    m = _PACKAGE_RE.search(source)
    return m.group(1) if m else None


def _find_enclosing_method(
    source: str, class_span: _ClassSpan, offset: int
) -> Optional[str]:
    """Given a call-site offset inside a class, return the enclosing method name."""
    method_re = re.compile(
        r"""
        (?:@[\w.]+\s*(?:\([^)]*\))?\s*)*
        (?:(?:public|private|protected|static|final|synchronized|abstract|default|native)\s+)*
        (?:<[^>]+>\s*)?
        [\w.<>?,\s\[\]]+?\s+
        ([A-Za-z_]\w*)
        \s*\([^)]*\)
        (?:\s*throws\s+[\w.,\s]+?)?
        \s*\{
        """,
        re.VERBOSE,
    )
    best: Optional[str] = None
    for m in method_re.finditer(source, class_span.start_offset, class_span.end_offset):
        brace = source.rfind("{", m.start(), m.end())
        if brace == -1 or brace >= offset:
            continue
        close = _find_matching_brace(source, brace)
        if close == -1 or close < offset:
            continue
        best = m.group(1)
    return best


def _pubsub_template_fields_in_class(source: str, class_span: _ClassSpan) -> set:
    """Return the set of identifier names declared as ``PubSubTemplate`` in this class."""
    body = source[class_span.start_offset:class_span.end_offset]
    return {m.group(1) for m in _PUBSUB_TEMPLATE_FIELD_RE.finditer(body)}


def _string_constants_in_class(source: str, class_span: _ClassSpan) -> dict:
    """Return a ``{IDENTIFIER: literal}`` map of trivial ``String X = "..."``
    field declarations inside the class."""
    body = source[class_span.start_offset:class_span.end_offset]
    result: dict = {}
    for m in _STRING_CONST_RE.finditer(body):
        result[m.group(1)] = m.group(2)
    return result


def _is_pubsub_template_receiver(ident: str, fields: set) -> bool:
    """True when ``ident`` is confirmed (declared type) or conventionally
    named as a ``PubSubTemplate`` instance.

    The declared-type set catches ``private final PubSubTemplate x;``-style
    fields; the name fallback covers call sites where the declaration isn't
    visible in the snippet (e.g. an autowired field elsewhere) but the
    variable still carries the conventional ``pubSubTemplate`` name. This is
    what keeps a raw GCP client's ``publisher.publish(message)`` call from
    being mistaken for a Spring Cloud GCP producer.
    """
    if ident in fields:
        return True
    return "pubsub" in ident.lower()


# ── Value parsing (literal / placeholder / identifier) ─────────────────────

def _read_first_call_arg(source: str, open_paren_offset: int) -> Optional[str]:
    """Return the text of the first comma-separated argument.

    ``open_paren_offset`` is the offset of ``(`` after the callable. We
    respect nested parens, generic angle brackets, and string literals.
    """
    i = open_paren_offset + 1
    n = len(source)
    depth_paren = 0
    depth_angle = 0
    start = i
    while i < n:
        c = source[i]
        if c == '"':
            i += 1
            while i < n:
                if source[i] == "\\":
                    i += 2
                    continue
                if source[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c == "(":
            depth_paren += 1
        elif c == ")":
            if depth_paren == 0:
                return source[start:i].strip() or None
            depth_paren -= 1
        elif c == "<":
            depth_angle += 1
        elif c == ">":
            if depth_angle > 0:
                depth_angle -= 1
        elif c == "," and depth_paren == 0 and depth_angle == 0:
            return source[start:i].strip() or None
        i += 1
    return None


_STRING_LITERAL_RE = re.compile(r'^"([^"\\]*(?:\\.[^"\\]*)*)"$')


def _classify_arg(
    arg_text: str,
    store: Optional[ConfigValueStore],
    active_profile: str,
    string_consts: dict,
) -> Tuple[str, str, str, str]:
    """Turn a first-argument expression into ``(topic_raw, topic_resolved, confidence, provenance)``."""
    text = arg_text.strip()

    m = _STRING_LITERAL_RE.match(text)
    if m:
        raw = m.group(1)
        return _classify_literal(raw, store, active_profile)

    if re.fullmatch(r"[A-Za-z_]\w*", text):
        const = string_consts.get(text)
        if const is not None:
            resolved_raw, resolved_val, tier, prov = _classify_literal(
                const, store, active_profile
            )
            return resolved_raw, resolved_val, tier, f"identifier:{text}→{prov}"
        # No in-class literal traces this identifier to a value — the
        # extractor cannot invent one, so it is reported as an unresolved
        # symbolic reference rather than dropped.
        return text, text, "SYMBOLIC", f"unresolved:identifier:{text}"

    # Anything else (`svc.getTopic()`, ternary, cast, string concat) — we do
    # not want to invent a value.
    return text, text, "AMBIGUOUS", "expression"


def _classify_literal(
    raw: str,
    store: Optional[ConfigValueStore],
    active_profile: str,
) -> Tuple[str, str, str, str]:
    """Classify a string-literal value (already unquoted)."""
    if "${" not in raw:
        return raw, raw, "EXTRACTED", "literal"
    if store is None:
        return raw, raw, "SYMBOLIC", "unresolved:no-store"
    resolution: PlaceholderResolution = store.resolve_placeholders(raw, active_profile)
    if resolution.fully_resolved():
        if resolution.used:
            first_key, source_label = resolution.used[0]
            prov = f"config:{first_key}({source_label})"
        else:
            prov = "config:defaults"
        return raw, resolution.output, "INFERRED", prov
    first_unresolved = resolution.unresolved[0] if resolution.unresolved else "?"
    return raw, resolution.output, "SYMBOLIC", f"unresolved:{first_unresolved}"


# ── Public entry points ────────────────────────────────────────────────────

def extract_publishers_from_source(
    source: str,
    path: Path,
    *,
    store: Optional[ConfigValueStore] = None,
    active_profile: str = BASE_PROFILE,
) -> PubsubExtractionResult:
    """Extract GCP Pub/Sub publisher records from a single Java file's text.

    ``path`` is used only for provenance (``source_file`` field); the extractor
    never reads it.

    Unlike :func:`codegraphcontext.wire.kafka_extractor.extract_from_source`,
    this does not early-return on ``looks_like_pubsub_source(source)`` being
    false: the per-call-site receiver check (declared ``PubSubTemplate`` type,
    or the conventional ``pubSubTemplate`` name) is what keeps unrelated
    ``.publish(...)`` calls out, so the import-based check is only needed at
    the end, to tell a file that never mentioned Pub/Sub at all apart from
    one that did but used an unrecognized call shape.
    """
    result = PubsubExtractionResult()
    path_str = str(path)

    pkg = _extract_package(source)
    spans = _extract_class_spans(source)
    if not spans:
        result.warnings.append(f"{path}: no top-level class/interface found; skipping")
        return result

    class_consts_cache: dict = {}
    class_pubsub_fields_cache: dict = {}

    def _fqn(span: _ClassSpan, method: str) -> str:
        prefix = f"{pkg}." if pkg else ""
        return f"{prefix}{span.name}.{method}"

    for m in _PUBLISH_CALL_RE.finditer(source):
        span = _enclosing_class(spans, m.start())
        if span is None:
            continue
        method_name = _find_enclosing_method(source, span, m.start())
        if method_name is None:
            continue
        fields = class_pubsub_fields_cache.setdefault(
            id(span), _pubsub_template_fields_in_class(source, span)
        )
        if not _is_pubsub_template_receiver(m.group(1), fields):
            continue
        arg = _read_first_call_arg(source, m.end() - 1)  # `(` is at end-1
        if arg is None:
            continue
        consts = class_consts_cache.setdefault(
            id(span), _string_constants_in_class(source, span)
        )
        raw, resolved, tier, prov = _classify_arg(arg, store, active_profile, consts)
        result.publishers.append(PubsubPublisherRecord(
            fqn=_fqn(span, method_name),
            topic_raw=raw,
            topic_resolved=resolved,
            confidence=tier,
            provenance=prov,
            source_file=path_str,
            line=_offset_to_line(source, m.start()),
            call_shape="PubSubTemplate.publish",
        ))

    if not result.publishers and looks_like_pubsub_source(source):
        result.warnings.append(
            f"{path}: looked like Pub/Sub source (imports a GCP Pub/Sub package) "
            "but used an unrecognized publisher call shape; only the Spring Cloud "
            "GCP PubSubTemplate.publish(...) shape is currently recognized, so a "
            "raw com.google.cloud.pubsub Publisher.publish(...) call is not traced"
        )

    return result


__all__ = [
    "PubsubExtractionResult",
    "PubsubPublisherRecord",
    "extract_publishers_from_source",
    "looks_like_pubsub_source",
]
