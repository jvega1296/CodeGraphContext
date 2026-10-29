"""GCP Pub/Sub publisher extraction from Java source (MULTI_REPO_LINKS).

This module walks Java source text and reports every place where a GCP
Pub/Sub topic is published to, together with the FQN of the enclosing
method. A downstream writer (added in a later PR, gated on
``MULTI_REPO_LINKS=true``) will turn these records into ``PRODUCES_TO``
edges between ``Function`` and ``Topic`` nodes.

This module also contains the consumer-side half: extraction of Spring
Cloud GCP ``@Bean``-declared ``PubSubInboundChannelAdapter`` beans and the
``@ServiceActivator`` methods that consume their output channel. The two
sides are extracted independently and per-file; joining a bean's
``output_channel_qualifier`` to an activator's ``input_channel_raw``
(same-file or cross-file) is the cross-file scanner's job, not this
module's.

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
    provenance: str         # short human tag: literal / config:<key> / unresolved:<key> / identifier:<name> / unresolved:identifier:<name>
    source_file: str
    line: int               # 1-based
    call_shape: str         # "PubSubTemplate.publish"


@dataclass
class PubsubExtractionResult:
    publishers: List[PubsubPublisherRecord] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.publishers


@dataclass(frozen=True)
class PubsubChannelAdapterBean:
    fqn: str                      # bean method FQN
    subscription_raw: str         # second ctor arg's identifier, verbatim (or unquoted literal)
    subscription_resolved: str    # after ConfigValueStore resolution (== raw if literal / unresolved)
    confidence: str                # EXTRACTED | INFERRED | SYMBOLIC | AMBIGUOUS
    provenance: str
    output_channel_ref: str       # variable name passed to setOutputChannel(...)
    output_channel_qualifier: Optional[str]  # raw @Qualifier(...) text on that parameter, verbatim (quotes kept), if present
    source_file: str
    line: int                     # 1-based


@dataclass(frozen=True)
class PubsubServiceActivator:
    fqn: str                      # method FQN
    input_channel_raw: str        # raw inputChannel = ... text, verbatim (quotes kept if a string literal)
    input_channel_resolved: Optional[str]  # unquoted literal value, or None for a symbolic/constant reference
    source_file: str
    line: int


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
    variable still carries the conventional ``pubSubTemplate`` name.

    The fallback deliberately checks for ``pubsubtemplate`` as one word, not
    just ``pubsub`` — a raw ``com.google.cloud.pubsub.v1.Publisher`` field
    can plausibly be named ``pubsubClient`` or ``pubsubPublisher`` in real
    code, and a bare ``"pubsub" in name`` check would wrongly treat that as
    a recognized Spring Cloud GCP producer instead of flowing it to the
    "recognized nothing" warning path like the plain ``publisher`` anti-goal
    case.
    """
    if ident in fields:
        return True
    return "pubsubtemplate" in ident.lower()


# ── Consumer-side structural helpers ────────────────────────────────────────

# `@Value("${...}")`-annotated `String` field declaration — used to resolve a
# channel-adapter bean's subscription-name identifier back to its placeholder,
# the one case Task 1's `_string_constants_in_class` (plain `String X = "lit";`)
# does not cover.
_VALUE_FIELD_RE = re.compile(
    r'@Value\s*\(\s*"([^"]*)"\s*\)\s*'
    r'(?:(?:public|private|protected|static|final)\s+)*'
    r'String\s+([A-Za-z_]\w*)\s*;'
)

# Method declaration up to and including the opening `(` of its parameter
# list. Group 1 captures every annotation preceding the method (so callers
# can grep it for `@Bean` / `@ServiceActivator`); group 2 is the method name.
# The parameter list itself is located afterwards via `_find_matching_paren`
# so nested parens in parameter annotations (e.g. `@Qualifier(Constants.X)`)
# are handled correctly instead of truncating on the first `)`.
_METHOD_START_RE = re.compile(
    r"""
    ((?:@[\w.]+\s*(?:\([^)]*\))?\s*)*)
    (?:(?:public|private|protected|static|final|synchronized|abstract|default|native)\s+)*
    (?:<[^>]+>\s*)?
    [\w.<>?,\s\[\]]+?\s+
    ([A-Za-z_]\w*)
    \s*\(
    """,
    re.VERBOSE,
)

# Whatever follows a method's `)` before its body: an optional `throws`
# clause, then either `{` (has a body) or `;` (abstract/interface method,
# no body to search for a constructor call in).
_AFTER_PARAMS_RE = re.compile(
    r"\s*(?:throws\s+[\w.,\s<>\[\]]+?)?\s*([{;])"
)

# `<var> = new PubSubInboundChannelAdapter(` — the local variable that a
# `@Bean` method assigns the adapter to; `.setOutputChannel(...)` is then
# looked up on that same variable name within the rest of the method body.
_ADAPTER_CTOR_RE = re.compile(
    r"([A-Za-z_]\w*)\s*=\s*new\s+PubSubInboundChannelAdapter\s*\("
)

# `@ServiceActivator(inputChannel = <value>)` — `<value>` is captured
# verbatim (quotes kept if it is a string literal, dotted form kept as-is
# if it is a `Constants.X`-style reference); see the Task 2 grounding
# correction for why both forms are real and neither is normalized here.
_SERVICE_ACTIVATOR_RE = re.compile(
    r"@ServiceActivator\s*\(\s*inputChannel\s*=\s*(.*?)\s*\)",
    re.DOTALL,
)


def _find_matching_paren(text: str, open_pos: int) -> int:
    """Return the offset of the ``)`` that closes the ``(`` at ``open_pos``.

    Mirrors :func:`_find_matching_brace` but for parens; handles nested
    parens and ``"strings"`` inside the call/parameter-list text.
    """
    depth = 0
    i = open_pos
    n = len(text)
    while i < n:
        c = text[i]
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
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _split_top_level_args(text: str) -> List[str]:
    """Split ``text`` on top-level commas, respecting ``()``, ``<>``, and
    ``"strings"``. Used for both call-argument lists and parameter lists."""
    parts: List[str] = []
    depth_paren = 0
    depth_angle = 0
    start = 0
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
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
        if c == "(":
            depth_paren += 1
        elif c == ")":
            if depth_paren > 0:
                depth_paren -= 1
        elif c == "<":
            depth_angle += 1
        elif c == ">":
            if depth_angle > 0:
                depth_angle -= 1
        elif c == "," and depth_paren == 0 and depth_angle == 0:
            parts.append(text[start:i].strip())
            start = i + 1
        i += 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


@dataclass
class _MethodDecl:
    name: str
    annotations_text: str
    header_start: int             # offset where the declaration (first annotation/modifier) starts
    params_text: str
    body_start: Optional[int]     # offset of the method's `{`, or None (abstract/interface method)
    body_end: Optional[int]       # offset of the matching `}`, or None


def _iter_method_declarations(source: str) -> List[_MethodDecl]:
    """Find every method declaration in ``source`` (whole-file scan).

    Mirrors the producer extractor's whole-file-scan-then-locate-enclosing-class
    pattern (see ``_PUBLISH_CALL_RE.finditer(source)`` + ``_enclosing_class``)
    rather than scanning per-class-span, so a method is reported exactly once
    even if its class is nested inside another class span.
    """
    decls: List[_MethodDecl] = []
    for m in _METHOD_START_RE.finditer(source):
        annotations_text = m.group(1)
        name = m.group(2)
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(source, open_paren)
        if close_paren == -1:
            continue
        params_text = source[open_paren + 1:close_paren]
        after = _AFTER_PARAMS_RE.match(source, close_paren + 1)
        body_start: Optional[int] = None
        body_end: Optional[int] = None
        if after is not None and after.group(1) == "{":
            brace = after.start(1)
            close_brace = _find_matching_brace(source, brace)
            if close_brace != -1:
                body_start, body_end = brace, close_brace
        decls.append(_MethodDecl(
            name=name,
            annotations_text=annotations_text,
            header_start=m.start(),
            params_text=params_text,
            body_start=body_start,
            body_end=body_end,
        ))
    return decls


def _value_fields_in_class(source: str, class_span: _ClassSpan) -> dict:
    """Return a ``{IDENTIFIER: placeholder}`` map of ``@Value("${...}")``
    annotated ``String`` field declarations inside the class."""
    body = source[class_span.start_offset:class_span.end_offset]
    result: dict = {}
    for m in _VALUE_FIELD_RE.finditer(body):
        result[m.group(2)] = m.group(1)
    return result


def _find_adapter_construction(
    source: str, body_start: int, body_end: int
) -> Optional[Tuple[str, List[str]]]:
    """Find ``<var> = new PubSubInboundChannelAdapter(<args>)`` in a method body.

    Returns ``(var_name, args)`` or ``None`` if the shape is not present or
    has fewer than two constructor arguments.
    """
    m = _ADAPTER_CTOR_RE.search(source, body_start, body_end)
    if m is None:
        return None
    var_name = m.group(1)
    open_paren = m.end() - 1
    close_paren = _find_matching_paren(source, open_paren)
    if close_paren == -1 or close_paren > body_end:
        return None
    args = _split_top_level_args(source[open_paren + 1:close_paren])
    if len(args) < 2:
        return None
    return var_name, args


def _find_set_output_channel(
    source: str, body_start: int, body_end: int, var_name: str
) -> Optional[str]:
    """Find ``<var_name>.setOutputChannel(<arg>)`` in a method body and
    return ``<arg>``'s raw text, or ``None`` if not present."""
    call_re = re.compile(r"\b" + re.escape(var_name) + r"\s*\.\s*setOutputChannel\s*\(")
    m = call_re.search(source, body_start, body_end)
    if m is None:
        return None
    return _read_first_call_arg(source, m.end() - 1)


def _qualifier_for_param(params_text: str, param_name: str) -> Optional[str]:
    """Return the raw ``@Qualifier(...)`` text (verbatim, quotes kept) for
    the parameter named ``param_name``, or ``None`` if that parameter has
    no ``@Qualifier`` annotation (or is not found in ``params_text`` at
    all — e.g. the ``setOutputChannel`` argument was not one of the bean
    method's own parameters)."""
    for part in _split_top_level_args(params_text):
        m_name = re.search(r"([A-Za-z_]\w*)\s*$", part)
        if not m_name or m_name.group(1) != param_name:
            continue
        m_q = re.search(r"@Qualifier\s*\(\s*(.*?)\s*\)", part, re.DOTALL)
        return m_q.group(1) if m_q else None
    return None


def _resolve_input_channel_literal(raw: str) -> Optional[str]:
    """Unquote ``raw`` if it is a string literal; ``None`` for a symbolic
    reference (e.g. ``Constants.X``), which this module cannot resolve
    without seeing the ``Constants`` class — that join is Task 3's job."""
    m = _STRING_LITERAL_RE.match(raw)
    return m.group(1) if m else None


def _classify_subscription(
    arg_text: str,
    store: Optional[ConfigValueStore],
    active_profile: str,
    value_fields: dict,
) -> Tuple[str, str, str, str]:
    """Classify a channel-adapter bean's second constructor argument.

    Returns ``(subscription_raw, subscription_resolved, confidence, provenance)``.
    ``subscription_raw`` is always the argument's own text (the identifier,
    or the unquoted literal) — unlike the producer's ``topic_raw``, it is
    never replaced by an underlying constant's value, per the Task 2 brief.
    """
    text = arg_text.strip()

    m = _STRING_LITERAL_RE.match(text)
    if m:
        literal = m.group(1)
        _, resolved, tier, prov = _classify_literal(literal, store, active_profile)
        return literal, resolved, tier, prov

    if re.fullmatch(r"[A-Za-z_]\w*", text):
        placeholder = value_fields.get(text)
        if placeholder is not None:
            _, resolved, tier, prov = _classify_literal(placeholder, store, active_profile)
            return text, resolved, tier, f"identifier:{text}→{prov}"
        return text, text, "SYMBOLIC", f"unresolved:identifier:{text}"

    return text, text, "AMBIGUOUS", "expression"


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


def extract_channel_adapter_beans_from_source(
    source: str,
    path: Path,
    *,
    store: Optional[ConfigValueStore] = None,
    active_profile: str = BASE_PROFILE,
) -> Tuple[List[PubsubChannelAdapterBean], List[str]]:
    """Extract ``@Bean``-declared ``PubSubInboundChannelAdapter`` beans.

    Recognises a method annotated ``@Bean`` whose body constructs
    ``new PubSubInboundChannelAdapter(<template>, <subscription>)`` and then
    calls ``.setOutputChannel(<channel>)`` on the resulting local variable.
    Per-file and side-effect-free; this does not attempt the cross-file join
    to a ``@ServiceActivator`` (that is Task 3's job, in ``pubsub_scanner.py``).
    """
    beans: List[PubsubChannelAdapterBean] = []
    warnings: List[str] = []
    path_str = str(path)

    pkg = _extract_package(source)
    spans = _extract_class_spans(source)
    if not spans:
        warnings.append(f"{path}: no top-level class/interface found; skipping")
        return beans, warnings

    def _fqn(span: _ClassSpan, method: str) -> str:
        prefix = f"{pkg}." if pkg else ""
        return f"{prefix}{span.name}.{method}"

    value_fields_cache: dict = {}

    for decl in _iter_method_declarations(source):
        if not re.search(r"@Bean\b", decl.annotations_text):
            continue
        if decl.body_start is None or decl.body_end is None:
            continue
        ctor = _find_adapter_construction(source, decl.body_start, decl.body_end)
        if ctor is None:
            continue
        var_name, args = ctor
        channel_ref = _find_set_output_channel(source, decl.body_start, decl.body_end, var_name)
        if channel_ref is None:
            continue
        span = _enclosing_class(spans, decl.header_start)
        if span is None:
            continue
        value_fields = value_fields_cache.setdefault(
            id(span), _value_fields_in_class(source, span)
        )
        sub_raw, sub_resolved, tier, prov = _classify_subscription(
            args[1], store, active_profile, value_fields
        )
        qualifier = _qualifier_for_param(decl.params_text, channel_ref)
        beans.append(PubsubChannelAdapterBean(
            fqn=_fqn(span, decl.name),
            subscription_raw=sub_raw,
            subscription_resolved=sub_resolved,
            confidence=tier,
            provenance=prov,
            output_channel_ref=channel_ref,
            output_channel_qualifier=qualifier,
            source_file=path_str,
            line=_offset_to_line(source, decl.header_start),
        ))

    return beans, warnings


def extract_service_activators_from_source(
    source: str,
    path: Path,
) -> Tuple[List[PubsubServiceActivator], List[str]]:
    """Extract ``@ServiceActivator(inputChannel = <value>)`` methods.

    ``<value>`` is captured verbatim from source (string literal with quotes,
    or a dotted ``Constants.X``-style reference) — see the Task 2 grounding
    correction for why both forms must be preserved without normalization.
    """
    activators: List[PubsubServiceActivator] = []
    warnings: List[str] = []
    path_str = str(path)

    pkg = _extract_package(source)
    spans = _extract_class_spans(source)
    if not spans:
        warnings.append(f"{path}: no top-level class/interface found; skipping")
        return activators, warnings

    def _fqn(span: _ClassSpan, method: str) -> str:
        prefix = f"{pkg}." if pkg else ""
        return f"{prefix}{span.name}.{method}"

    for decl in _iter_method_declarations(source):
        m = _SERVICE_ACTIVATOR_RE.search(decl.annotations_text)
        if m is None:
            continue
        span = _enclosing_class(spans, decl.header_start)
        if span is None:
            continue
        raw = m.group(1).strip()
        activators.append(PubsubServiceActivator(
            fqn=_fqn(span, decl.name),
            input_channel_raw=raw,
            input_channel_resolved=_resolve_input_channel_literal(raw),
            source_file=path_str,
            line=_offset_to_line(source, decl.header_start),
        ))

    return activators, warnings


__all__ = [
    "PubsubChannelAdapterBean",
    "PubsubExtractionResult",
    "PubsubPublisherRecord",
    "PubsubServiceActivator",
    "extract_channel_adapter_beans_from_source",
    "extract_publishers_from_source",
    "extract_service_activators_from_source",
    "looks_like_pubsub_source",
]
