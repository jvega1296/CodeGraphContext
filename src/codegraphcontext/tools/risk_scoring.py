"""Core pure risk-scoring factors ported from code-review-graph.

These functions are deliberately pure (no graph access, no I/O) so that
Task 16 can combine them with the churn and test-coverage factors (Tasks
14-15) inside ``analyze_impact`` without re-deriving the formulas here.
"""

from __future__ import annotations

from typing import List

from codegraphcontext.wire.kafka_scanner import DEFAULT_JAVA_SOURCE_DIRS

SECURITY_KEYWORDS: frozenset = frozenset({
    "auth", "login", "password", "token", "session", "crypt", "secret",
    "credential", "permission", "sql", "query", "execute", "connect",
    "socket", "request", "http", "sanitize", "validate", "encrypt",
    "decrypt", "hash", "sign", "verify", "admin", "privilege",
})


def security_sensitivity_score(name: str, qualified_name: str) -> float:
    """0.20 if name or qualified_name contains a security keyword (case-insensitive substring), else 0.0."""
    haystack = f"{name} {qualified_name}".lower()
    if any(keyword in haystack for keyword in SECURITY_KEYWORDS):
        return 0.20
    return 0.0


def caller_count_score(caller_count: int) -> float:
    """min(caller_count / 20.0, 0.10) -- exact port of code-review-graph's formula."""
    return min(caller_count / 20.0, 0.10)


def _top_level_module_segment(path: str) -> str:
    """Return the comparison key used to decide whether two paths cross a module boundary.

    Per the Java source-root convention (``DEFAULT_JAVA_SOURCE_DIRS``), strip the
    language-root marker (``src/main/java`` or ``src/main/kotlin``) and the filename,
    then use the last remaining directory segment -- i.e. the directory that directly
    contains the file. This is the file's feature-module directory (e.g. "order",
    "shipping", "payment") and is what actually distinguishes same-module callers from
    cross-module callers for Java packages that share a common org-wide prefix such as
    "com/az/oms". The third ("." ) entry in DEFAULT_JAVA_SOURCE_DIRS is a last-resort
    marker, not a literal path-splitting substring, so it is skipped here.
    """
    posix_path = path.replace("\\", "/")
    parts = [part for part in posix_path.split("/") if part]

    for marker in DEFAULT_JAVA_SOURCE_DIRS:
        if marker == ".":
            continue
        marker_parts = marker.split("/")
        window = len(marker_parts)
        for i in range(len(parts) - window + 1):
            if parts[i:i + window] == marker_parts:
                remainder = parts[i + window:-1]
                if remainder:
                    return remainder[-1]

    if len(parts) >= 2:
        return parts[-2]
    return posix_path


def community_crossing_score(target_path: str, caller_paths: List[str]) -> float:
    """0.05 per caller whose top-level module/package directory differs from the target's, capped at 0.15.

    Proxy for code-review-graph's Leiden-community-crossing factor -- see Decision #4.
    """
    target_segment = _top_level_module_segment(target_path)
    crossings = sum(
        1 for caller_path in caller_paths
        if _top_level_module_segment(caller_path) != target_segment
    )
    return min(crossings * 0.05, 0.15)
