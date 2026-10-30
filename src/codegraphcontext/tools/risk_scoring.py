"""Core pure risk-scoring factors ported from code-review-graph.

These functions are deliberately pure (no graph access, no I/O) so that
Task 16 can combine them with the churn and test-coverage factors (Tasks
14-15) inside ``analyze_impact`` without re-deriving the formulas here.
"""

from __future__ import annotations

import logging
import math
import os
import re
import subprocess
import threading
from typing import Dict, List, Optional, Tuple

from codegraphcontext.wire.kafka_scanner import DEFAULT_JAVA_SOURCE_DIRS

logger = logging.getLogger(__name__)

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


def _strip_source_root(path: str) -> Optional[List[str]]:
    """Return the path segments after a ``DEFAULT_JAVA_SOURCE_DIRS`` marker,
    filename included, or ``None`` when no marker matches with a non-empty
    directory remainder.

    Shared by :func:`_top_level_module_segment` (keeps only the last
    directory segment) and :func:`qualified_name_proxy` (keeps every
    segment, filename included, as a package-like proxy). Matches markers
    exactly as the original single-use version of this loop did: it keeps
    searching later marker positions/markers rather than stopping at the
    first match whose directory remainder (the segments between the marker
    and the filename) is empty, so a marker match that sits directly above
    the file with no package directories in between does not shadow a real
    match found elsewhere in the path. The third ("." ) entry in
    DEFAULT_JAVA_SOURCE_DIRS is a last-resort marker, not a literal
    path-splitting substring, so it is skipped here.
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
                remainder = parts[i + window:]
                if remainder[:-1]:
                    return remainder
    return None


def _top_level_module_segment(path: str) -> str:
    """Return the comparison key used to decide whether two paths cross a module boundary.

    Per the Java source-root convention (``DEFAULT_JAVA_SOURCE_DIRS``), strip the
    language-root marker (``src/main/java`` or ``src/main/kotlin``) and the filename,
    then use the last remaining directory segment -- i.e. the directory that directly
    contains the file. This is the file's feature-module directory (e.g. "order",
    "shipping", "payment") and is what actually distinguishes same-module callers from
    cross-module callers for Java packages that share a common org-wide prefix such as
    "com/az/oms".
    """
    remainder = _strip_source_root(path)
    if remainder:
        return remainder[:-1][-1]

    posix_path = path.replace("\\", "/")
    parts = [part for part in posix_path.split("/") if part]
    if len(parts) >= 2:
        return parts[-2]
    return posix_path


def qualified_name_proxy(path: str) -> str:
    """Package-relative proxy for a qualified name, for callers (Task 16's
    ``analyze_impact``) that only have a raw file path and no real
    ``qualified_name`` field to pass to :func:`security_sensitivity_score`.

    Keeps only the segments after a ``DEFAULT_JAVA_SOURCE_DIRS`` marker
    (the package-equivalent directories) plus the file's basename (extension
    stripped), joined with ``"."`` -- e.g.
    ``"src/main/java/com/az/oms/order/OrderValidator.java"`` becomes
    ``"com.az.oms.order.OrderValidator"``. This deliberately drops the
    repo-name and any outer directory segments above the source root: those
    are noise an unrelated hyphenated service name (e.g. "oms-order-consumer")
    could coincidentally share a ``SECURITY_KEYWORDS`` word with (``session``,
    ``request``, ``query``, ...), producing a false-positive security match
    that has nothing to do with the function actually being scored.

    Falls back to the bare filename (extension stripped) when no source-root
    marker matches -- still excludes the unrelated outer path segments, even
    though there is no package-qualification signal left to include.
    """
    remainder = _strip_source_root(path)
    if remainder is None:
        posix_path = path.replace("\\", "/")
        parts = [part for part in posix_path.split("/") if part]
        remainder = parts[-1:]

    if not remainder:
        return ""

    *package_parts, filename = remainder
    basename = filename.rsplit(".", 1)[0] if "." in filename else filename
    return ".".join([*package_parts, basename])


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


def find_test_coverage_count(session, function_name: str, function_path: str) -> int:
    """Count of Function nodes matching a Java/JUnit test-naming convention
    (test<Name> / <name>Test / <Name>Test) that are co-located under the
    mirrored src/test tree for function_path's package. Lightweight proxy
    for a real TESTED_BY edge -- see Decision #4.

    Scoped to Java/JUnit naming only, per Decision #4: the src/main -> src/test
    substitution covers both ``src/main/java`` and ``src/main/kotlin`` -- the
    two source roots DEFAULT_JAVA_SOURCE_DIRS already treats as siblings, and
    Kotlin-on-JVM code commonly uses JUnit too -- but this does not generalize
    to any other language's source-tree convention; an unrecognized layout
    returns 0 rather than guessing.
    """
    posix_path = function_path.replace("\\", "/")
    directory, _, _filename = posix_path.rpartition("/")
    if not directory:
        return 0

    if "src/main/java" in directory:
        test_directory = directory.replace("src/main/java", "src/test/java", 1)
    elif "src/main/kotlin" in directory:
        test_directory = directory.replace("src/main/kotlin", "src/test/kotlin", 1)
    else:
        return 0

    test_tree_prefix = f"{test_directory}/"
    candidate_names = [
        f"test{function_name[0].upper()}{function_name[1:]}",
        f"{function_name}Test",
        f"{function_name[0].upper()}{function_name[1:]}Test",
    ]

    cypher = """
    MATCH (fn:Function)
    WHERE fn.path STARTS WITH $test_tree_prefix AND fn.name IN $candidate_names
    RETURN count(fn) as count
    """
    result = session.run(cypher, test_tree_prefix=test_tree_prefix, candidate_names=candidate_names)
    row = result.single()
    return row["count"] if row else 0


def test_coverage_score(test_count: int) -> float:
    """0.30 - (min(test_count / 5.0, 1.0) * 0.25) -- exact port of the formula."""
    return 0.30 - (min(test_count / 5.0, 1.0) * 0.25)


_warned_env_vars: set[str] = set()


def _warn_invalid_env(name: str, raw: str, default: object) -> None:
    """Warn once per variable that its value was ignored."""
    if name in _warned_env_vars:
        return
    _warned_env_vars.add(name)
    logger.warning(
        "Ignoring invalid %s=%r (not a number); using the default %s.",
        name,
        raw,
        default,
    )


def env_int(name: str, default: int) -> int:
    """Read *name* as an int, falling back to *default* when it is unusable.

    An unset variable is the default silently; a set-but-unparseable one
    (empty string, word, version number) is the default *and* a warning
    naming the variable, so the operator can find the typo.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        _warn_invalid_env(name, raw, default)
        return default


def env_float(name: str, default: float) -> float:
    """Read *name* as a float. See :func:`env_int` for the fallback rules.

    NaN and infinity are rejected alongside unparseable text: they are
    accepted by ``float()`` but poison every comparison they reach.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw.strip())
    except (TypeError, ValueError):
        _warn_invalid_env(name, raw, default)
        return default
    if not math.isfinite(value):
        _warn_invalid_env(name, raw, default)
        return default
    return value


_CHURN_SATURATION = 10.0
_CHURN_WEIGHT = 0.15
_NUMSTAT_COUNT = re.compile(r"^(?:\d+|-)$")

# ``git log --since --numstat`` now runs inside MCP tool calls, where an
# agent is blocked on the result, so it gets its own budget rather than the
# 30-second diff timeout: the history walk is capped at a commit count, and a
# slow repository degrades to the pre-churn behaviour (an empty mapping, and
# therefore a zero change-frequency term) instead of hanging the call.
_CHURN_TIMEOUT = env_float("CGC_CHURN_TIMEOUT", 5.0)
_CHURN_MAX_COMMITS = env_int("CGC_CHURN_MAX_COMMITS", 2000)

# Churn counts commits, so the answer only changes when HEAD does: successful
# results are keyed by (repo, commit, window).
_CHURN_CACHE_MAX_ENTRIES = 32
_CHURN_CACHE_LOCK = threading.Lock()
_CHURN_CACHE: Dict[Tuple[str, str, int], Dict[str, int]] = {}

# Failures cannot be keyed by commit, because the commit is the first thing a
# slow or broken repository fails to tell us: ``git rev-parse`` runs under the
# same budget as the walk, so on the repositories the timeout exists for it
# times out too, and a commit-keyed failure cache never gets a key to store
# anything under. That is the whole "pays the timeout once" claim gone -- a
# 5-second rev-parse plus a 5-second walk, on every tool call, for the rest of
# the session.
#
# Churn being unavailable is therefore recorded per (repo, window) for the
# life of the process, ahead of any git call. It is deliberately sticky: the
# condition it records is a property of the repository (no Git, no commits, a
# history too slow to walk inside a tool call), not a transient. Callers that
# outlive a repository's state -- a long-lived MCP server, a test suite --
# clear it with ``clear_churn_cache``.
_CHURN_UNAVAILABLE: set = set()

# Status values reported alongside the counts, so a degraded review says so
# rather than quietly scoring the change-frequency term at zero.
CHURN_OK = "ok"
CHURN_UNAVAILABLE = "unavailable"
CHURN_OFF = "off"


def clear_churn_cache() -> None:
    """Drop every memoised churn result, successful and failed alike."""
    with _CHURN_CACHE_LOCK:
        _CHURN_CACHE.clear()
        _CHURN_UNAVAILABLE.clear()


def _churn_is_unavailable(fail_key: Tuple[str, int]) -> bool:
    with _CHURN_CACHE_LOCK:
        return fail_key in _CHURN_UNAVAILABLE


def _mark_churn_unavailable(fail_key: Tuple[str, int]) -> None:
    with _CHURN_CACHE_LOCK:
        if len(_CHURN_UNAVAILABLE) >= _CHURN_CACHE_MAX_ENTRIES:
            _CHURN_UNAVAILABLE.clear()
        _CHURN_UNAVAILABLE.add(fail_key)
    logger.warning(
        "Change-frequency risk disabled for %s: git history could not be "
        "read within %.3gs. Risk scores exclude the churn term.",
        fail_key[0], _CHURN_TIMEOUT,
    )


def _churn_cache_key(repo_root: str, window_days: int) -> Optional[Tuple[str, str, int]]:
    """Identify the commit churn was computed at, or None when unknown.

    An unknown commit (no Git, a repository without commits, a ``rev-parse``
    that did not answer in time) is not a transient: it is the same condition
    that stops the walk below from answering, so the caller records churn as
    unavailable rather than retrying it on every subsequent call.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=repo_root,
            timeout=_CHURN_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    head = result.stdout.strip()
    return (repo_root, head, window_days) if head else None


def _parse_numstat(log_text: str) -> Dict[str, int]:
    """Parse NUL-terminated ``git log --numstat -z`` records.

    NUL termination is required for correctness: Git's default line format
    quotes unusual paths, while ``-z`` preserves tabs and newlines in file
    names without making the graph-path lookup ambiguous.
    """
    counts: Dict[str, int] = {}
    for record in log_text.split("\0"):
        if not record:
            continue
        fields = record.split("\t", 2)
        if len(fields) != 3:
            continue
        added, deleted, path = fields
        if (
            not path
            or _NUMSTAT_COUNT.fullmatch(added) is None
            or _NUMSTAT_COUNT.fullmatch(deleted) is None
        ):
            continue
        counts[path] = counts.get(path, 0) + 1
    return counts


def compute_file_churn(
    repo_root: str,
    window_days: Optional[int] = None,
) -> Dict[str, int]:
    """Count commits touching each file over a trailing window.

    Thin wrapper over :func:`compute_file_churn_with_status` for callers that
    only want the counts.
    """
    return compute_file_churn_with_status(repo_root, window_days)[0]


def compute_file_churn_with_status(
    repo_root: str,
    window_days: Optional[int] = None,
) -> Tuple[Dict[str, int], str]:
    """Count commits touching each file, and say whether the count is real.

    Memoised per ``(repo, HEAD commit, window)``: the answer cannot change
    until a new commit lands, and the walk is too expensive to repeat inside
    every tool call. The history walk is capped at ``_CHURN_MAX_COMMITS`` and
    given ``_CHURN_TIMEOUT`` seconds.

    Returns ``({}, CHURN_UNAVAILABLE)`` when Git is slow, missing, or fails,
    which leaves the change-frequency risk term at zero -- exactly the
    behaviour callers had before churn was enabled for them -- and records
    that for the life of the process so the next call costs nothing. An
    invalid or non-positive window is ``CHURN_OFF``: churn was switched off,
    not attempted and lost. Renames are deliberately not followed: churn
    belongs to the path that existed in each commit.
    """
    if window_days is None:
        raw_window = os.environ.get("CGC_CHURN_WINDOW_DAYS", "90")
        try:
            window_days = int(raw_window)
        except ValueError:
            logger.warning(
                "Invalid CGC_CHURN_WINDOW_DAYS value %r; churn disabled",
                raw_window,
            )
            return {}, CHURN_OFF
    if window_days <= 0:
        return {}, CHURN_OFF

    # Checked before any subprocess: on the repositories this exists for, the
    # cheap-looking rev-parse below is itself what times out.
    fail_key = (repo_root, window_days)
    if _churn_is_unavailable(fail_key):
        return {}, CHURN_UNAVAILABLE

    cache_key = _churn_cache_key(repo_root, window_days)
    if cache_key is None:
        _mark_churn_unavailable(fail_key)
        return {}, CHURN_UNAVAILABLE
    with _CHURN_CACHE_LOCK:
        cached = _CHURN_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached), CHURN_OK

    try:
        result = subprocess.run(
            [
                "git",
                "-c",
                "core.quotepath=off",
                "log",
                f"--since={window_days}.days.ago",
                f"--max-count={_CHURN_MAX_COMMITS}",
                "--numstat",
                "--no-renames",
                "--format=",
                "-z",
                "--",
            ],
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=repo_root,
            timeout=_CHURN_TIMEOUT,
        )
        if result.returncode != 0:
            logger.warning(
                "git log failed (rc=%d): %s",
                result.returncode,
                result.stderr[:200],
            )
            _mark_churn_unavailable(fail_key)
            return {}, CHURN_UNAVAILABLE
        counts = _parse_numstat(result.stdout)
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("git log error: %s", exc)
        _mark_churn_unavailable(fail_key)
        return {}, CHURN_UNAVAILABLE

    with _CHURN_CACHE_LOCK:
        if len(_CHURN_CACHE) >= _CHURN_CACHE_MAX_ENTRIES:
            _CHURN_CACHE.clear()
        _CHURN_CACHE[cache_key] = dict(counts)
    return counts, CHURN_OK


def churn_score(commit_count: int) -> float:
    """min(commit_count / _CHURN_SATURATION, 1.0) * _CHURN_WEIGHT -- exact port of code-review-graph's formula."""
    return min(commit_count / _CHURN_SATURATION, 1.0) * _CHURN_WEIGHT
