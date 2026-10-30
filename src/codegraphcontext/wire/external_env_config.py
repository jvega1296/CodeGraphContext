"""Discover externalized environment-variable config (MULTI_REPO_LINKS).

Real Spring services frequently defer a `@Value("${...}")` placeholder to a
bare OS environment variable with no literal value anywhere the application
code itself resolves it from (e.g.
`az.pubsub.subscription.name: ${PUB_SUB_SUBSCRIPTION_NAME}` in
application.yml). Where the real value actually lives varies a lot by team
and by service:

* a separate "ops config" repo entirely (Cloud Run service configs, Cloud
  Run job configs, Kubernetes/Helm values files -- each team may have more
  than one, named however they like);
* checked into the SAME repo as the application code, under a deploy/CI
  directory (e.g. a `cicd/` folder holding the service's own Cloud Run/K8s
  manifests).

This module hardcodes neither a specific sibling repo name nor a specific
in-repo directory name. It searches two places generically:

1. The indexed repo's own tree, for any YAML file (deploy manifests commonly
   live under a `cicd`/`deploy`/`k8s`/`helm`/`ops`-ish directory, but no
   specific name is required -- the shape check below is what filters out
   false positives, not the directory name).
2. Every sibling repo under the same workspace, narrowed to directories
   whose name matches the indexed repo's OWN directory name (the convention
   every cross-repo config layout observed so far actually uses:
   `<domain>/<service-dir-name>/<env>/config.yaml` or `.../values.yaml`) --
   unlike (1), siblings are themselves often large multi-service config
   monorepos, so this narrowing avoids scanning or matching unrelated
   services' config.

Either way, a YAML file is only ever treated as a config source if it parses
to a dict with a top-level `env:` list of ``{name, value}`` pairs -- the one
shape shared by every example seen (Cloud Run service configs, Cloud Run job
configs, and Helm/Kubernetes values files all use it). A repo with no
sibling and no in-repo match simply contributes nothing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

# Directories that commonly contain a top-level `env:` key for a completely
# unrelated reason (CI pipeline steps, not deployment manifests) and would
# otherwise produce false-positive matches when searching a repo's own tree
# broadly. Deployment-manifest directories are deliberately NOT listed here
# (no specific name, e.g. "cicd", is assumed) -- only known-unrelated tooling
# directories are excluded.
_SKIP_DIR_NAMES = frozenset({
    ".git", ".github", ".gitlab", ".circleci", ".idea", ".vscode",
    "target", "build", "node_modules", "dist", "out", ".venv", "venv",
})


@dataclass
class ExternalEnvConfig:
    env_vars: Dict[str, str] = field(default_factory=dict)
    active_profile: Optional[str] = None
    source_files: List[str] = field(default_factory=list)


def _parse_env_list(doc: object) -> Dict[str, str]:
    """Extract ``{name: value}`` pairs from a parsed YAML doc's top-level
    ``env:`` list, skipping entries with no static ``value`` (e.g. Kubernetes
    ``valueFrom: secretKeyRef: ...`` references, which carry no literal this
    module could ever recover statically)."""
    if not isinstance(doc, dict):
        return {}
    entries = doc.get("env")
    if not isinstance(entries, list):
        return {}
    result: Dict[str, str] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if isinstance(name, str) and isinstance(value, (str, int, float, bool)):
            result[name] = str(value)
    return result


def _walk_yaml_files(root: Path, *, skip_dirs: bool) -> List[Path]:
    """Return every ``.yaml``/``.yml`` file under ``root``.

    ``skip_dirs=True`` prunes ``_SKIP_DIR_NAMES`` (used for the broad,
    whole-repo-tree walk); the narrower sibling-repo walk already starts
    from a specific `<service-name>` directory and does not need pruning.
    """
    found: List[Path] = []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(current.iterdir())
        except OSError:
            continue
        for entry in entries:
            if entry.is_dir():
                if skip_dirs and entry.name in _SKIP_DIR_NAMES:
                    continue
                stack.append(entry)
            elif entry.is_file() and entry.suffix in (".yaml", ".yml"):
                found.append(entry)
    return found


def discover_external_env_config(
    repo_root: Path, *, workspace_root: Optional[Path] = None, max_files: int = 200,
) -> ExternalEnvConfig:
    """Search for externalized env-var config for ``repo_root``.

    ``workspace_root`` is the directory containing every sibling repo to
    additionally search (defaults to ``repo_root``'s own parent -- the
    common layout this acceptance run already uses: multiple independent
    repo checkouts side by side under one workspace directory).

    ``max_files`` bounds how many matching YAML files are actually parsed
    (across both the in-repo and sibling-repo searches combined), so neither
    an unexpectedly large service tree nor an unexpectedly large config repo
    can make indexing hang; the first ``max_files`` found (sorted for
    determinism) are used and the rest are silently not consulted -- every
    real example seen so far duplicates the same env-var value across
    environments after normalization, so this is not expected to lose real
    answers.
    """
    repo_root = Path(repo_root).resolve()
    workspace_root = Path(workspace_root).resolve() if workspace_root else repo_root.parent
    service_name = repo_root.name

    result = ExternalEnvConfig()

    candidate_files: List[Path] = []

    # 1. The indexed repo's own tree (e.g. special-order-export-listener's
    #    own `cicd/` folder keeping its Cloud Run manifest alongside the
    #    application code, rather than in a separate ops-config repo).
    if repo_root.is_dir():
        candidate_files.extend(_walk_yaml_files(repo_root, skip_dirs=True))

    # 2. Every sibling repo, narrowed to directories named after this repo.
    if workspace_root.is_dir():
        for sibling in sorted(workspace_root.iterdir()):
            if not sibling.is_dir() or sibling.resolve() == repo_root:
                continue
            try:
                service_dirs = [p for p in sibling.rglob(service_name) if p.is_dir()]
            except OSError:
                continue
            for service_dir in service_dirs:
                candidate_files.extend(_walk_yaml_files(service_dir, skip_dirs=False))

    candidate_files = sorted(set(candidate_files))[:max_files]

    for f in candidate_files:
        try:
            doc = yaml.safe_load(f.read_text(encoding="utf-8", errors="replace"))
        except (OSError, yaml.YAMLError):
            continue
        env_vars = _parse_env_list(doc)
        if not env_vars:
            continue
        result.source_files.append(str(f))
        for key, value in env_vars.items():
            result.env_vars.setdefault(key, value)
        if result.active_profile is None and "spring_profiles_active" in env_vars:
            # Spring accepts a comma-separated profile list; the first entry
            # is this module's reasonable, documented choice of "the" active
            # profile for ConfigValueStore.resolve_placeholders's single
            # active_profile parameter.
            result.active_profile = env_vars["spring_profiles_active"].split(",")[0].strip()

    return result


__all__ = ["ExternalEnvConfig", "discover_external_env_config"]
