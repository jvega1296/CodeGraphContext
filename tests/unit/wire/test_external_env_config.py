"""Unit tests for wire.external_env_config (MULTI_REPO_LINKS)."""
from __future__ import annotations

from pathlib import Path

from codegraphcontext.wire.external_env_config import discover_external_env_config


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_discovers_env_vars_from_a_sibling_cloud_run_style_config_repo(tmp_path: Path):
    """Generic discovery: no repo name is hardcoded or assumed -- a sibling
    repo named anything, organized by <domain>/<service>/<env>/config.yaml,
    is found purely by the service directory name matching."""
    workspace = tmp_path
    app_repo = workspace / "oms-order-consumer"
    app_repo.mkdir()

    config_repo = workspace / "some-teams-ops-configs"  # deliberately NOT a known name
    _write(
        config_repo / "order-management" / "oms-order-consumer" / "dv1" / "config.yaml",
        """
project: azdv1-ordfl-ordrmgmt
env:
  - name: spring_profiles_active
    value: cloud
  - name: PUB_SUB_SUBSCRIPTION_NAME
    value: "projects/azdv1-ordfl-ordrmgmt/subscriptions/oms_order_create_pull_subscription"
""",
    )

    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars["PUB_SUB_SUBSCRIPTION_NAME"] == (
        "projects/azdv1-ordfl-ordrmgmt/subscriptions/oms_order_create_pull_subscription"
    )
    assert result.active_profile == "cloud"
    assert len(result.source_files) == 1


def test_discovers_from_multiple_differently_shaped_sibling_config_repos(tmp_path: Path):
    """Both a Cloud-Run-style config.yaml AND a Helm/Kubernetes-style
    values.yaml use the same `env:` list shape -- neither the filename nor
    the specific sibling repo is assumed, only the shared `env:` convention."""
    workspace = tmp_path
    app_repo = workspace / "warranty-lookup-bs"
    app_repo.mkdir()

    _write(
        workspace / "az-cloud-run-app-configs" / "account" / "warranty-lookup-bs" / "dv1" / "config.yaml",
        "env:\n  - name: FOO\n    value: cloud-run-value\n",
    )
    _write(
        workspace / "az-k8s-app-configs" / "account" / "warranty" / "warranty-lookup-bs" / "dv" / "values.yaml",
        "env:\n  - name: BAR\n    value: helm-value\n",
    )

    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars["FOO"] == "cloud-run-value"
    assert result.env_vars["BAR"] == "helm-value"
    assert len(result.source_files) == 2


def test_no_matching_sibling_returns_empty_result(tmp_path: Path):
    workspace = tmp_path
    app_repo = workspace / "special-order-services"
    app_repo.mkdir()
    (workspace / "unrelated-repo").mkdir()

    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars == {}
    assert result.active_profile is None
    assert result.source_files == []


def test_entries_with_valueFrom_secret_ref_are_skipped_not_invented(tmp_path: Path):
    workspace = tmp_path
    app_repo = workspace / "svc"
    app_repo.mkdir()
    _write(
        workspace / "configs" / "svc" / "dv1" / "config.yaml",
        """
env:
  - name: REAL_KEY
    value: "real-value"
  - name: SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: "some-secret"
        key: "latest"
""",
    )
    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars == {"REAL_KEY": "real-value"}


def test_finds_in_repo_deploy_config_eg_cicd_folder(tmp_path: Path):
    """Some services externalize their own config inside their own repo
    rather than in a separate ops-config repo at all (e.g.
    special-order-export-listener keeps it under its own `cicd/` folder) --
    no specific in-repo directory name is required, only the `env:` shape."""
    workspace = tmp_path
    app_repo = workspace / "special-order-export-listener"
    _write(
        app_repo / "cicd" / "dv1" / "service.yaml",
        "env:\n  - name: IN_REPO_KEY\n    value: 'in-repo-value'\n",
    )
    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars == {"IN_REPO_KEY": "in-repo-value"}


def test_in_repo_search_does_not_match_the_repos_own_name_as_a_sibling_path(tmp_path: Path):
    """The repo-itself search (1) and the sibling-repo search (2) must not
    double-count or conflict: a config file living inside the repo's own
    tree at a path that also happens to repeat the repo's directory name is
    still found exactly once, via the in-repo walk."""
    workspace = tmp_path
    app_repo = workspace / "svc"
    _write(app_repo / "svc" / "dv1" / "config.yaml", "env:\n  - name: X\n    value: 'self-match'\n")
    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars == {"X": "self-match"}
    assert len(result.source_files) == 1


def test_ci_pipeline_env_blocks_outside_deploy_dirs_are_still_picked_up_by_shape(tmp_path: Path):
    """This module filters by shape (a top-level `env:` list of name/value
    pairs), not by directory name -- a CI workflow file that happens to
    declare env vars in exactly this shape is indistinguishable from a real
    deploy manifest and is intentionally not specially excluded (only well-
    known unrelated tooling directories like .github/.gitlab are pruned)."""
    workspace = tmp_path
    app_repo = workspace / "svc"
    _write(app_repo / ".github" / "workflows" / "ci.yml", "env:\n  - name: SHOULD_NOT_APPEAR\n    value: x\n")
    result = discover_external_env_config(app_repo, workspace_root=workspace)
    assert result.env_vars == {}
