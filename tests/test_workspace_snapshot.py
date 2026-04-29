import io
import zipfile

import pytest

import mainsequence.client.command_center.workspace_snapshot as workspace_snapshot


def test_build_storage_payload_uses_explicit_auth_mode():
    tokens = {
        "accessToken": "runtime-access",
        "tokenType": "Bearer",
        "refreshToken": None,
    }

    payload = workspace_snapshot._build_storage_payload("runtime_credential", tokens)

    assert payload == {
        "authMode": "runtime_credential",
        "tokens": tokens,
    }


def test_build_storage_payload_requires_auth_mode():
    with pytest.raises(workspace_snapshot._WorkspaceSnapshotError, match="auth_mode is required"):
        workspace_snapshot._build_storage_payload("", {"accessToken": "access"})


def test_build_workspace_url_without_snapshot():
    url = workspace_snapshot._build_workspace_url("http://localhost:5173", 42)

    assert url == "http://localhost:5173/app/workspace-studio/workspaces?workspace=42"


def test_build_workspace_url_with_snapshot():
    url = workspace_snapshot._build_workspace_url("http://localhost:5173", 42, snapshot=True)

    assert (
        url
        == "http://localhost:5173/app/workspace-studio/workspaces?workspace=42&snapshot=true&snapshotProfile=full-data"
    )


def test_refresh_and_collect_tokens_delegates_to_auth_loaders(monkeypatch):
    calls = []

    class _FakeAuthLoaders:
        def refresh_headers(self, force=False):
            calls.append(force)
            monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "runtime-access")
            monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
            return {"Authorization": "Bearer runtime-access"}

    monkeypatch.setattr(workspace_snapshot, "AuthLoaders", _FakeAuthLoaders)
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    tokens = workspace_snapshot._refresh_and_collect_tokens()

    assert calls == [False]
    assert tokens == {
        "accessToken": "runtime-access",
        "tokenType": "Bearer",
        "refreshToken": None,
    }


def test_refresh_and_collect_tokens_includes_refresh_token_when_present(monkeypatch):
    class _FakeAuthLoaders:
        def refresh_headers(self, force=False):
            return {"Authorization": "Bearer jwt-access"}

    monkeypatch.setattr(workspace_snapshot, "AuthLoaders", _FakeAuthLoaders)
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")

    tokens = workspace_snapshot._refresh_and_collect_tokens()

    assert tokens == {
        "accessToken": "jwt-access",
        "tokenType": "Bearer",
        "refreshToken": "jwt-refresh",
    }


def test_refresh_and_collect_tokens_does_not_reject_session_jwt(monkeypatch):
    class _FakeAuthLoaders:
        def refresh_headers(self, force=False):
            return {"Authorization": "Bearer session-access"}

    monkeypatch.setattr(workspace_snapshot, "AuthLoaders", _FakeAuthLoaders)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "session-access")
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    tokens = workspace_snapshot._refresh_and_collect_tokens()

    assert tokens == {
        "accessToken": "session-access",
        "tokenType": "Bearer",
        "refreshToken": None,
    }


def test_refresh_and_collect_tokens_requires_access_after_refresh(monkeypatch):
    class _FakeAuthLoaders:
        def refresh_headers(self, force=False):
            monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
            return {}

    monkeypatch.setattr(workspace_snapshot, "AuthLoaders", _FakeAuthLoaders)
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    with pytest.raises(workspace_snapshot._WorkspaceSnapshotError, match="MAINSEQUENCE_ACCESS_TOKEN"):
        workspace_snapshot._refresh_and_collect_tokens()


def test_snapshot_output_dir_defaults_to_home_workspaces(monkeypatch, tmp_path):
    default_dir = tmp_path / "mainsequence" / "workspaces"
    monkeypatch.setattr(workspace_snapshot, "_default_snapshot_output_dir", lambda: default_dir)
    monkeypatch.setattr(workspace_snapshot, "_snapshot_timestamp", lambda: "20260423T101112Z")

    resolved = workspace_snapshot._resolve_snapshot_output_dir(
        None,
        42,
    )

    assert resolved == default_dir / "workspace-42-20260423T101112Z"


def test_snapshot_output_dir_uses_workspace_id_fallback(monkeypatch, tmp_path):
    default_dir = tmp_path / "mainsequence" / "workspaces"
    monkeypatch.setattr(workspace_snapshot, "_default_snapshot_output_dir", lambda: default_dir)
    monkeypatch.setattr(workspace_snapshot, "_snapshot_timestamp", lambda: "20260423T101112Z")

    resolved = workspace_snapshot._resolve_snapshot_output_dir(None, 42)

    assert resolved == default_dir / "workspace-42-20260423T101112Z"


def test_snapshot_output_dir_sanitizes_workspace_id(monkeypatch, tmp_path):
    default_dir = tmp_path / "mainsequence" / "workspaces"
    monkeypatch.setattr(workspace_snapshot, "_default_snapshot_output_dir", lambda: default_dir)
    monkeypatch.setattr(workspace_snapshot, "_snapshot_timestamp", lambda: "20260423T101112Z")

    resolved = workspace_snapshot._resolve_snapshot_output_dir(
        None,
        "../Unsafe\\Workspace:Id",
    )

    assert resolved == default_dir / "workspace-Workspace-Id-20260423T101112Z"


def test_snapshot_output_dir_accepts_existing_directory(tmp_path):
    output_dir = tmp_path / "exports"
    output_dir.mkdir()

    resolved = workspace_snapshot._resolve_snapshot_output_dir(
        output_dir,
        42,
    )

    assert resolved == output_dir


def test_snapshot_output_dir_uses_file_stem_for_explicit_file_path(tmp_path):
    output_file = tmp_path / "custom-name.zip"

    resolved = workspace_snapshot._resolve_snapshot_output_dir(
        output_file,
        42,
    )

    assert resolved == tmp_path / "custom-name"


def test_snapshot_output_dir_treats_suffixless_explicit_path_as_directory(tmp_path):
    output_dir = tmp_path / "custom-folder"

    resolved = workspace_snapshot._resolve_snapshot_output_dir(output_dir, 42)

    assert resolved == output_dir


def test_extract_archive_to_directory_unpacks_files(tmp_path):
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", '{"workspaceId": 42}')
        archive.writestr("screenshots/view.png", b"png-bytes")

    extract_dir = tmp_path / "snapshot"
    resolved = workspace_snapshot._extract_archive_to_directory(archive_buffer.getvalue(), extract_dir)

    assert resolved == extract_dir
    assert (extract_dir / "manifest.json").read_text(encoding="utf-8") == '{"workspaceId": 42}'
    assert (extract_dir / "screenshots" / "view.png").read_bytes() == b"png-bytes"


def test_default_bootstrap_wait_is_30_seconds():
    assert workspace_snapshot._DEFAULT_BOOTSTRAP_WAIT_MS == 30_000
