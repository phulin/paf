from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from starlette.testclient import TestClient

import paf.web as web_module
from paf.cli import main
from paf.config import load_config
from paf.models import PipelineConfig
from paf.state import StateStore
from tests.support import write_project


@pytest.fixture
def static_dir(tmp_path: Path) -> Path:
    root = tmp_path / "web-static"
    assets = root / "assets"
    assets.mkdir(parents=True)
    (root / "index.html").write_text('<main id="root"></main>\n', encoding="utf-8")
    (assets / "index-12345678.js").write_text("export {};\n", encoding="utf-8")
    return root


def _project(tmp_path: Path, *, external_state: Path | None = None):
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    if external_state is not None:
        config_path.write_text(
            config_path.read_text().replace(
                'repo = "."', f'repo = "."\nstate_dir = "{external_state}"'
            )
        )
    config = load_config(config_path)
    target = tmp_path / "lean" / "Book01Example" / "Chapter01" / "Section01Intro.lean"
    target.parent.mkdir(parents=True)
    target.write_text(
        "/-- A documented result. -/\ntheorem Demo.result : True := by\n  trivial\n\n"
        "lemma Demo.open : True := by\n  sorry\n",
        encoding="utf-8",
    )
    return config


def test_app_serves_packaged_assets_and_spa_fallback(tmp_path: Path, static_dir: Path) -> None:
    config = _project(tmp_path)
    with TestClient(web_module.create_app(config, static_dir=static_dir)) as client:
        index = client.get("/")
        fallback = client.get("/statements/result")
        asset_name = next(
            path.relative_to(static_dir).as_posix() for path in static_dir.rglob("*.js")
        )
        asset = client.get(f"/{asset_name}")

        assert index.status_code == fallback.status_code == asset.status_code == 200
        assert "text/html" in index.headers["content-type"]
        assert index.content == fallback.content
        assert "immutable" in asset.headers["cache-control"]
        assert client.get("/assets/missing-deadbeef.js").status_code == 404


def test_state_list_snapshot_and_system_contracts(tmp_path: Path, static_dir: Path) -> None:
    config = _project(tmp_path)
    asyncio.run(StateStore(config).load_or_create())

    with TestClient(web_module.create_app(config, static_dir=static_dir)) as client:
        listing = client.get("/api/swarms")
        alias = client.get("/api/runs")
        assert listing.status_code == alias.status_code == 200
        summaries = listing.json()["swarms"]
        assert summaries and summaries[0]["task_count"] == 4
        assert summaries[0]["revision"] > 0
        assert alias.json()["runs"] == summaries

        selected = client.get("/api/swarm", params={"swarm": summaries[0]["id"]})
        by_path = client.get(f"/api/snapshots/{summaries[0]['id']}")
        assert selected.status_code == by_path.status_code == 200
        assert selected.json()["swarm_id"] == summaries[0]["id"]
        assert selected.json()["project_root"] == str(tmp_path)
        revision = selected.json()["revision"]
        changes = client.get(
            "/api/changes", params={"swarm": summaries[0]["id"], "after": revision}
        ).json()
        assert changes == {
            "revision": revision,
            "resync_required": False,
            "changes": [],
        }
        assert client.get("/api/swarm", params={"swarm": "unknown"}).status_code == 404

        system = client.get("/api/system").json()
        assert system["cpu_percent"] is None or 0 <= system["cpu_percent"] <= 100
        assert 0 <= system["memory_used_bytes"] <= system["memory_total_bytes"]
        assert 0 <= system["memory_percent"] <= 100


def test_external_state_directory_is_the_only_state_root(tmp_path: Path, static_dir: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    external = tmp_path / "state-elsewhere"
    config = _project(project, external_state=external)
    asyncio.run(StateStore(config).load_or_create())
    decoy = project / ".paf" / "decoy"
    decoy.mkdir(parents=True)
    (decoy / "state.json").write_text('{"updated_at":"9999","tasks":{}}')

    with TestClient(web_module.create_app(config, static_dir=static_dir)) as client:
        summaries = client.get("/api/swarms").json()["swarms"]
        assert len(summaries) == 1
        assert summaries[0]["id"] == external.name
        assert client.get("/api/swarm").json()["project_root"] == str(project)


def test_source_and_target_browsing_are_scoped_and_reject_escapes(
    tmp_path: Path, static_dir: Path
) -> None:
    config = _project(tmp_path)
    secret = tmp_path / "secret.txt"
    secret.write_text("not configured source")
    outside = tmp_path.parent / f"{tmp_path.name}-outside.txt"
    outside.write_text("outside")
    symlink = tmp_path / "books" / "escape.txt"
    symlink.symlink_to(outside)
    target_symlink = tmp_path / "lean" / "escape.txt"
    target_symlink.symlink_to(outside)

    with TestClient(web_module.create_app(config, static_dir=static_dir)) as client:
        root = client.get("/api/source")
        source = client.get("/api/source", params={"path": "books/book.md"})
        target = client.get(
            "/api/target",
            params={"path": "Book01Example/Chapter01/Section01Intro.lean"},
        )
        assert root.status_code == source.status_code == target.status_code == 200
        assert source.json()["content"].startswith("# Book")
        assert "theorem Demo.result" in target.json()["content"]
        assert (
            client.get("/api/files", params={"root": "source", "path": "books/book.md"}).status_code
            == 200
        )
        assert client.get("/api/source", params={"path": "secret.txt"}).status_code == 403

        for path in ("../secret.txt", str(outside), "books/escape.txt"):
            assert client.get("/api/source", params={"path": path}).status_code == 403
        assert client.get("/api/target", params={"path": "../paf.toml"}).status_code == 403
        assert client.get("/api/target", params={"path": "escape.txt"}).status_code == 403
        assert (
            client.get("/api/files", params={"root": "state", "path": "state.json"}).status_code
            == 400
        )
        assert client.get("/api/not-an-endpoint/extra").status_code == 404


def test_declarations_preserve_frontend_filter_contract(tmp_path: Path, static_dir: Path) -> None:
    config = _project(tmp_path)
    with TestClient(web_module.create_app(config, static_dir=static_dir)) as client:
        response = client.get("/api/statements")
        assert response.status_code == 200
        payload = response.json()
        assert payload["source"] == "repository"
        assert payload["total"] == 2
        assert payload["facets"]["statuses"] == {"proved": 1, "sorry": 1}
        assert payload["declarations"][0]["doc"] == "A documented result."

        filtered = client.get(
            "/api/declarations", params={"q": "open", "status": "sorry", "limit": 1}
        ).json()
        assert filtered["total"] == 1
        assert filtered["declarations"][0]["name"] == "Demo.open"
        assert "search" not in filtered["declarations"][0]
        assert client.get("/api/statements", params={"limit": "nope"}).status_code == 400


def test_web_cli_resolves_project_outside_checkout_and_defaults_to_loopback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _project(project)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)
    calls: list[tuple[Path, str, int]] = []

    def run_web(config: PipelineConfig, *, host: str, port: int) -> None:
        assert config.project is not None
        calls.append((config.project.root, host, port))

    monkeypatch.setattr(web_module, "run_web", run_web)
    assert main(["web", str(project)]) == 0
    assert main(["web", str(project), "--host", "0.0.0.0", "--port", "8080"]) == 0
    assert calls == [(project, "127.0.0.1", 5173), (project, "0.0.0.0", 8080)]


def test_web_command_wins_over_same_named_checkout_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _project(project)
    (project / "web").mkdir()
    monkeypatch.chdir(project)
    called = False

    def run_web(_config: PipelineConfig, *, host: str, port: int) -> None:
        nonlocal called
        called = True
        assert (host, port) == ("127.0.0.1", 5173)

    monkeypatch.setattr(web_module, "run_web", run_web)
    assert main(["web"]) == 0
    assert called


def test_app_factory_reports_missing_project_configuration(tmp_path: Path) -> None:
    with pytest.raises((OSError, ValueError)):
        web_module.create_app(tmp_path)


def test_run_web_rejects_invalid_port_before_starting_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _project(tmp_path)
    monkeypatch.setattr(web_module, "create_app", lambda _config: object())
    with pytest.raises(ValueError, match="--port"):
        web_module.run_web(config, host="127.0.0.1", port=0)
