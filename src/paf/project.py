from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import Path

from paf.package_model import normalize_repository_path


def _absolute(path: str | Path, *, base: Path) -> Path:
    value = Path(path).expanduser()
    return value.resolve() if value.is_absolute() else (base / value).resolve()


def _search_start(path: Path) -> Path:
    return path if path.is_dir() else path.parent


def _nearest(start: Path, name: str) -> Path | None:
    for candidate in (start, *start.parents):
        match = candidate / name
        if match.is_file():
            return match.resolve()
    return None


def _git_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate.resolve()
    return None


@dataclass(frozen=True)
class Project:
    """All filesystem locations associated with one PAF project.

    ``root`` and every populated path are absolute.  Source document and target
    mappings stored in pipeline configuration remain repository-relative for
    portability; this object is the sole place where they become filesystem
    paths.
    """

    root: Path
    config_path: Path | None = None
    source_paths: tuple[Path, ...] = ()
    target_dir: Path | None = None
    state_dir: Path | None = None

    @property
    def repository(self) -> Path:
        return self.root

    @property
    def repo(self) -> Path:
        """Compatibility spelling used by the existing swarm settings."""

        return self.root

    @property
    def sources(self) -> tuple[Path, ...]:
        return self.source_paths

    @property
    def target_root(self) -> Path | None:
        return self.target_dir

    def resolve(self, value: str | Path, *, base: Path | None = None) -> Path:
        return _absolute(value, base=base or self.root)

    def repository_path(self, value: str | Path = ".", *, base: Path | None = None) -> Path:
        """Resolve the configured repository, retaining config-relative legacy behavior."""

        return self.resolve(value, base=base)

    def source_path(self, value: str | Path) -> Path:
        return self.resolve(value)

    def target_path(self, value: str | Path) -> Path:
        return self.resolve(value)

    def state_path(self, value: str | Path = ".paf") -> Path:
        return self.resolve(value)

    def canonical_repository_path(self, value: str | Path) -> str:
        """Resolve a writable path to its canonical, portable reservation key."""

        resolved = self.resolve(value)
        try:
            relative = resolved.relative_to(self.root)
        except ValueError as error:
            raise ValueError(f"path is outside project repository: {value}") from error
        return normalize_repository_path(relative)

    def bind(
        self,
        *,
        root: Path | None = None,
        config_path: Path | None = None,
        source_paths: Iterable[Path] | None = None,
        target_dir: Path | None = None,
        state_dir: Path | None = None,
    ) -> Project:
        resolved_root = (root or self.root).resolve()
        return replace(
            self,
            root=resolved_root,
            config_path=(config_path.resolve() if config_path is not None else self.config_path),
            source_paths=(
                tuple(path.resolve() for path in source_paths)
                if source_paths is not None
                else self.source_paths
            ),
            target_dir=target_dir.resolve() if target_dir is not None else self.target_dir,
            state_dir=state_dir.resolve() if state_dir is not None else self.state_dir,
        )


class ProjectResolver:
    """Resolve a project without relying on the PAF installation location."""

    def __init__(self, cwd: str | Path | None = None) -> None:
        self.cwd = Path.cwd().resolve() if cwd is None else Path(cwd).resolve()

    def resolve(
        self,
        *,
        project: str | Path | None = None,
        explicit_project: str | Path | None = None,
        targets: Iterable[str | Path] = (),
        target: str | Path | None = None,
        config: str | Path | None = None,
    ) -> Project:
        if project is not None and explicit_project is not None:
            raise ValueError("pass only one explicit project path")
        project = project if project is not None else explicit_project
        if target is not None:
            targets = (*targets, target)
        target_paths = tuple(_absolute(item, base=self.cwd) for item in targets)
        explicit_config = _absolute(config, base=self.cwd) if config is not None else None

        if project is not None:
            selected = _absolute(project, base=self.cwd)
            if selected.is_file():
                if selected.name != "paf.toml":
                    raise ValueError("--project must name a directory or paf.toml")
                root = selected.parent
                discovered_config = selected
            else:
                root = selected
                discovered_config = (root / "paf.toml").resolve()
                if not discovered_config.is_file():
                    discovered_config = None
        elif target_paths:
            start = _search_start(target_paths[0])
            discovered_config = _nearest(start, "paf.toml")
            root = (
                discovered_config.parent
                if discovered_config is not None
                else (_git_root(start) or start).resolve()
            )
        elif explicit_config is not None:
            root = explicit_config.parent
            discovered_config = explicit_config
        else:
            discovered_config = _nearest(self.cwd, "paf.toml")
            root = discovered_config.parent if discovered_config is not None else self.cwd

        return Project(
            root=root.resolve(),
            config_path=explicit_config or discovered_config,
            source_paths=target_paths,
            state_dir=(root / ".paf").resolve(),
        )
