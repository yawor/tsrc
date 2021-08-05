from pathlib import Path
from typing import List, Optional

import cli_ui as ui

from tsrc.executor import Task
from tsrc.git import run_git, run_git_captured
from tsrc.repo import Remote, Repo


class RemoteSetter(Task[Repo]):
    """
    For each repository:

      * look for the remote configured in the manifest,
      * add any missing remote,
      * if a remote is found but with an incorrect URL, update its URL.

    """

    def __init__(self, workspace_path: Path) -> None:
        self.workspace_path = workspace_path
        self.summary: List[ui.Token] = []

    def describe_item(self, item: Repo) -> str:
        return item.dest

    def describe_process_start(self, item: Repo) -> List[ui.Token]:
        return ["Configuring remotes", item.dest]

    def describe_process_end(self, item: Repo) -> List[ui.Token]:
        return [ui.green, "ok", ui.reset, item.dest]

    def process(self, index: int, count: int, repo: Repo) -> List[ui.Token]:
        for remote in repo.remotes:
            existing_remote = self.get_remote(repo, remote.name)
            if existing_remote:
                if existing_remote.url != remote.url:
                    self.set_remote(repo, remote)
            else:
                self.add_remote(repo, remote)
        return self.summary

    def get_remote(self, repo: Repo, name: str) -> Optional[Remote]:
        full_path = self.workspace_path / repo.dest
        rc, url = run_git_captured(full_path, "remote", "get-url", name, check=False)
        if rc != 0:
            return None
        else:
            return Remote(name=name, url=url)

    def set_remote(self, repo: Repo, remote: Remote) -> None:
        full_path = self.workspace_path / repo.dest
        # fmt: off
        message = (
            repo.dest + ":", "Update remote", ui.reset,
            ui.bold, remote.name, ui.reset,
            "to new url:", ui.brown, f"({remote.url})"
        )
        # fmt: on
        self.info_3(*message)
        self.summary += [*message]
        run_git(full_path, "remote", "set-url", remote.name, remote.url)

    def add_remote(self, repo: Repo, remote: Remote) -> None:
        full_path = self.workspace_path / repo.dest
        # fmt: off
        message = (
            repo.dest + ":", "Add remote", ui.reset,
            ui.bold, remote.name, ui.reset, ui.brown, f"({remote.url})"
        )
        # fmt: on
        self.info_3(*message)
        if self.summary and self.summary[:-1] != "\n":
            self.summary += ["\n"]
        self.summary += [*message]
        run_git(full_path, "remote", "add", remote.name, remote.url)
