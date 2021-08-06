from pathlib import Path
from typing import List, Optional

import attr
import cli_ui as ui

from tsrc.errors import Error
from tsrc.executor import Outcome, Task
from tsrc.git import get_current_branch, get_git_status, run_git_captured
from tsrc.repo import Remote, Repo


class IncorrectBranch(Error):
    def __init__(self, *, actual: str, expected: str):
        self.message = (
            f"Current branch: '{actual}' does not match expected branch: '{expected}'"
        )


@attr.s(frozen=True)
class RepoAtIncorrectBranchDescription:
    dest: str = attr.ib()
    actual: str = attr.ib()
    expected: str = attr.ib()


class Syncer(Task[Repo]):
    def __init__(
        self,
        workspace_path: Path,
        *,
        force: bool = False,
        remote_name: Optional[str] = None,
    ) -> None:
        self.workspace_path = workspace_path
        self.bad_branches: List[RepoAtIncorrectBranchDescription] = []
        self.force = force
        self.remote_name = remote_name

    def describe_item(self, item: Repo) -> str:
        return item.dest

    def describe_process_start(self, item: Repo) -> List[ui.Token]:
        return ["Syncing", item.dest]

    def describe_process_end(self, item: Repo) -> List[ui.Token]:
        return [ui.green, "ok", ui.reset, item.dest]

    def process(self, index: int, count: int, repo: Repo) -> Outcome:
        """Synchronize a repo given its configuration in the manifest.

        Always start by running `git fetch`, then either:

        * try resetting the repo to the given tag or sha1 (abort
          if the repo is dirty)

        * or try merging the local branch with its upstream (abort if not
          on on the correct branch, or if the merge is not fast-forward).
        """
        outcome = Outcome.empty()
        summary_lines = []
        self.info_count(index, count, "Synchronizing", repo.dest)
        self.fetch(repo)
        ref = None

        if repo.tag:
            ref = repo.tag
        elif repo.sha1:
            ref = repo.sha1

        if ref:
            self.info_3("Resetting to", ref)
            self.sync_repo_to_ref(repo, ref)
            summary_lines.append(f"Reset to {ref}")
        else:
            self.info_3("Updating branch")
            error = self.check_branch(repo)
            if error:
                outcome.error = error
            sync_summary = self.sync_repo_to_branch(repo)
            if sync_summary:
                summary_lines += sync_summary

        self.update_submodules(repo)
        outcome.summary = "\n".join(summary_lines)
        return outcome

    def check_branch(self, repo: Repo) -> Optional[Error]:
        repo_path = self.workspace_path / repo.dest
        current_branch = None
        try:
            current_branch = get_current_branch(repo_path)
        except Error:
            raise Error("Not on any branch")

        if current_branch and current_branch != repo.branch:
            return IncorrectBranch(actual=current_branch, expected=repo.branch)
        else:
            return None

    def _pick_remotes(self, repo: Repo) -> List[Remote]:
        if self.remote_name:
            for remote in repo.remotes:
                if remote.name == self.remote_name:
                    return [remote]
            message = f"Remote {self.remote_name} not found for repository {repo.dest}"
            raise Error(message)

        return repo.remotes

    def fetch(self, repo: Repo) -> None:
        repo_path = self.workspace_path / repo.dest
        for remote in self._pick_remotes(repo):
            try:
                self.info_3("Fetching", remote.name)
                cmd = ["fetch", "--tags", "--prune", remote.name]
                if self.force:
                    cmd.append("--force")
                self.run_git(repo_path, *cmd)
            except Error:
                raise Error(f"fetch from '{remote.name}' failed")

    def sync_repo_to_ref(self, repo: Repo, ref: str) -> None:
        repo_path = self.workspace_path / repo.dest
        status = get_git_status(repo_path)
        if status.dirty:
            raise Error(f"{repo_path} is dirty, skipping")
        try:
            self.run_git(repo_path, "reset", "--hard", ref)
        except Error:
            raise Error("updating ref failed")

    def update_submodules(self, repo: Repo) -> None:
        repo_path = self.workspace_path / repo.dest
        self.run_git(repo_path, "submodule", "update", "--init", "--recursive")

    def sync_repo_to_branch(self, repo: Repo) -> List[str]:
        repo_path = self.workspace_path / repo.dest
        if self.parallel:
            rc, out = run_git_captured(
                repo_path, "log", "--oneline", "HEAD..@{upstream}", check=False
            )
            if rc == 0 and not out:
                return []
            _, out = run_git_captured(
                repo_path, "merge", "--ff-only", "@{upstream}", check=True
            )
            return [repo.dest, "-" * len(repo.dest), out]
        else:
            try:
                self.run_git(repo_path, "merge", "--ff-only", "@{upstream}")
            except Error:
                raise Error("updating branch failed")
            return []
