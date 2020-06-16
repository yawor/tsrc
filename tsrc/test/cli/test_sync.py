from typing import Any
import os

from path import Path

import tsrc.cli
from tsrc.workspace.config import WorkspaceConfig

from cli_ui.tests import MessageRecorder
from tsrc.test.helpers.cli import CLI
from tsrc.test.helpers.git_server import GitServer


def test_sync_happy(tsrc_cli: CLI, git_server: GitServer, workspace_path: Path) -> None:
    """" Scenario:
    * Create a manifest with two repos (foo and bar)
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Run `tsrc sync`
    * Check that the foo clone has been updated
    """
    git_server.add_repo("foo")
    git_server.add_repo("bar")
    manifest_url = git_server.manifest_url
    tsrc_cli.run("init", manifest_url)
    git_server.push_file("foo", "new.txt", contents="new file")

    tsrc_cli.run("sync")

    new_txt_path = workspace_path / "foo/new.txt"
    assert new_txt_path.exists(), "foo should have been updated"


def test_sync_with_errors(
    tsrc_cli: CLI,
    git_server: GitServer,
    workspace_path: Path,
    message_recorder: MessageRecorder,
) -> None:
    """" Scenario:
    * Create a manifest with two repos (foo and bar)
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Create a merge conflict in the foo repo
    * Run `tsrc sync`
    * Check that it fails and contains the proper
      error message
    """
    git_server.add_repo("foo")
    git_server.add_repo("bar")
    manifest_url = git_server.manifest_url
    tsrc_cli.run("init", manifest_url)
    git_server.push_file("foo", "conflict.txt", contents="this is red")

    foo_src = workspace_path / "foo"
    (foo_src / "conflict.txt").write_text("this is green")

    tsrc_cli.run_and_fail("sync")

    assert message_recorder.find("Failed to synchronize workspace")
    assert message_recorder.find(r"\* foo")


def test_sync_finds_root(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path, monkeypatch: Any
) -> None:
    """ Check that you can run `tsrc sync` from inside a cloned
    repository

    """
    git_server.add_repo("foo/bar")
    tsrc_cli.run("init", git_server.manifest_url)
    monkeypatch.chdir(workspace_path / "foo/bar")
    tsrc_cli.run("sync")


def test_new_repo_added_to_manifest(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """
    Scenario:
    * Create a manifest containing the foo repo
    * Initialize a workspace from this manifest
    * Add a bar repo to the manifest
    * Run `tsrc sync`
    * Check that bar is cloned
    """
    git_server.add_repo("foo")
    manifest_url = git_server.manifest_url
    tsrc_cli.run("init", manifest_url)

    git_server.add_repo("bar")
    tsrc_cli.run("sync")

    assert (workspace_path / "bar").exists()


def change_workspace_manifest_branch(workspace_path: Path, branch: str) -> None:
    cfg_path = workspace_path / ".tsrc/config.yml"
    workspace_config = WorkspaceConfig.from_file(cfg_path)
    workspace_config.manifest_branch = branch
    workspace_config.save_to_file(cfg_path)


def test_switching_manifest_branch(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Initialize aw workspace with a manifest on the master branch
    * Create a new repo bar, on the 'devel' branch of the manifest
    * Run `tsrc sync`: bar should not get cloned
    * Configure the workspace to use the `devel` branch of the manifest
    * Run `tsrc sync` again
    * Check that bar is cloned
    """

    git_server.add_repo("foo")
    tsrc_cli.run("init", git_server.manifest_url)

    git_server.manifest.change_branch("devel")
    git_server.add_repo("bar")
    bar_path = workspace_path / "bar"

    tsrc_cli.run("sync")
    assert not bar_path.exists(), "bar should not have been cloned"

    change_workspace_manifest_branch(workspace_path, "devel")

    tsrc_cli.run("sync")
    assert bar_path.exists(), "bar should have been cloned"


def test_sync_not_on_master(
    tsrc_cli: CLI,
    git_server: GitServer,
    workspace_path: Path,
    message_recorder: MessageRecorder,
) -> None:
    """"
   Scenario:
   * Create a manifest with two repos, foo and bar
   * Initialize a workspace from this manifest
   * Checkout a different branch on foo, tracking an existing remote
   * Run `tsrc sync`
   * Check that:
      * foo is updated
      * but the command fails because foo was not an the expected branch
    """
    git_server.add_repo("foo")
    git_server.add_repo("bar")

    git_server.push_file("foo", "devel.txt", branch="devel")
    manifest_url = git_server.manifest_url
    tsrc_cli.run("init", manifest_url)

    foo_path = workspace_path / "foo"
    tsrc.git.run(foo_path, "checkout", "-B", "devel")
    tsrc.git.run(foo_path, "branch", "--set-upstream-to", "origin/devel")

    tsrc_cli.run_and_fail("sync")

    assert (foo_path / "devel.txt").exists(), "foo should have been updated"
    assert message_recorder.find("not on the correct branch")


def test_sync_with_force(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """
    Scenario:
    * Create a manifest with one repo, foo
    * Create a tag `latest` on foo
    * Initialize a workspace from this manifest
    * Delete and re-create the `latest` tag
    * Run tsrc sync --force
    * Check that the clone was reset to the correct revision
      (aka, `git fetch --force` was called).
    """
    git_server.add_repo("foo")
    git_server.push_file("foo", "latest.txt", contents="1")
    git_server.tag("foo", "latest")
    tsrc_cli.run("init", git_server.manifest_url)

    git_server.push_file("foo", "latest.txt", contents="2")
    git_server.tag("foo", "latest", force=True)
    tsrc_cli.run("sync", "--force")

    foo_path = workspace_path / "foo"
    assert (
        foo_path / "latest.txt"
    ).read_text() == "2", "foo should have been reset to the latest tag"


def test_copies_are_up_to_date(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """
    Scenario:
    * Create a manifest with one repo, foo
    * Configure a copy from foo/foo.txt to top.txt
    * Initialize a workspace from this manifest
    * Push a new version of `foo.txt` to the foo repo
    * Run `tsrc sync`
    * Check that `top.txt` has been updated

    """
    manifest_url = git_server.manifest_url
    git_server.add_repo("foo")
    git_server.push_file("foo", "foo.txt", contents="v1")
    git_server.manifest.set_file_copy("foo", "foo.txt", "top.txt")
    tsrc_cli.run("init", manifest_url)
    git_server.push_file("foo", "foo.txt", contents="v2")

    tsrc_cli.run("sync")

    assert (
        workspace_path / "top.txt"
    ).read_text() == "v2", "copy should have been updated"
    assert (workspace_path / "top.txt").read_text() == "v2"


def test_copies_preserve_stat(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """
    Scenario:
    * Create a manifest with one repo, foo
    * Push a foo.exe executable file in the foo repo
    * Configure a copy from foo/foo.exe to top.exe
    * Check that `top.exe` is executable

    """
    manifest_url = git_server.manifest_url
    git_server.add_repo("foo")
    git_server.push_file("foo", "foo.exe", contents="v1", executable=True)
    git_server.manifest.set_file_copy("foo", "foo.exe", "top.exe")

    tsrc_cli.run("init", manifest_url)
    top_exe = workspace_path / "top.exe"
    assert os.access(top_exe, os.X_OK)


def test_update_symlink(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Crate a manifest with a 'foo' repo
    * Push 'foo.txt' to the 'foo' repo
    * Configure the 'foo' repo with a symlink copy from 'foo.link' to 'foo/foo.txt'
    * Run `tsrc init`
    * Update the link in the manifest
    * Push 'bar.txt' to the 'foo' repo
    * Run `tsrc sync`
    * Update the 'foo' repo with a symlink from 'foo.link' to 'foo/bar.txt'
    * Check that the link in <workspace>/foo.link was updated to point to foo/bar.txt
    """
    manifest_url = git_server.manifest_url
    git_server.add_repo("foo")
    git_server.push_file("foo", "foo.txt")
    git_server.manifest.set_symlink("foo", "foo.link", "foo/foo.txt")
    tsrc_cli.run("init", manifest_url)
    git_server.push_file("foo", "bar.txt")
    git_server.manifest.set_symlink("foo", "foo.link", "foo/bar.txt")

    tsrc_cli.run("sync")

    actual_link = workspace_path / "foo.link"
    assert actual_link.exists()
    assert actual_link.readlink() == os.path.normpath("foo/bar.txt")


def test_changing_branch(
    tsrc_cli: CLI,
    git_server: GitServer,
    workspace_path: Path,
    message_recorder: MessageRecorder,
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo
    * Initialize a workspace from this manifest
    * Create a new branch named `next` on the foo repo
    * Update foo branch in the manifest
    * Run `tsrc sync`
    * Check that the command fails because `foo` is no
      longer on the expected branch
    """
    git_server.add_repo("foo")
    manifest_url = git_server.manifest_url
    tsrc_cli.run("init", manifest_url)

    git_server.push_file("foo", "next.txt", branch="next")
    git_server.manifest.set_repo_branch("foo", "next")

    tsrc_cli.run_and_fail("sync")
    assert message_recorder.find("not on the correct branch")


def test_tags_are_not_updated(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at the `v0.1` tag
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Run `tsrc sync`
    * Check that foo was not updated
    """
    git_server.add_repo("foo")
    git_server.tag("foo", "v0.1")
    git_server.manifest.set_repo_tag("foo", "v0.1")

    tsrc_cli.run("init", git_server.manifest_url)

    git_server.push_file("foo", "new.txt")

    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    assert not (
        foo_path / "new.txt"
    ).exists(), "foo should not have been updated (frozen at v0.1 tag)"


def test_sha1s_are_not_updated(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at a given revision
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Run `tsrc sync`
    * Check that foo was not updated
    """
    git_server.add_repo("foo")
    initial_sha1 = git_server.get_sha1("foo")
    git_server.manifest.set_repo_sha1("foo", initial_sha1)

    tsrc_cli.run("init", git_server.manifest_url)

    git_server.push_file("foo", "new.txt")

    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    assert not (
        foo_path / "new.txt"
    ).exists(), f"foo should not have been updated (frozen at {initial_sha1})"


def test_tags_are_updated_when_clean(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at the v0.1 tag
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Create a new v0.2 tag on the foo repo
    * Configure the manifest so that `foo` is frozen at the v0.2 tag
    * Run `tsrc sync`
    * Check that foo has been updated to the `v0.2` tag
    """
    git_server.add_repo("foo")
    git_server.tag("foo", "v0.1")
    git_server.manifest.set_repo_tag("foo", "v0.1")

    tsrc_cli.run("init", git_server.manifest_url)

    git_server.push_file("foo", "new.txt")
    git_server.tag("foo", "v0.2")
    git_server.manifest.set_repo_tag("foo", "v0.2")

    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    assert (
        foo_path / "new.txt"
    ).exists(), "foo should have been updated to the v0.2 tag"


def test_sha1s_are_updated_when_clean(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at an initial revision
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Configure the manifest so that `foo` is frozen at the new revision
    * Run `tsrc sync`
    * Check that foo has been updated to the new revision
    """
    git_server.add_repo("foo")
    initial_sha1 = git_server.get_sha1("foo")
    git_server.manifest.set_repo_sha1("foo", initial_sha1)

    tsrc_cli.run("init", git_server.manifest_url)

    git_server.push_file("foo", "new.txt")
    new_sha1 = git_server.get_sha1("foo")
    git_server.manifest.set_repo_sha1("foo", new_sha1)

    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    assert (
        foo_path / "new.txt"
    ).exists(), f"foo should have been updated to the {new_sha1} revision"


def test_tags_are_skipped_when_not_clean_tags(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at the v0.1 tag
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Create a new v0.2 tag on the foo repo
    * Configure the manifest so that foo is frozen at the v0.2 tag
    * Create an untracked file in the foo repo
    * Check that `tsrc sync` fails and that foo is not updated
    """
    git_server.add_repo("foo")
    git_server.tag("foo", "v0.1")
    git_server.manifest.set_repo_tag("foo", "v0.1")

    tsrc_cli.run("init", git_server.manifest_url)
    (workspace_path / "foo/untracked.txt").write_text("")

    git_server.push_file("foo", "new.txt")
    git_server.tag("foo", "v0.2")
    git_server.manifest.set_repo_tag("foo", "v0.2")

    tsrc_cli.run_and_fail("sync")

    foo_path = workspace_path / "foo"
    assert not (
        foo_path / "new.txt"
    ).exists(), "foo should not have been updated (untracked files)"


def test_sha1s_are_skipped_when_not_clean(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest with a foo repo, frozen at an initial revision
    * Initialize a workspace from this manifest
    * Push a new file to the foo repo
    * Configure the manifest so that foo is frozen at the new revision
    * Create an untracked file in the foo repo
    * Run `tsrc sync`
    * Check that `tsrc sync` fails and that foo is not updated
    """
    git_server.add_repo("foo")
    initial_sha1 = git_server.get_sha1("foo")
    git_server.manifest.set_repo_sha1("foo", initial_sha1)

    tsrc_cli.run("init", git_server.manifest_url)
    (workspace_path / "foo/untracked.txt").write_text("")

    git_server.push_file("foo", "new.txt")
    new_sha1 = git_server.get_sha1("foo")
    git_server.manifest.set_repo_sha1("foo", new_sha1)

    tsrc_cli.run_and_fail("sync")

    foo_path = workspace_path / "foo"
    assert not (
        foo_path / "new.txt"
    ).exists(), "foo should not have been updated (untracked files)"


def test_sync_uses_group_from_config_by_default(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest containing:
      * a group named 'foo'  containing the repos 'bar'  and 'baz'
      * a repo named 'other' not in any group
    * Initialize a workspace from this manifest using the `foo` group
    * Check that bar and baz are cloned
    * Check that `other` is not cloned
    """
    git_server.add_group("foo", ["bar", "baz"])
    git_server.add_repo("other")

    tsrc_cli.run("init", git_server.manifest_url, "--group", "foo")

    tsrc_cli.run("sync")

    assert (
        workspace_path / "bar"
    ).exists(), "bar should have been cloned (in foo group)"
    assert (
        workspace_path / "baz"
    ).exists(), "baz should have been cloned (in foo group)"
    assert not (
        workspace_path / "other"
    ).exists(), "other should not have been cloned (not in foo group)"


def test_fetch_additional_remotes(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest containing a foo repo with two remotes
    * Initialize a workspace from this manifest
    * Update the second remote
    * Run `tsrc sync`
    * Check that the second remote was fetched
    """
    git_server.add_repo("foo")
    foo2_url = git_server.add_repo("foo2")
    git_server.manifest.set_repo_remotes("foo", [("other", foo2_url)])

    tsrc_cli.run("init", git_server.manifest_url)
    foo_path = workspace_path / "foo"
    first_sha1 = tsrc.git.get_sha1(foo_path, ref="other/master")
    git_server.push_file("foo2", "new.txt")

    tsrc_cli.run("sync")
    second_sha1 = tsrc.git.get_sha1(foo_path, ref="other/master")

    assert first_sha1 != second_sha1, "remote 'other' was not fetched"


def test_adding_remotes(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest containing a foo repo with one remote
    * Initialize a workspace from this manifest
    * Add an 'other' remote to the foo repo in the manifest
    * Run `tsrc sync`
    * Check that the second remote was fetched
    """
    git_server.add_repo("foo")
    tsrc_cli.run("init", git_server.manifest_url)
    foo2_url = git_server.add_repo("foo2")
    git_server.manifest.set_repo_remotes("foo", [("other", foo2_url)])

    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    assert tsrc.git.get_sha1(
        foo_path, ref="other/master"
    ), "remote 'other' was not added"


def test_changing_remote_url(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    * Create a manifest containing a foo repo with one remote
    * Initialize a workspace from this manifest
    * Change the url in the manifest
    * Run `tsrc sync`
    * Check that the remote was updated
    """
    git_server.add_repo("foo")
    tsrc_cli.run("init", git_server.manifest_url)

    foo2_url = git_server.add_repo("foo2")
    git_server.manifest.set_repo_url("foo", foo2_url)
    tsrc_cli.run("sync")

    foo_path = workspace_path / "foo"
    _, actual_url = tsrc.git.run_captured(foo_path, "remote", "get-url", "origin")
    assert actual_url == foo2_url, "remote was not updated"


def test_sync_with_singular_remote(
    tsrc_cli: CLI, git_server: GitServer, workspace_path: Path
) -> None:
    """ Scenario:
    Scenario:
     * Create a manifest that contains one repo with two remotes
       ('origin' and 'vpn')
     * Marke sure that the 'origin' URL is valid but the 'vpn'
       URL is not.
     * Run 'tsrc init -r origin'
     * Check that 'tsrc sync' does not try and fetch the 'vpn' remote
    """
    foo_url = git_server.add_repo("foo")
    vpn_url = "/does/not/exist"
    # fmt: off
    git_server.manifest.set_repo_remotes(
        "foo",
        [("origin", foo_url),
         ("vpn", vpn_url)])
    # fmt: on
    tsrc_cli.run("init", git_server.manifest_url, "-r", "origin")

    tsrc_cli.run("sync")
