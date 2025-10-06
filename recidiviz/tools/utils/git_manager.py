# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Git repository manager for handling common git operations.
"""
import logging
import os
from pathlib import Path

import attr

from recidiviz.tools.utils.script_helpers import run_command


@attr.define
class GitManager:
    """Git repository manager for handling common git operations in an existing local repo."""

    repo_root: Path = attr.ib(validator=attr.validators.instance_of(Path))

    def __attrs_post_init__(self) -> None:
        if not self.repo_root.exists():
            raise ValueError(f"repo_root path [{self.repo_root}] does not exist.")

        if (
            not (self.repo_root / ".git").exists()
            or self._run_git_command("git rev-parse --is-inside-work-tree") != "true"
        ):
            raise ValueError(
                f"repo_root path [{self.repo_root}] is not a valid git repository."
            )

    @classmethod
    def clone_repo_and_create_manager(
        cls, github_token: str, repo_name: str, repo_root: Path
    ) -> "GitManager":
        """Clone the given repository and return a GitManager for it.

        Args:
            github_token: GitHub token for authentication
            repo_name: Name of the repository (e.g., "Recidiviz/looker")
            repo_root: Local path to clone the repository into

        Returns:
            GitManager: An instance of GitManager for the cloned repository.
        """
        if os.path.exists(repo_root):
            raise ValueError(
                f"Cannot clone repository into {repo_root} - path already exists."
            )
        repo_url = f"https://{github_token}@github.com/{repo_name}.git"

        run_command(f"git clone {repo_url} {repo_root}")

        return GitManager(repo_root=repo_root)

    def _delete_remote_branch(self, branch_name: str) -> None:
        """Delete the remote branch with the name |branch_name|."""
        if not self.remote_branch_exists(branch_name):
            raise ValueError(
                f"Cannot delete remote branch [{branch_name}] - "
                "branch does not exist."
            )
        self._run_git_command(f"git push origin --delete {branch_name}")

    def _delete_local_branch(self, branch_name: str) -> None:
        """Delete the local branch with the name |branch_name|."""
        self._run_git_command(f"git branch -D {branch_name}")

    def checkout_branch(self, branch_name: str) -> None:
        """Checkout the given branch name."""
        self._run_git_command(f"git checkout {branch_name}")

    def _create_branch(self, new_branch_name: str, base_branch: str) -> None:
        """Create a new branch from the base branch."""
        self._run_git_command(f"git checkout -b {new_branch_name} {base_branch}")

    def delete_branch_if_exists(self, branch_name: str) -> None:
        """Delete the given branch name if it exists (both local and remote)."""
        if self.remote_branch_exists(branch_name):
            logging.info("Deleting remote branch [%s].", branch_name)
            self._delete_remote_branch(branch_name)

        local_branches_output = self._run_git_command("git branch")
        local_branches = {
            b.strip().lstrip("* ") for b in local_branches_output.splitlines()
        }

        if branch_name in local_branches:
            logging.info("Deleting local branch [%s].", branch_name)
            self._delete_local_branch(branch_name)

    def create_branch_if_not_exists(
        self, base_branch_name: str, new_branch_name: str
    ) -> None:
        """Create a new branch from the base branch if it does not exist."""
        if self.remote_branch_exists(new_branch_name):
            logging.info("Branch [%s] already exists on remote.", new_branch_name)
            self.checkout_branch(new_branch_name)
        else:
            logging.info(
                "Creating new branch [%s] from [%s].", new_branch_name, base_branch_name
            )
            self._create_branch(new_branch_name, base_branch_name)

        head_commit = self._run_git_command("git rev-parse HEAD")

        logging.info(
            "Successfully checked out branch [%s] at hash [%s].",
            new_branch_name,
            head_commit,
        )

    def has_uncommitted_changes(self) -> bool:
        """Check if there are any uncommitted changes in the local repo."""
        changes = self._run_git_command("git status --porcelain")
        return bool(changes)

    def remote_branch_exists(self, branch_name: str) -> bool:
        """Check if the remote branch already exists."""
        remote_branch = self._run_git_command(
            f"git ls-remote --heads origin {branch_name}"
        )
        return bool(remote_branch)

    def _get_current_branch(self) -> str:
        """Get the name of the currently checked out branch."""
        return self._run_git_command("git rev-parse --abbrev-ref HEAD")

    def _stage_all_changes(self) -> None:
        """Stage all changes for commit."""
        self._run_git_command("git add .")

    def _commit_staged_changes(self, commit_msg: str) -> None:
        """Commit staged changes with the given message."""
        self._run_git_command(f'git commit -m "{commit_msg}"')

    def _push_branch(self, branch_name: str) -> None:
        """Push the given branch to the remote repository, creating it if it doesn't exist."""
        if self.remote_branch_exists(branch_name):
            cmd = f"git push origin {branch_name}"
        else:
            cmd = f"git push --set-upstream origin {branch_name}"

        self._run_git_command(cmd)

    def commit_and_push_all_changes(self, commit_msg: str) -> None:
        """Commit and push all local changes to the remote repository."""
        self._stage_all_changes()
        self._commit_staged_changes(commit_msg)
        self._push_branch(branch_name=self._get_current_branch())

    def _run_git_command(self, cmd: str) -> str:
        """Execute a git command in the repository directory."""
        return run_command(command=cmd, cwd=self.repo_root).strip()
