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
Class to manage Git operations for the Looker repository.
"""
import logging
import os
from pathlib import Path
from typing import Optional

import attr

from recidiviz.common import attr_validators
from recidiviz.tools.utils.script_helpers import run_command
from recidiviz.utils.github import LOOKER_REPO_NAME


@attr.define
class LookerGitManager:
    """Handles Git operations for the Looker repository."""

    looker_branch_name: str = attr.ib(
        validator=attr_validators.is_str,
    )
    github_token: str = attr.ib(
        validator=attr_validators.is_str,
    )

    # The Path to the looker repo that this LookerGitManager will operate on. Must call
    # clone_and_checkout_branch() first for this to be set.
    _looker_repo_root: Path | None = attr.ib(
        default=None, validator=attr_validators.is_opt(Path), init=False
    )

    @property
    def looker_repo_root(self) -> Path:
        if not self._looker_repo_root:
            raise ValueError(
                "Must first set self.looker_repo_root (i.e. call "
                "clone_and_checkout_branch())"
            )
        return self._looker_repo_root

    def clone_and_checkout_branch(self) -> None:
        """Clone the Looker repository and checkout a new branch."""

        looker_repo_url = (
            f"https://{self.github_token}@github.com/{LOOKER_REPO_NAME}.git"
        )

        self._looker_repo_root = Path("__TEMP_LOOKER_REPO_DIR__").resolve()

        if os.path.exists(self.looker_repo_root):
            raise ValueError(
                f"Cannot clone {LOOKER_REPO_NAME} into {self.looker_repo_root} - "
                f"path already exists."
            )

        run_command(f"git clone {looker_repo_url} {self.looker_repo_root}")

        if self.remote_branch_exists(self.looker_branch_name):
            logging.info(
                "Checking out branch [%s] which already exists on remote.",
                self.looker_branch_name,
            )
            cmd = f"git checkout {self.looker_branch_name}"
        else:
            logging.info("Creating new branch [%s].", self.looker_branch_name)
            cmd = f"git checkout -b {self.looker_branch_name}"

        self._run_in_looker_repo(cmd)

        head_commit = self._run_in_looker_repo("git rev-parse HEAD")

        logging.info(
            "Successfully cloned the Looker repo and checked out branch "
            "[%s] at hash [%s].",
            self.looker_branch_name,
            head_commit,
        )

    def _run_in_looker_repo(self, cmd: str) -> Optional[str]:
        """Execute a command in the Looker repository directory."""
        return run_command(command=cmd, cwd=self.looker_repo_root)

    def has_changes(self) -> bool:
        """Check if there are any changes in the repository."""
        changes = self._run_in_looker_repo("git status --porcelain")
        return bool(changes)

    def remote_branch_exists(self, branch_name: str) -> bool:
        """Check if the remote branch already exists."""
        remote_branch = self._run_in_looker_repo(
            f"git ls-remote --heads origin {branch_name}"
        )
        return bool(remote_branch)

    def commit_and_push_changes(self, commit_msg: str) -> None:
        """Commit and push changes to the remote repository."""
        self._run_in_looker_repo("git add .")
        self._run_in_looker_repo(f'git commit -m "{commit_msg}"')

        if self.remote_branch_exists(self.looker_branch_name):
            cmd = f"git push origin {self.looker_branch_name}"
        else:
            cmd = f"git push --set-upstream origin {self.looker_branch_name}"

        self._run_in_looker_repo(cmd)
