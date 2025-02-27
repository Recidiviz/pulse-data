# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines a number of git-related helper functions."""
import re

from recidiviz.tools.utils.script_helpers import run_command


def get_hash_of_data_platform_version(data_platform_version: str) -> str:
    """Returns the git commit hash of the data platform version with the given tag."""

    if not re.match(
        r"^v[0-9]+\.[0-9]+\.[0-9]+(-alpha\.[0-9]+)?$", data_platform_version
    ):
        raise ValueError(
            f"Data platform version [{data_platform_version}] does not match expected "
            f"version regex."
        )

    # First make sure all tags are current locally
    run_command("git fetch --all --tags --prune --prune-tags --force", timeout_sec=30)

    get_commit_cmd = f"git rev-list -n 1 {data_platform_version}"
    return run_command(get_commit_cmd, timeout_sec=30).strip()


def is_commit_in_current_branch(commit_hash: str) -> bool:
    result = run_command(
        f"git branch $(git symbolic-ref --short HEAD) --contains {commit_hash}",
        timeout_sec=30,
    )
    return bool(result)
