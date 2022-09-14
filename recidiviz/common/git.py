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
from recidiviz.tools.utils.script_helpers import run_command


def get_hash_of_deployed_commit(project_id: str) -> str:
    """Returns the commit hash of the currently deployed version in the provided
    project.
    """

    # First make sure all tags are current locally
    run_command("git fetch --all --tags --prune --prune-tags", timeout_sec=30)

    get_tag_cmd = (
        f"gcloud app versions list --project={project_id} --hide-no-traffic "
        f"--service=default --format=yaml | yq .id | tr -d \\\" | tr '-' '.'"
        ' | sed "s/.alpha/-alpha/"'
    )

    get_commit_cmd = f"git rev-list -n 1 $({get_tag_cmd})"
    return run_command(get_commit_cmd, timeout_sec=30).strip()


def is_commit_in_current_branch(commit_hash: str) -> bool:
    result = run_command(
        f"git branch $(git symbolic-ref --short HEAD) --contains {commit_hash}",
        timeout_sec=30,
    )
    return bool(result)
