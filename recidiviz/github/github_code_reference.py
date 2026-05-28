# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""GithubCodeReference data class for referencing code locations in a GitHub
repository."""

import attr


@attr.s(frozen=True, kw_only=True)
class GithubCodeReference:
    """A reference to a specific line of code in a GitHub repository."""

    # e.g. "Recidiviz/pulse-data"
    repo: str = attr.ib()
    # Repo-root-relative path, e.g. "recidiviz/utils/github.py"
    filepath: str = attr.ib()
    line_number: int = attr.ib()
    # Full text of the line at line_number
    line_text: str = attr.ib()

    def get_github_url(self, commit_sha: str) -> str:
        return (
            f"https://github.com/{self.repo}/blob/"
            f"{commit_sha}/{self.filepath}#L{self.line_number}"
        )

    def __str__(self) -> str:
        return f"{self.filepath}:{self.line_number}"
