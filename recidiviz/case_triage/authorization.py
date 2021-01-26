# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
This module contains various pieces related to the Case Triage authentication / authorization flow
"""

import json
from typing import List

from recidiviz.case_triage.util import get_local_file
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.environment import in_development
from recidiviz.utils.metadata import project_id


class AuthorizationStore:
    """
    Simple store that fetches the allowlist of early Case Triage users' email addresses.
    The allowlist is manually managed by Recidiviz employees.
    """

    def __init__(self) -> None:
        prefix = "" if in_development() else f"{project_id()}-"
        self.allowlist_path = GcsfsFilePath.from_absolute_path(f'{prefix}case-triage-data/allowlist.json')
        self.allowed_users: List[str] = []

    def refresh(self) -> None:
        self.allowed_users = json.loads(get_local_file(self.allowlist_path))


class CaseTriageAuthorizationError(Exception):
    """ Exception for when the a user has signed into a valid account, but has not yet been allowed access."""

    def __init__(self, error: dict, status_code: int) -> None:
        self.error = error
        self.status_code = status_code
        super().__init__()
