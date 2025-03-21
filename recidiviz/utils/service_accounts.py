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
"""Helpers for getting information about Google Compute Engine service accounts."""
from googleapiclient import discovery

from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type

_compute_engine_service_accounts_cache: dict[str, str] = {}


def get_default_compute_engine_service_account_email(
    project_id: str | None = None,
) -> str:
    """Returns the email of the default Compute Engine service account email for the
    current project.
    """
    project_id = project_id or metadata.project_id()

    if project_id not in _compute_engine_service_accounts_cache:
        # Fetch compute engine information for this project
        request = discovery.build("compute", "v1").projects().get(project=project_id)
        response = request.execute()

        _compute_engine_service_accounts_cache[project_id] = assert_type(
            response["defaultServiceAccount"], str
        )

    return _compute_engine_service_accounts_cache[project_id]
