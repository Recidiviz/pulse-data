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
"""Shared utils to support the Workflows view queries"""

from recidiviz.utils import secrets
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_ci
from recidiviz.utils.metadata import local_project_id_override

_WORKFLOWS_SALT_KEY = "workflows_salt_key"


def get_workflows_salt_value() -> str:
    """Returns the value for the Workflows salt key from the secrets manager."""
    with local_project_id_override(GCP_PROJECT_STAGING):
        if in_ci():
            return "NOT A SECRET"
        workflows_salt_key_value = secrets.get_secret(_WORKFLOWS_SALT_KEY)
        if not workflows_salt_key_value:
            raise ValueError(
                f"Could not get secret value for `Workflows salt key`. Provided with "
                f"key={_WORKFLOWS_SALT_KEY}"
            )
        return workflows_salt_key_value
