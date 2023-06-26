# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Constants used by the asset-generation-service integration."""
from recidiviz.reporting.email_reporting_utils import get_env_var
from recidiviz.utils.environment import in_development, in_gcp


def _get_base_url() -> str:
    try:
        return get_env_var("ASSET_GENERATION_URL")
    except KeyError:
        if in_gcp() or in_development():
            raise

        return "http://asset-generation-test"


ASSET_GENERATION_SERVICE_URI = _get_base_url()

GENERATE_API_BASE = f"{_get_base_url()}/generate"

# for local development we don't want an internal docker address,
# because we don't request the files within a docker environment
RETRIEVE_API_BASE = (
    "http://localhost:5174" if in_development() else ASSET_GENERATION_SERVICE_URI
)
