# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for gating ingest-related features."""
from typing import Optional, Set

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

# states that are enabled in prod for primary
PRODUCTION_PRIMARY_ENABLED_STATES: Set[StateCode] = set()
# all states enabled in prod primary must also be enabled in prod secondary, plus
# the states we just want to be enabled in prod secondary
PRODUCTION_SECONDARY_ENABLED_STATES: Set[StateCode] = {
    *PRODUCTION_PRIMARY_ENABLED_STATES
}


# the states just want to be enabled in staging primary
# all states in any version of prod must able be enabled in staging primary, plus
STAGING_PRIMARY_ENABLED_STATES: Set[StateCode] = {
    StateCode.US_OZ,
    *PRODUCTION_SECONDARY_ENABLED_STATES,
}
# all states enabled in staging primary must be enabled in staging secondary, plus the
# states we just want to be enabled in staging secondary
STAGING_SECONDARY_ENABLED_STATES: Set[StateCode] = {
    StateCode.US_IA,
    StateCode.US_MO,
    StateCode.US_PA,
    *STAGING_PRIMARY_ENABLED_STATES,
}


# TODO(#28239): delete once raw data import DAG is live
def is_raw_data_import_dag_enabled(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
    project_id: Optional[str] = None,
) -> bool:

    if not project_id:
        project_id = metadata.project_id()

    if project_id == GCP_PROJECT_PRODUCTION:
        enabled_states_for_project_and_raw_data_instance = (
            PRODUCTION_PRIMARY_ENABLED_STATES
            if raw_data_instance == DirectIngestInstance.PRIMARY
            else PRODUCTION_SECONDARY_ENABLED_STATES
        )

        return state_code in enabled_states_for_project_and_raw_data_instance

    if project_id == GCP_PROJECT_STAGING:
        enabled_states_for_project_and_raw_data_instance = (
            STAGING_PRIMARY_ENABLED_STATES
            if raw_data_instance == DirectIngestInstance.PRIMARY
            else STAGING_SECONDARY_ENABLED_STATES
        )

        return state_code in enabled_states_for_project_and_raw_data_instance

    # returns false for testing envs
    return False
