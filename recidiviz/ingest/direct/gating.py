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
PRODUCTION_PRIMARY_ENABLED_STATES: Set[StateCode] = {
    StateCode.US_AR,
    StateCode.US_CA,
    StateCode.US_CO,
    StateCode.US_ID,
    StateCode.US_IX,
    StateCode.US_MI,
    StateCode.US_MO,
    StateCode.US_ND,
    StateCode.US_PA,
    StateCode.US_TN,
    # states we "rolled" out to prod that aren't enabled in prod but we un-gate them
    # so if/when they are they will use the new raw data infra
    StateCode.US_OZ,
    StateCode.US_MA,
    StateCode.US_NE,
    StateCode.US_IA,
}
# all states enabled in prod primary must also be enabled in prod secondary, plus
# the states we just want to be enabled in prod secondary
PRODUCTION_SECONDARY_ENABLED_STATES: Set[StateCode] = {
    *PRODUCTION_PRIMARY_ENABLED_STATES,
}


# the states just want to be enabled in staging primary
# all states in any version of prod must able be enabled in staging primary, plus
STAGING_PRIMARY_ENABLED_STATES: Set[StateCode] = {
    StateCode.US_ME,
    StateCode.US_OR,
    StateCode.US_TX,
    *PRODUCTION_SECONDARY_ENABLED_STATES,
}
# all states enabled in staging primary must be enabled in staging secondary, plus the
# states we just want to be enabled in staging secondary
STAGING_SECONDARY_ENABLED_STATES: Set[StateCode] = {
    StateCode.US_AZ,
    *STAGING_PRIMARY_ENABLED_STATES,
}

ALL_STATES_WITH_GATING: Set[StateCode] = STAGING_SECONDARY_ENABLED_STATES


# TODO(#28239): delete once raw data import DAG is live
def is_raw_data_import_dag_enabled(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
    project_id: Optional[str] = None,
) -> bool:
    """Gating logic for raw data import DAG. By default, all new states will be enabled
    everywhere where ingest is enabled; otherwise, it is only enabled where we enumerate
    it to be.
    """

    if not project_id:
        project_id = metadata.project_id()

    if project_id == GCP_PROJECT_PRODUCTION:
        enabled_states_for_project_and_raw_data_instance = (
            PRODUCTION_PRIMARY_ENABLED_STATES
            if raw_data_instance == DirectIngestInstance.PRIMARY
            else PRODUCTION_SECONDARY_ENABLED_STATES
        )

        return (
            state_code in enabled_states_for_project_and_raw_data_instance
            or state_code not in ALL_STATES_WITH_GATING
        )

    if project_id == GCP_PROJECT_STAGING:
        enabled_states_for_project_and_raw_data_instance = (
            STAGING_PRIMARY_ENABLED_STATES
            if raw_data_instance == DirectIngestInstance.PRIMARY
            else STAGING_SECONDARY_ENABLED_STATES
        )

        return (
            state_code in enabled_states_for_project_and_raw_data_instance
            or state_code not in ALL_STATES_WITH_GATING
        )

    # returns false for testing envs
    return False
