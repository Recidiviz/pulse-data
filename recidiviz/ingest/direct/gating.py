#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Used for gating various ingest-related features."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment


# TODO(#20997): delete once ingest is enabled in dataflow in all states
def is_ingest_in_dataflow_enabled(
    state_code: StateCode,  # pylint: disable=unused-argument
    instance: DirectIngestInstance,  # pylint: disable=unused-argument
) -> bool:
    return False


def ingest_pipeline_can_run_in_dag(
    state_code: StateCode,  # pylint: disable=unused-argument
    instance: DirectIngestInstance,  # pylint: disable=unused-argument
) -> bool:
    if environment.in_gcp_production():
        return False
    staging_enabled_states = [
        StateCode.US_OZ,
    ]
    return state_code in staging_enabled_states
