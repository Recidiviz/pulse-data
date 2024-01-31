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
    state_code: StateCode,
    instance: DirectIngestInstance,  # pylint: disable=unused-argument
) -> bool:
    staging_only_states = [
        StateCode.US_IX,
        StateCode.US_TN,
    ]
    if state_code not in staging_only_states:
        return True
    return not environment.in_gcp_production()
