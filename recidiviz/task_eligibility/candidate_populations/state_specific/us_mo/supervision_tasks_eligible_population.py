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
"""Identifies when a client is a candidate for supervision Tasks in MO under routine
contact standards we support in our Tasks tool.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_MO_SUPERVISION_TASKS_ELIGIBLE_POPULATION"

# TODO(#50537): Update/refine candidate population to ensure it's correct.
# TODO(#50537): Exclude clients residing in Community Supervision Centers (CSCs).
_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
FROM `{project_id}.sessions.compartment_sub_sessions_materialized`
WHERE state_code = 'US_MO'
    AND compartment_level_1 = 'SUPERVISION'
    AND compartment_level_2 NOT IN ('ABSCONSION', 'IN_CUSTODY', 'INTERNAL_UNKNOWN')
    -- we only support Tasks for clients at these levels
    AND correctional_level IN ('MEDIUM', 'HIGH', 'MAXIMUM')
    -- exclude clients in Transition Centers (TCs)
    /* TODO(#50537): Should we do the TC exclusions via `location_metadata` at some
    point, if we can add the necessary info to the metadata there? */
    AND supervision_office NOT IN (
        -- TCs
        'TCKC', 'TCSTL'
    )
"""

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
