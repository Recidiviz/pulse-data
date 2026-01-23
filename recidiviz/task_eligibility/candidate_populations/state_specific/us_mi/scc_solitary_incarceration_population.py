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
"""Selects all spans of time in which a person is in INCARCERATION solitary confinement housing type eligible for SCC
review opportunity, which includes TEMPORARY, ADMINISTRATIVE, or DISCIPLINARY solitary confinement types.
"""
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_MI_SCC_SOLITARY_INCARCERATION_POPULATION"

_QUERY_TEMPLATE = """
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    FROM
        `{project_id}.sessions.compartment_sub_sessions_materialized`
    WHERE
        compartment_level_1 IN ('INCARCERATION')
        AND metric_source != "INFERRED"
        AND housing_unit_type IN ('ADMINISTRATIVE_SOLITARY_CONFINEMENT','TEMPORARY_SOLITARY_CONFINEMENT','DISCIPLINARY_SOLITARY_CONFINEMENT')
"""

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
