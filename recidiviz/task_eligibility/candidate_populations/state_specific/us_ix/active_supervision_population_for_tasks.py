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
"""
This script selects all spans of time in which a person is a candidate for active supervision
for tasks in the state of Idaho. Specifically, it selects people with the following
conditions:
- Active supervision: probation, parole, or dual
- Case types: GENERAL, SEX_OFFENSE, XCRC, or MENTAL_HEALTH_COURT
- Supervision levels: MINIMUM, MEDIUM, HIGH, or XCRC
"""

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_IX_ACTIVE_SUPERVISION_POPULATION_FOR_TASKS"

_QUERY_TEMPLATE = f"""
WITH active_supervision_population AS (
    -- Active supervision population on PROBATION, PAROLE, or DUAL supervision
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        compartment_level_1,
        compartment_level_2,
        SAFE_CAST(NULL AS STRING) AS case_type,
        SAFE_CAST(NULL AS STRING) AS supervision_level
    FROM
        `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 IN ('SUPERVISION')
        AND metric_source != "INFERRED"
        AND compartment_level_2 IN ('PROBATION', 'PAROLE', 'DUAL', 'COMMUNITY_CONFINEMENT')
        AND correctional_level NOT IN ('IN_CUSTODY','WARRANT','ABSCONDED','ABSCONSION','EXTERNAL_UNKNOWN')
        AND start_date >= '1900-01-01'
),
supervision_case_and_level AS (
    -- Only GENERAL and SEX_OFFENSE case types with MINIMUM, MEDIUM, or HIGH supervision levels
    SELECT
        ctsl.state_code,
        ctsl.person_id,
        ctsl.start_date,
        ctsl.end_date,
        SAFE_CAST(NULL AS STRING) AS compartment_level_1,
        SAFE_CAST(NULL AS STRING) AS compartment_level_2,
        ctsl.case_type,
        ctsl.supervision_level,
    FROM `{{project_id}}.tasks_views.us_ix_case_type_supervision_level_spans_materialized` ctsl
        WHERE ctsl.case_type IN ('GENERAL', 'SEX_OFFENSE', 'XCRC', 'MENTAL_HEALTH_COURT')
            AND ctsl.supervision_level IN ('MINIMUM', 'MEDIUM', 'HIGH', 'XCRC')
),

combine AS (
    SELECT *
    FROM active_supervision_population

    UNION ALL

    SELECT *
    FROM supervision_case_and_level
),

{create_sub_sessions_with_attributes(
    table_name="combine",)}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
-- We only want spans where both criteria are met, so we filter to those with count > 1
HAVING COUNT(*) > 1

"""

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
