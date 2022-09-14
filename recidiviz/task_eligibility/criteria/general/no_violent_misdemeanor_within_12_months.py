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
"""Defines a criteria span view that shows spans of time during which there
is no violent misdemeanor within 12 months on supervision
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_VIOLENT_MISDEMEANOR_WITHIN_12_MONTHS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which there
is no violent misdemeanor within 12 months on supervision."""

_QUERY_TEMPLATE = f"""
WITH supervision_violations AS (
    /*This CTE identifies violent misdemeanor violations and sets a 12 month 
    window where felony_violation is TRUE*/
    SELECT
        vr.state_code,
        vr.person_id,
        COALESCE(v.violation_date, vr.response_date) AS start_date,
        DATE_ADD(COALESCE(v.violation_date, vr.response_date), INTERVAL 12 MONTH) AS end_date,
        TRUE as violent_misdemeanor_violation,
        COALESCE(v.violation_date, vr.response_date) AS violation_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_type_entry` vt
        ON vr.supervision_violation_id = vt.supervision_violation_id
        AND vr.person_id = vt.person_id
        AND vr.state_code = vt.state_code
        AND vt.violation_type = "MISDEMEANOR"
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        ON vr.supervision_violation_id = v.supervision_violation_id
        AND vr.person_id = v.person_id
        AND vr.state_code = v.state_code
    WHERE v.is_violent
    #TODO(#15105) remove this section of the query once criteria can default to true
    UNION ALL
   /*This CTE identifies supervision sessions for individuals 
   and sets violent_misdemeanor_violation as FALSE*/
    SELECT
        state_code,
        person_id,
        start_date,
        DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
        FALSE as violent_misdemeanor_violation,
        NULL AS violation_date
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` ses
    WHERE compartment_level_1 = 'SUPERVISION'
),
{create_sub_sessions_with_attributes('supervision_violations')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    --when felony_violation is TRUE meets_criteria is FALSE 
    --for identical spans, choose TRUE over FALSE felony_convictions
    NOT LOGICAL_OR(violent_misdemeanor_violation) AS meets_criteria,
    TO_JSON(STRUCT(ARRAY_AGG(violation_date IGNORE NULLS) AS latest_violent_convictions)) AS reason,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""
VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
