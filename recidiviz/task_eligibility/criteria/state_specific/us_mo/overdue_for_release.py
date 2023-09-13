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
# ============================================================================
"""Describes the spans of time during which someone in MO
is overdue for release from a Restrictive Housing placement.
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_OVERDUE_FOR_RELEASE"

_DESCRIPTION = """Describes the spans of time during which someone in MO
is overdue for release from a Restrictive Housing placement.
"""

_QUERY_TEMPLATE = f"""
    WITH sanction_spans AS (
        SELECT 
            person_id,
            state_code,
            date_effective as start_date, 
            projected_end_date as end_date,
            TRUE as under_sanction,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome`
        WHERE 
            state_code = "US_MO"
            AND outcome_type_raw_text = "D1"
    )
    ,
    {create_sub_sessions_with_attributes("sanction_spans")}
    ,
    sanction_spans_without_duplicates AS (
        SELECT DISTINCT
            state_code,
            person_id,
            start_date,
            end_date,
            under_sanction
        FROM sub_sessions_with_attributes
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        -- Set current span's end date to null
        IF(end_date > CURRENT_DATE('US/Pacific'), NULL, end_date) AS end_date,
        under_sanction AS meets_criteria,
        TO_JSON(STRUCT(
            start_date as sanction_start_date,
            end_date as sanction_end_date
        )) AS reason
    FROM sanction_spans_without_duplicates
    WHERE 
        start_date != end_date
        AND start_date <= CURRENT_DATE('US/Pacific')
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
