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
"""Spans during which someone is not currently subject to a D1 sanction
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_NO_ACTIVE_D1_SANCTIONS"

_DESCRIPTION = """Spans during which someone is not currently subject to a D1 sanction
"""

_QUERY_TEMPLATE = f"""
    WITH d1_sanctions AS (
        SELECT
            state_code,
            person_id,
            date_effective AS start_date,
            projected_end_date AS end_date,
            date_effective AS latest_sanction_start_date,
            projected_end_date AS latest_sanction_end_date,
        FROM `{{project_id}}.normalized_state.state_incarceration_incident_outcome`
        WHERE
            outcome_type_raw_text = 'D1'
            -- Exclude 0-day sanctions
            AND date_effective != projected_end_date
    )
    ,
    {create_sub_sessions_with_attributes(
        table_name="d1_sanctions",
    )}
    ,
    dedup_cte AS
    /*
    If a person has overlapping sanctions, they will have duplicate sub-sessions for the period of
    time where there were more than 1 sanction. We deduplicate below so that we surface the
    most-recent sanction that is relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY 
            latest_sanction_end_date DESC,
            latest_sanction_start_date DESC
        ) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a sanction. A
    new session exists either when a person becomes eligible, or if a person has a sanction immediately following
    this one.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_sanction_start_date', 'latest_sanction_end_date'],
                       end_date_field_name='end_date')}
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(
            latest_sanction_start_date,
            latest_sanction_end_date
        )) AS reason,
    FROM sessionized_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
