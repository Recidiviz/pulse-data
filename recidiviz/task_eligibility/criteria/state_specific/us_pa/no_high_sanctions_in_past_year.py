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
"""
Defines a criteria view that shows spans of time when clients have not incurred a high sanction
for 12 months
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NO_HIGH_SANCTIONS_IN_PAST_YEAR"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients have not incurred a high sanction
for 12 months"""

_REASON_QUERY = f"""
WITH sanctions_sessions_cte AS (
        SELECT
          v.person_id,
          v.state_code,
          COALESCE(violation_date, response_date) AS start_date,
          DATE_ADD(COALESCE(violation_date, response_date), INTERVAL 12 MONTH) AS end_date,
          COALESCE(violation_date, response_date) AS latest_sanction_date,
          FALSE as meets_criteria,
        FROM
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation` v
        LEFT JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response` vr
        USING
          (person_id, state_code, supervision_violation_id)
        LEFT JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response_decision_entry` vrd
        USING
          (person_id, state_code, supervision_violation_response_id)
        WHERE
          vrd.decision_raw_text IN (
            'IDOX', -- Placement in Drug and Alcohol Detoxification Facility
            'CPCB', -- Placement in CCC Half Way Back
            'IPAT', -- Placement in Inpatient Drug and Alcohol Facility 
            'IPMH', -- Placement in a Mental Health Facility
            'VCCF', -- Placement in Parole Violator CCC / CCF
            'ARR2', -- Incarceration
            'GARR', -- GPS House Arrest
            'GCON', -- GPS Home Confinement
            'SPB5', -- Swift, Predictable, and Brief 5 Days
            'SPB7', -- Swift, Predictable, and Brief 7 Day
            'HOTR' -- Other
            )
          AND state_code = 'US_PA'
        ),
    /*
    A person can have multiple sanctions within a 12 month period, we create sub sessions to deal with this
    */
        {create_sub_sessions_with_attributes('sanctions_sessions_cte')},
    /*
    If a person has more than 1 sanction in a 12 month period, they will have duplicate sub-sessions for the period of
    time where there were more than 1 sanction. For example, if a person has a sanction on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both sanctions are relevant at that time.
    We deduplicate below so that we surface the most-recent sanction that as relevant at each time. 
    */
    dedup_cte AS (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY latest_sanction_date DESC) = 1
        ),
    /*
    Sessionize again so that we have continuous periods of time for which a person is not eligible due to a high sanction. A
    new session exists either when a person becomes eligible, or if a person has an additional sanction within a 12-month
    period which changes the "latest_sanction_date" value.
    */
    sessionized_cte AS (
        {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_sanction_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_sanction_date)) AS reason,
        latest_sanction_date,
    FROM sessionized_cte"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_PA,
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        criteria_spans_query_template=_REASON_QUERY,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_sanction_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest high sanction",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
