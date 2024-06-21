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
"""Describes the spans of time when at least 12 months have passed since a resident's last classification assessment."""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_AT_LEAST_12_MONTHS_SINCE_LATEST_ASSESSMENT"

_DESCRIPTION = """Describes the spans of time when at least 12 months have passed since a resident's last
classification assessment."""

_QUERY_TEMPLATE = f"""
    WITH assessment_sessions_cte AS
    (
        SELECT
            state_code,
            person_id,
            /* The logic behind the start and end date is the following -  We want the span when someone is ineligible 
            to begin on classification_decision_date; this ensures that someone will continue to show up as eligible 
            even after a form has been submitted for them, until the final hearing is done and a decision is made.
            
            We want the span to end 12 months after the ClassificationDate, as per what Counselors have told us they use
            to evaluate when a person needs an annual reclass.
            
            Since a person is considered "eligible" during the whole month where their assessment is due, we use a
            DATE_TRUNC() for the end date so that someone becomes eligible in the month of their assessment due date.
            
            This also improves tracking historical spans in situations when an annual assessment may be filled out
            prior to the due date, since we consider someone eligible the starting the first of the month when their
            assessment is due 
            */
            classification_decision_date AS start_date,
            DATE_SUB(DATE_TRUNC(assessment_due_date, MONTH), INTERVAL 1 WEEK) AS end_date,
            FALSE AS meets_criteria,
            assessment_date,
            assessment_due_date,
        FROM
            `{{project_id}}.analyst_data.custody_classification_assessment_dates_materialized`
        WHERE
            state_code = 'US_TN'
            -- Removes a small number of spans where ClassificationDecisionDate appears to be 1 year after the ClassficationDate
            AND DATE_SUB(DATE_TRUNC(assessment_due_date, MONTH), INTERVAL 1 WEEK) > classification_decision_date
    )
    ,
    {create_sub_sessions_with_attributes('assessment_sessions_cte')}
    ,
    dedup_cte AS
    (
        SELECT
            *,
        FROM sub_sessions_with_attributes
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
            ORDER BY assessment_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is eligible. A
    new session exists either when a person becomes eligible, or if a person has an additional assessment in the 
    specified date interval, which changes the "assessment_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['assessment_date','assessment_due_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(assessment_date AS most_recent_assessment_date,
                        assessment_due_date AS assessment_due_date
                        )
                ) AS reason,
        assessment_date AS most_recent_assessment_date,
        assessment_due_date AS assessment_due_date,
    FROM sessionized_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        state_code=StateCode.US_TN,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="most_recent_assessment_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="assessment_due_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
