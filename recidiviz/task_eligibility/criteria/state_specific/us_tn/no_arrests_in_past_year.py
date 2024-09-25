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
"""Describes the spans of time when a TN client has not had a positive arrest check for 12 months."""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_ARRESTS_IN_PAST_YEAR"

_DESCRIPTION = """Describes the spans of time when a TN client has not had a positive arrest check for 12 months."""

_QUERY_TEMPLATE = f"""
    WITH arrp_sessions_cte AS
    (
        SELECT  
            DISTINCT
            state_code,
            person_id, 
            CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS start_date,
            DATE_ADD(CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE), INTERVAL 12 MONTH) AS end_date,
            --create this field to keep track of the actual positive arrest check date even after we sub-sessionize 
            -- to handle overlapping-periods (cases when a person has more than one check in a 12 month period)
            CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS latest_positive_arrest_check_date,
            FALSE as meets_criteria,
        FROM
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` contact
        INNER JOIN 
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            contact.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            ContactNoteType = 'ARRP'
    )
    ,
    /*
    If a person has more than 1 positive arrest check in a 12 month period, they will have overlapping sessions
    created in the above CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('arrp_sessions_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 positive arrest check in a 12 month period, they will have duplicate sub-sessions for 
    the period of time where there were more than 1 check. For example, if a person has a check on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both checks are relevant at that time.
    We deduplicate below so that we surface the most-recent positive check that as relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_positive_arrest_check_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a positive check. A
    new session exists either when a person becomes eligible, or if a person has an additional check within a 12-month
    period which changes the "latest_positive_arrest_check_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_positive_arrest_check_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_positive_arrest_check_date AS latest_positive_arrest_check)) AS reason,
        latest_positive_arrest_check_date,
    FROM sessionized_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TN,
        meets_criteria_default=True,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
        ),
        reasons_fields=[
            ReasonsField(
                name="latest_positive_arrest_check_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
