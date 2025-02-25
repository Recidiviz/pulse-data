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
"""Describes the spans of time when a TN client is not eligible due to being within 3 months of a non-permanent
rejection from compliant reporting."""
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

_CRITERIA_NAME = "US_TN_NO_RECENT_COMPLIANT_REPORTING_REJECTIONS"

_DESCRIPTION = """Describes the spans of time when a TN client is not eligible due to being within 3 months of a non-permanent
rejection from compliant reporting.
"""

_QUERY_TEMPLATE = f"""
    WITH rejections_sessions_cte AS 
    (
    SELECT DISTINCT 
        person_id,
        "US_TN" AS state_code,
        CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS start_date,
        DATE_ADD(CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE), INTERVAL 3 MONTH) AS end_date,
        CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS latest_cr_rejection_date,
        ContactNoteType AS contact_code,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` a
    JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei            
    ON pei.external_id = a.OffenderID
          AND id_type = "US_TN_DOC"
    WHERE ContactNoteType IN ('DECF','DEDF','DEDU','DEIO','DEIR')
    )
    ,
    {create_sub_sessions_with_attributes('rejections_sessions_cte')}
    ,
    /*
    If a person has more than 1 CR rejection within a 3 month period, there will be a span in which more than 1
    rejections are relevant. For that period of time, we store the date of the most recent rejection, and all of the 
    unique reason codes that apply at that point. 
    */
    dedup_cte AS
    (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        TO_JSON(STRUCT(
            ARRAY_AGG(DISTINCT latest_cr_rejection_date ORDER BY latest_cr_rejection_date) AS contact_date,
            ARRAY_AGG(DISTINCT contact_code ORDER BY contact_code) AS contact_code
        )) AS reason,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
    )
    ,
    sessionized_cte AS 
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute='reason',
                       is_struct=True,
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        reason,
        JSON_EXTRACT(reason, "$.contact_date") AS contact_date_array,
        JSON_EXTRACT(reason, "$.contact_code") AS contact_code_array,
    FROM sessionized_cte
    """

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="contact_date_array",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="contact_code_array",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
