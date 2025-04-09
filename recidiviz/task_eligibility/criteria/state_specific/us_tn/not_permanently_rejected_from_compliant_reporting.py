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
# ============================================================================
"""Describes the spans of time when a TN client has been permanently rejected from
Compliant Reporting.
"""

from google.cloud import bigquery

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

_CRITERIA_NAME = "US_TN_NOT_PERMANENTLY_REJECTED_FROM_COMPLIANT_REPORTING"

_QUERY_TEMPLATE = """
    WITH cte AS 
    (
    /*
    Use the ContactNoteType table to get the unique person/days in which a person had a contact note of type DEIJ
    or DECR. Store these contact note type values as an array.
    */
    SELECT 
        person_id,
        "US_TN" AS state_code,
        CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS contact_date,
        ARRAY_AGG(DISTINCT ContactNoteType ORDER BY ContactNoteType) AS contact_code
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.ContactNoteType_latest` a
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON a.OffenderID = pei.external_id
        AND pei.state_code = 'US_TN'
    WHERE ContactNoteType IN ('DEIJ','DECR')
    GROUP BY 1,2,3
    )
    /*
    Create sessions between contact_dates so that we can report the most recent contact that prevents a person from
    being eligible
    */
    SELECT 
        state_code,
        person_id,
        contact_date AS start_date,
        LEAD(contact_date) OVER(PARTITION BY person_id ORDER BY contact_date) AS end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(
            contact_code AS contact_code,
            contact_date AS contact_code_date
        )) AS reason,
        contact_code,
        contact_date AS contact_code_date,
    FROM cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TN,
            instance=DirectIngestInstance.PRIMARY,
        ),
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="contact_code",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Contact-note type for latest permanent rejection",
            ),
            ReasonsField(
                name="contact_code_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of contact note with latest permanent rejection",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
