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
# ============================================================================
"""Spans of time when a client in TN has not had a warrant within the past 2 years."""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
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

_CRITERIA_NAME = "US_TN_NO_WARRANT_WITHIN_2_YEARS"

# TODO(#33630): Refactor this query to use ingested data if/when possible.
_QUERY_TEMPLATE = f"""
    WITH ineligibility_spans AS (
        /* Identify warrants and the spans of ineligibility in which they result. */
        SELECT
            pei.state_code,
            pei.person_id,
            -- use date of relevant contact note as the date of the warrant
            CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE) AS warrant_date,
            CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE) AS start_date,
            DATE_ADD(CAST(CAST(contact.ContactNoteDateTime AS DATETIME) AS DATE), INTERVAL 2 YEAR) AS end_date,
            FALSE as meets_criteria,
        FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ContactNoteType_latest` contact
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON contact.OffenderID=pei.external_id
            AND pei.id_type='US_TN_DOC'
        /* Use contact notes with type 'VWAR' ('VIOLATION WARRANT AND REPORT ISSUED')
        to identify the issuance of a warrant. */
        WHERE contact.ContactNoteType='VWAR'
    ),
    /* Sub-sessionize in case there are overlapping spans (i.e., if someone has multiple
    still-relevant warrants at once). */
    {create_sub_sessions_with_attributes("ineligibility_spans")},
    ineligibility_spans_aggregated AS (
        /* Aggregate across sub-sessions to get attributes for each span of time for
        each person. */
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            LOGICAL_AND(meets_criteria) AS meets_criteria,
            MAX(warrant_date) AS latest_warrant_date,
        FROM sub_sessions_with_attributes
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_warrant_date AS latest_warrant_date
        )) AS reason,
        latest_warrant_date,
    FROM ineligibility_spans_aggregated
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN,
        instance=DirectIngestInstance.PRIMARY,
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="latest_warrant_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest warrant",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
