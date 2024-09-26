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
"""Describes spans of time when someone is a US citizen or legal permanent resident
without an ICE detainer"""
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

_CRITERIA_NAME = "US_AZ_IS_US_CITIZEN_OR_LEGAL_PERMANENT_RESIDENT"

_DESCRIPTION = """Describes spans of time when someone is a US citizen or legal permanent resident
without an ICE detainer"""

_QUERY_TEMPLATE = """
WITH citizenship_status_with_priority AS (
  SELECT 
    *,
  # We assume individuals with an update_date of 2019-11-30 (data migration date) have been on their current 
  # immigration status since 1900. This assumption is not ideal, but its the best we can do.
    IF(update_date = '2019-11-30', '1900-01-01', update_date) AS start_date,
  FROM (
    SELECT 
        peid.state_code,
        peid.person_id,
        COUNTRY.DESCRIPTION AS citizenship_status, 
        citizenship_status_priority,
        SAFE_CAST(LEFT(dem.UPDT_DTM, 10) AS DATE) AS update_date,
    -- DEMOGRAPHICS contains the codes for many demographic fields, including citizenship status,
    -- which includes the categories in the UNNEST below. We join on the LOOKUPS table to get the
    -- human-readable descriptions of the codes.
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.DEMOGRAPHICS_latest` dem
    INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.LOOKUPS_latest` COUNTRY
        ON(COUNTRY_OF_CITIZENSHIP = LOOKUP_ID)
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` peid
        ON peid.external_id = dem.PERSON_ID
        AND peid.id_type = 'US_AZ_PERSON_ID'
    LEFT JOIN (
        SELECT *
        FROM UNNEST(
        -- The order of the citizenship statuses is important. We want to prioritize U.S. citizens
        -- and legal permanent residents without an ICE detainer (LEGAL/NO DP).
            ["U.S. CITIZEN", "LEGAL/NO DP", "LEGAL P/DP", "CRIMNL ALIEN", "UNKNOWN"]
        ) AS citizenship_status
        WITH OFFSET citizenship_status_priority 
    ) AS priority
    ON priority.citizenship_status = COUNTRY.DESCRIPTION
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id, GREATEST(update_date) ORDER BY citizenship_status_priority) = 1
  )
)

SELECT 
    state_code,
    person_id,
    start_date,
    LEAD(start_date) OVER(PARTITION BY state_code, person_id ORDER BY start_date) AS end_date,
    -- We only care about US citizens and legal permanent residents without an ICE detainer
    citizenship_status IN ("U.S. CITIZEN", "LEGAL/NO DP") AS meets_criteria,
    TO_JSON(STRUCT(citizenship_status AS citizenship_status)) AS reason,
    citizenship_status,
FROM citizenship_status_with_priority
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="citizenship_status",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="What type of citizenship/residency someone has",
    )
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=False,
        reasons_fields=_REASONS_FIELDS,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
