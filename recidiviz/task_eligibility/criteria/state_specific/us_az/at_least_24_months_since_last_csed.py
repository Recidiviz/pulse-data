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
"""Describes spans of time when it has been 24 months since a resident's previous CSED"""
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

_CRITERIA_NAME = "US_AZ_AT_LEAST_24_MONTHS_SINCE_LAST_CSED"

_DESCRIPTION = """Describes spans of time when it has been 24 months since a resident's previous CSED"""

_REASONS_FIELDS = [
    ReasonsField(
        name="most_recent_csed",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="A resident's most recent CSED",
    ),
]

_QUERY_TEMPLATE = f"""
WITH distinct_cseds AS (
    SELECT
        DISTINCT
        sent.state_code,
        sent.person_id,
        COALESCE(
            SAFE_CAST(SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', 
                COALESCE(off.COMMUNITY_SUPV_END_DTM_ML, off.COMMUNITY_SUPV_END_DTM_ARD, off.COMMUNITY_SUPV_END_DTM)) AS DATE),
            SAFE_CAST(LEFT(
                COALESCE(off.COMMUNITY_SUPV_END_DTM_ML, off.COMMUNITY_SUPV_END_DTM_ARD, off.COMMUNITY_SUPV_END_DTM), 10) AS DATE),
            SAFE.PARSE_DATE('%m/%d/%Y', 
                COALESCE(off.COMMUNITY_SUPV_END_DTM_ML, off.COMMUNITY_SUPV_END_DTM_ARD, off.COMMUNITY_SUPV_END_DTM))
        )
        AS start_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_sentence` sent
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.AZ_DOC_SC_OFFENSE_latest` off
        ON(sent.external_id = off.OFFENSE_ID)
    WHERE sent.state_code = 'US_AZ'
),

distinct_cseds_spans AS (
    SELECT
        state_code,
        person_id,
        start_date,
        DATE_ADD(start_date, INTERVAL 24 MONTH) AS end_date,
        start_date AS csed,
    FROM distinct_cseds
    WHERE start_date IS NOT NULL
),
{create_sub_sessions_with_attributes('distinct_cseds_spans')}
 
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(
        MAX(csed) AS most_recent_csed
    )) AS reason,
    MAX(csed) AS most_recent_csed
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_AZ,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
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
