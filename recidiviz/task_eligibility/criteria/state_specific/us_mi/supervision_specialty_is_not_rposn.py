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
"""This criteria view builder defines spans of time that a client is on RPOSN Supervision Specialty
"""
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

MAGIC_END_DATE = "9999-12-31"

_CRITERIA_NAME = "US_MI_SUPERVISION_SPECIALTY_IS_NOT_RPOSN"

_DESCRIPTION = """This criteria view builder defines spans of time that a client is on RPOSN Supervision Specialty
"""

_QUERY_TEMPLATE = f"""
WITH rposn_spans AS (
        SELECT
          'US_MI' AS state_code,
          pei.person_id,
          SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) AS start_date,
          SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE) AS end_date,
          TRUE as is_rposn,
        FROM
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.COMS_Specialties_latest`
        INNER JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
          LTRIM(Offender_Number, '0')= pei.external_id
          AND pei.state_code = 'US_MI'
          AND pei.id_type = "US_MI_DOC"
        WHERE
          Specialty LIKE '%RPOSN%'
          AND SAFE_CAST(SAFE_CAST(start_date AS DATETIME) AS DATE) != COALESCE(SAFE_CAST(SAFE_CAST(end_date AS DATETIME) AS DATE), "{MAGIC_END_DATE}")
),
{create_sub_sessions_with_attributes('rposn_spans')},
deduped_sub_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        is_rposn,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
    TO_JSON(STRUCT(TRUE AS supervision_specialty_is_rposn)) AS reason,
    TRUE AS supervision_specialty_is_rposn,
    FROM deduped_sub_sessions
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_MI,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="supervision_specialty_is_rposn",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether a client is on RPOSN Supervision Specialty",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
