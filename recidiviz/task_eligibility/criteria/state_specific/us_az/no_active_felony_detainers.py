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
"""Describes spans of time during which a candidate does not have an active
    felony detainer"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NO_ACTIVE_FELONY_DETAINERS"

_DESCRIPTION = """Describes spans of time during which a candidate does not have an active
    felony detainer"""

_QUERY_TEMPLATE = f"""
    WITH detainer_status AS (
        SELECT
          pei.state_code,
          pei.person_id,
          CAST(SPLIT(DATE_PLACED, ' ')[OFFSET(0)] AS DATE) AS start_date,
          CAST(SPLIT(CANCEL_DTM, ' ')[OFFSET(0)] AS DATE) AS end_date,
          CAST(SPLIT(DATE_PLACED, ' ')[OFFSET(0)] AS DATE) AS detainer_start_date,
          FALSE AS meets_criteria,
        FROM
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_HWD_DETAINER_latest` hwd_detainer
        LEFT JOIN
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` doc_ep
        USING
          (DOC_ID)
        LEFT JOIN
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.PERSON_latest` person
        USING
          (PERSON_ID)
        LEFT JOIN
          `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.LOOKUPS_latest` status
        ON
          (hwd_detainer.STATUS_ID = LOOKUP_ID)
        INNER JOIN
          `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
          ADC_NUMBER = external_id
        WHERE
          status.DESCRIPTION = 'Detainer Saved'
          AND pei.state_code = 'US_AZ'
          AND pei.id_type = 'US_AZ_ADC_NUMBER'
    ),
    {create_sub_sessions_with_attributes('detainer_status')},
    dedup_cte AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        MAX(detainer_start_date) AS detainer_start_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria AS meets_criteria,
        TO_JSON(STRUCT(
            detainer_start_date AS latest_detainer_date
        )) AS reason,
        detainer_start_date AS latest_detainer_date,
    FROM dedup_cte
    WHERE start_date != {nonnull_end_date_clause('end_date')}
"""

_REASONS_FIELDS = [
    ReasonsField(
        name="latest_detainer_date",
        type=bigquery.enums.StandardSqlTypeNames.DATE,
        description="Date of the most recent felony detainer.",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_AZ,
            instance=DirectIngestInstance.PRIMARY,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
