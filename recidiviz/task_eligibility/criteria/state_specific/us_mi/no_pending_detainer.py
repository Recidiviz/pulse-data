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
"""Defines a criteria span view that shows spans of time during which someone does
not have a pending detainer.
"""
from google.cloud import bigquery

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

_CRITERIA_NAME = "US_MI_NO_PENDING_DETAINER"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone does
not have a pending detainer. 
"""

_QUERY_TEMPLATE = f"""
WITH detainers_parsed AS(
/*This CTE identifies parses detainer data from raw data*/
    SELECT
      pei.state_code,
      pei.person_id,
      CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %I:%M:%S%p', REGEXP_REPLACE(detainer_received_date, r'\\:\\d\\d\\d', '')) AS DATETIME) AS DATE) AS start_date,
      --revoked_date, expiration_date, and inactive_date all have instances where they are non null but the other fields are null, 
      --so here we coalesce all three dates for an end_date 
      COALESCE(CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %I:%M:%S%p', REGEXP_REPLACE(revoked_date, r'\\:\\d\\d\\d', '')) AS DATETIME) AS DATE), 
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %I:%M:%S%p', REGEXP_REPLACE(expiration_date,r'\\:\\d\\d\\d', '')) AS DATETIME) AS DATE),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %I:%M:%S%p', REGEXP_REPLACE(inactive_date, r'\\:\\d\\d\\d', '')) AS DATETIME) AS DATE), "9999-12-31") AS end_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.ADH_OFFENDER_DETAINER_latest` o
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON o.offender_id= pei.external_id
      AND pei.state_code = 'US_MI'
      AND pei.id_type = "US_MI_DOC"
), 
pending_detainers AS (
/*This CTE identifies pending detainers and sets active_ppo as TRUE*/
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      TRUE AS pending_detainer
    FROM detainers_parsed
    WHERE start_date != end_date
),
{create_sub_sessions_with_attributes('pending_detainers')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        NOT pending_detainer AS meets_criteria,
        TO_JSON(STRUCT(LOGICAL_OR(pending_detainer) AS active_ppo)) AS reason,
        LOGICAL_OR(pending_detainer) AS active_ppo,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5
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
        meets_criteria_default=True,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="active_ppo",
                type=bigquery.enums.SqlTypeNames.BOOLEAN,
                description="#TODO(#29059): Add reasons field description",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
