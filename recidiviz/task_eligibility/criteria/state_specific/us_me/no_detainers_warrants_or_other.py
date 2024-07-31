# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# =============================================================================

"""Defines a criteria view that shows spans of time for
which residents have detainers, warrants or other pending holds.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
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

_CRITERIA_NAME = "US_ME_NO_DETAINERS_WARRANTS_OR_OTHER"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents have detainers, warrants or other pending holds.
"""

_QUERY_TEMPLATE = f"""
WITH cur_stat_cte AS (
  SELECT 
    state_code,
    person_id,
    SAFE_CAST(LEFT(Effct_Date, 10) AS DATE) AS start_date,
    SAFE_CAST(LEFT(Ineffct_Date, 10) AS DATE) AS end_date,
    False AS meets_criteria,
    E_Current_Status_Desc AS detainer,
  FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_125_CURRENT_STATUS_HIST_latest` 
  -- TODO(#16975) pull from our schema once ingested
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_1000_CURRENT_STATUS_latest`
    ON Cis_1000_Current_Status_Cd = Current_Status_Cd
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
      ON Cis_100_Client_Id = external_id
      AND id_type = 'US_ME_DOC'
  WHERE Cis_1000_Current_Status_Cd = '7'
),
{create_sub_sessions_with_attributes('cur_stat_cte', use_magic_date_end_dates = True)},
sub_sessions_with_attributes_grouped AS (
  SELECT 
    state_code,
    person_id,
    start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    meets_criteria,
    detainer,
  FROM sub_sessions_with_attributes
  WHERE start_date != end_date
  GROUP BY 1,2,3,4,5,6
)

SELECT 
  state_code,
  person_id,
  start_date,
  end_date,
  meets_criteria,
  TO_JSON(STRUCT(detainer AS detainer, start_date AS detainer_start_date)) AS reason,
  detainer,
  start_date AS detainer_start_date,
FROM sub_sessions_with_attributes_grouped
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="detainer",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="Detainer, warrant or other pending hold.",
        ),
        ReasonsField(
            name="detainer_start_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Start date of the detainer, warrant or other pending hold.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
