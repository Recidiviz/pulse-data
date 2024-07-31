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
which residents are within 6 months of having received a A or B DOR."""
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
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    DOR_CRITERIA_COLUMNS,
    dor_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_NO_CLASS_A_OR_B_DOR_FOR_6_MONTHS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which residents are within 6 months of having received a A or B DOR."""

_QUERY_TEMPLATE = f"""
WITH dor_external_id AS (
    {dor_query(columns_str=DOR_CRITERIA_COLUMNS, classes_to_include=['A', 'B'])}
),
dor_w_person_id AS (
  SELECT
      peid.state_code,
      peid.person_id,
      deid.start_date,
      DATE_ADD(deid.start_date, INTERVAL 6 MONTH) AS end_date,
      deid.dor_class,
  FROM dor_external_id deid
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = deid.external_id
    AND peid.id_type = 'US_IX_DOC'
),
{create_sub_sessions_with_attributes(table_name='dor_w_person_id')},
no_dup_subsessions_cte AS (
  SELECT *
  FROM sub_sessions_with_attributes
  -- Only keep the first row for each person_id, state_code, start_date, end_date and
  -- prioritize the most severe classes (A is the most severe)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date
                            ORDER BY dor_class) = 1
)

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    False AS meets_criteria,
    TO_JSON(STRUCT(dor_class AS latest_dor_class)) AS reason,
    dor_class AS latest_dor_class,
FROM no_dup_subsessions_cte"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_IX,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="latest_dor_class",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="The most recent DOR class (A or B) received by the resident",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
