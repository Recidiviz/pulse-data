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

"""Defines a criteria view that shows spans of time when clients are not in the Intensive Mental Health Unit (IMHU)
in Maine State Prison (MSP)
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_NOT_IN_MSP_IMHU"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients are not in the Intensive Mental
Health Unit (IMHU) in Maine State Prison (MSP)"""

_QUERY_TEMPLATE = f"""
WITH
  msp_imhu_clients AS (
      SELECT
        state_code,
        person_id,
        admission_date AS start_date,
        release_date AS end_date,
        TRUE AS is_imhu,
      FROM
        `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period`
      WHERE
        state_code = 'US_ME'
        AND facility = 'MAINE STATE PRISON'
        AND housing_unit = 'IMHU' ),
  {create_sub_sessions_with_attributes('msp_imhu_clients')},
  deduped_sub_sessions AS (
      SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        is_imhu,
      FROM
        sub_sessions_with_attributes
      WHERE start_date != end_date
      GROUP BY
        1,
        2,
        3,
        4,
        5),
  /* This boolean is then used to aggregate spans on which someone is and isn't in the IMHU */ 
  sessionized_cte AS 
        ( {aggregate_adjacent_spans(table_name='deduped_sub_sessions', attribute=['is_imhu'])} )
SELECT
  state_code,
  person_id,
  start_date,
  {revert_nonnull_end_date_clause('end_date')} AS end_date,
  NOT is_imhu AS meets_criteria,
  TO_JSON(STRUCT(start_date AS imhu_start_date)) AS reason,
  start_date AS imhu_start_date,
FROM
  sessionized_cte
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="imhu_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the client was admitted to the Intensive Mental Health Unit (IMHU).",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
