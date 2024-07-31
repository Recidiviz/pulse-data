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
"""
Describes the spans of time during which someone in ND is not in the Women's Treatment 
and Recovery Unit (WTRU) in Bismarck Transition Center (BTC).
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NOT_IN_WTRU_BTC"

_DESCRIPTION = """Describes the spans of time during which someone in ND is not in the Women's Treatment
and Recovery Unit (WTRU) in Bismarck Transition Center (BTC)."""

_QUERY_TEMPLATE = f"""
SELECT 
    state_code,
    person_id,
    start_date,
    DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(start_date AS wtru_start_date)) AS reason,
    start_date AS wtru_start_date,
FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_sessions_materialized`
WHERE state_code = 'US_ND'
    AND (facility = 'BTC' AND REGEXP_CONTAINS(housing_unit, r'WTRU'))
    AND start_date < {nonnull_end_date_exclusive_clause('end_date_exclusive')}
GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_ND,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="wtru_start_date",
                type=bigquery.StandardSqlTypeNames.DATE,
                description="Date when the person was placed into the WTRU",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
