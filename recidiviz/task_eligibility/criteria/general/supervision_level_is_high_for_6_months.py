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
"""This criteria view builder defines spans of time where clients have been on HIGH
supervision level for 6 months as tracked by our `sessions` dataset.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_LEVEL_IS_HIGH_FOR_6_MONTHS"

_QUERY_TEMPLATE = f"""

WITH clients_on_high_with_6months AS (
    SELECT 
        *,
        DATE_ADD(start_date, INTERVAL 6 MONTH) start_date_plus_6mo
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized`
    WHERE supervision_level = 'HIGH'
)
SELECT 
    state_code,
    person_id,
    start_date_plus_6mo AS start_date,
    end_date_exclusive AS end_date,
    TRUE as meets_criteria,
    TO_JSON(STRUCT(start_date_plus_6mo AS high_start_date)) AS reason,
    start_date_plus_6mo AS high_start_date,
FROM clients_on_high_with_6months
WHERE {nonnull_end_date_clause('end_date_exclusive')} > start_date_plus_6mo
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="high_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date that a client started on high supervision",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
