# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Identifies when clients do not have a supervision level of 'FURLOUGH', as tracked by
our `sessions` dataset.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_LEVEL_IS_NOT_FURLOUGH"

# TODO(#22511): Refactor criterion to build off of a general criteria view builder.
_QUERY_TEMPLATE = f"""
    WITH furlough_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
        /* TODO(#20035): Replace with raw-text supervision level sessions once views
        agree. */
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
        WHERE compartment_level_1 = 'SUPERVISION' 
            AND correctional_level = 'FURLOUGH' 
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(
            start_date AS furlough_start_date
        )) AS reason,
        start_date AS furlough_start_date,
    FROM ({aggregate_adjacent_spans(table_name='furlough_spans')})
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="furlough_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date that a client started on furlough supervision",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
