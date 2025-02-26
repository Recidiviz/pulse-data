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
"""This criteria view builder defines spans of time where residents are not on MAXIMUM custody level
as tracked by our `sessions` dataset.
"""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODY_LEVEL_IS_NOT_MAX"

_DESCRIPTION = """This criteria view builder defines spans of time where residents are not on MAXIMUM custody level
as tracked by our `sessions` dataset.
"""

_QUERY_TEMPLATE = f"""
WITH max_spans AS (
    SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
    FROM
        `{{project_id}}.{{sessions_dataset}}.custody_level_sessions_materialized`
    WHERE
        custody_level = 'MAXIMUM' 
)
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(TRUE AS custody_level_is_max)) AS reason,
    FROM 
        ({aggregate_adjacent_spans(table_name='max_spans')})
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
