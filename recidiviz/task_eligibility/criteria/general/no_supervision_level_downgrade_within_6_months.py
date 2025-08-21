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
# =============================================================================
"""Defines a criteria span view that shows spans of time during which clients have
    not experienced a supervision level downgrade within the past 6 months"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_SUPERVISION_LEVEL_DOWNGRADE_WITHIN_6_MONTHS"

_DESCRIPTION = __doc__

_QUERY = f"""
WITH sld_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        DATE_ADD(start_date, INTERVAL 6 MONTH) AS end_date,
        start_date AS supervision_level_downgrade_date,
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized`
    WHERE supervision_downgrade = 1
),
{create_sub_sessions_with_attributes('sld_sessions')}
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(MAX(supervision_level_downgrade_date) AS supervision_level_downgrade_date)) AS reason,
    MAX(supervision_level_downgrade_date) AS supervision_level_downgrade_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="supervision_level_downgrade_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of client's most recent supervision level downgrade",
            ),
        ],
    )
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
