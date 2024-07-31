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
"""Defines a criteria span view that shows spans of time during which someone is serving
a supervision or supervision out of state parole term of one year or more.
This query is only relevant for states who have parole sentencing data stored separately in supervision sentences.
"""

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

_CRITERIA_NAME = "NO_ABSCONSION_WITHIN_10_YEARS"

_DESCRIPTION = """"""

_QUERY_TEMPLATE = f"""
WITH absconded_sessions AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        DATE_ADD(start_date, INTERVAL 10 YEAR) AS end_date,
        FALSE AS meets_criteria,
        start_date AS absconded_date,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
    WHERE compartment_level_2 = 'ABSCONSION'
),

{create_sub_sessions_with_attributes('absconded_sessions')}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(meets_criteria) AS meets_criteria,
    TO_JSON(STRUCT(MAX(absconded_date) AS most_recent_absconded_date)) AS reason,
    MAX(absconded_date) AS most_recent_absconded_date,
FROM sub_sessions_with_attributes
GROUP BY 1,2,3,4
"""


VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="most_recent_absconded_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
