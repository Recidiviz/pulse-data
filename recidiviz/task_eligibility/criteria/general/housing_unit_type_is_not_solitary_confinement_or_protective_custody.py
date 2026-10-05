# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""Describes spans of time that residents are not in any solitary confinement housing
unit type and not in protective custody.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HOUSING_UNIT_TYPE_IS_NOT_SOLITARY_CONFINEMENT_OR_PROTECTIVE_CUSTODY"

_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    FALSE AS meets_criteria,
    TO_JSON(STRUCT(
        housing_unit_type AS housing_unit_type,
        start_date AS housing_unit_type_start_date
    )) AS reason,
    housing_unit_type,
    start_date AS housing_unit_type_start_date,
FROM `{project_id}.{sessions_dataset}.housing_unit_type_sessions_materialized`
WHERE CONTAINS_SUBSTR(housing_unit_type, 'SOLITARY_CONFINEMENT')
    OR housing_unit_type = 'PROTECTIVE_CUSTODY'
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="housing_unit_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Solitary confinement or protective custody housing unit "
                "type the resident is in",
            ),
            ReasonsField(
                name="housing_unit_type_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date the resident's stay in this housing unit type began",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
