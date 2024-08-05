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
"""Describes spans of time that residents are in OTHER_SOLITARY_CONFINEMENT (ex in MI, the START program) """
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "HOUSING_UNIT_TYPE_IS_NOT_OTHER_SOLITARY_CONFINEMENT"

_DESCRIPTION = """Describes spans of time that residents are in OTHER_SOLITARY_CONFINEMENT (ex in MI, the START program)"""

_QUERY_TEMPLATE = """
       SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
            FALSE AS meets_criteria,
            TO_JSON(STRUCT(
                start_date AS START_start_date
            )) AS reason,
            start_date AS other_solitary_start_date
        FROM `{project_id}.{sessions_dataset}.housing_unit_type_sessions_materialized` hu
        WHERE hu.housing_unit_type = 'OTHER_SOLITARY_CONFINEMENT'
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="other_solitary_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date that a resident began other_solitary_confinement",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
