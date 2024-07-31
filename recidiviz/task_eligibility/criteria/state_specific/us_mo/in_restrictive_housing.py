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
"""Describes the spans of time during which someone in MO
is in Restrictive Housing.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_IN_RESTRICTIVE_HOUSING"

_DESCRIPTION = """Describes the spans of time during which someone in MO
is in Restrictive Housing.
"""

_QUERY_TEMPLATE = """
    SELECT 
        state_code, 
        person_id, 
        start_date,
        end_date,
        confinement_type IN ("SOLITARY_CONFINEMENT") as meets_criteria,
        TO_JSON(STRUCT(
            confinement_type AS confinement_type
        )) AS reason,
        confinement_type AS confinement_type
    -- TODO(#23550) Replace with housing_unit_type once ingested
    FROM `{project_id}.{sessions_dataset}.us_mo_confinement_type_sessions_materialized`
    WHERE start_date != COALESCE(end_date, '9999-01-01')
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="confinement_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of cell or unit where the resident is confined.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
