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
"""Spans during which someone is in Restrictive Housing and has been subject to a D1 sanction
since their Restrictive Housing placement began.
"""
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import (
    latest_d1_sanction_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_D1_SANCTION_AFTER_RESTRICTIVE_HOUSING_START"

_DESCRIPTION = """Spans during which someone is in Restrictive Housing and has been
subject to a D1 sanction since their Restrictive Housing placement began.
"""

_QUERY_TEMPLATE = f"""
    WITH latest_restrictive_housing_start_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            LEAD(start_date) OVER w AS end_date,
            start_date AS latest_restrictive_housing_start_date,
        FROM `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized`
        WHERE 
            -- TODO(#21788): Use ingested enums once MO housing_unit_type is ingested
            confinement_type IN ("SOLITARY_CONFINEMENT")
        WINDOW w AS (
            PARTITION BY state_code, person_id
            ORDER BY start_date ASC
        )
    )
    ,
    {latest_d1_sanction_spans_cte()}
    ,
    intersection_spans AS (
        {create_intersection_spans(
            table_1_name="latest_restrictive_housing_start_spans",
            table_2_name="latest_d1_sanction_spans",
            index_columns=["state_code", "person_id"],
            table_1_columns=["latest_restrictive_housing_start_date"],
            table_2_columns=["latest_d1_sanction_start_date"],
            table_1_end_date_field_name="end_date",
            table_2_end_date_field_name="end_date"
        )}
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        latest_d1_sanction_start_date >= latest_restrictive_housing_start_date AS meets_criteria,
        TO_JSON(STRUCT(
            latest_d1_sanction_start_date,
            latest_restrictive_housing_start_date AS restrictive_housing_start_date
        )) AS reason
    FROM intersection_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
