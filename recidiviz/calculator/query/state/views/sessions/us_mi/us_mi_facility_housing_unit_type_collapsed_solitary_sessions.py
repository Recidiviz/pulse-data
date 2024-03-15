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
# =============================================================================
"""Michigan state specific view of solitary confinement sessions within a specific facility and excluding
the START program."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_NAME = (
    "us_mi_facility_housing_unit_type_collapsed_solitary_sessions"
)

US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_DESCRIPTION = """
Michigan state specific view of solitary confinement sessions within a specific facility and excluding
the START program."""

US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_QUERY_TEMPLATE = f"""
 WITH solitary_sessions_without_start_preprocessed AS (
   /* This CTE sessionizes all adjacent solitary housing_unit_type sessions, with the exception of START sessions,
   that are within the same facility. Because clients who are part of START are in 
   solitary, but do not require SCC reviews with the same date cadence, they are removed from this query */
        SELECT
            * EXCEPT (housing_unit_type),
            IF(
                (CONTAINS_SUBSTR(housing_unit_type, 'SOLITARY_CONFINEMENT') AND
                --exclude START sessions
                NOT CONTAINS_SUBSTR(housing_unit_type, 'OTHER_SOLITARY_CONFINEMENT')),
                'SOLITARY_CONFINEMENT',
                housing_unit_type
            ) AS housing_unit_type_collapsed_solitary,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    )
    ({aggregate_adjacent_spans(
        table_name="solitary_sessions_without_start_preprocessed",
        attribute=["housing_unit_type_collapsed_solitary","facility"],
        end_date_field_name='end_date_exclusive'
    )})
"""

US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    view_id=US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_NAME,
    description=US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER.build_and_print()
