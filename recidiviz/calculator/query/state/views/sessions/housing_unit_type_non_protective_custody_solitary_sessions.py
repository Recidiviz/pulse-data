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
"""Sessionized view of each individual that is incarcerated. Session defined as continuous period of time at a given
 housing unit type, using Recidiviz schema mappings. Collapses over all solitary confinement types, excluding protective custody."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_NAME = (
    "housing_unit_type_non_protective_custody_solitary_sessions"
)

HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual that is incarcerated. Session
defined as continuous period of time at a given housing unit type, using Recidiviz schema mappings. Collapses over all 
solitary confinement types, excluding protective custody."""

HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_QUERY_TEMPLATE = f"""
    WITH housing_unit_type_collapsed_solitary_spans AS (
        SELECT
            * EXCEPT (housing_unit_type),
            IF(
                CONTAINS_SUBSTR(housing_unit_type, 'SOLITARY_CONFINEMENT'),
                'SOLITARY_CONFINEMENT',
                housing_unit_type
            ) AS housing_unit_type_collapsed_solitary,
        FROM `{{project_id}}.{{sessions_dataset}}.housing_unit_type_sessions_materialized`
    )
    {aggregate_adjacent_spans(
        table_name="housing_unit_type_collapsed_solitary_spans",
        attribute="housing_unit_type_collapsed_solitary",
        end_date_field_name='end_date_exclusive'
    )}
"""

HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_NAME,
    view_query_template=HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_QUERY_TEMPLATE,
    description=HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER.build_and_print()
