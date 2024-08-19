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
 housing unit type, using Recidiviz schema mappings"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

HOUSING_UNIT_TYPE_SESSIONS_VIEW_NAME = "housing_unit_type_sessions"

HOUSING_UNIT_TYPE_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual that is incarcerated. Session
defined as continuous period of time at a given housing unit type, using Recidiviz schema mappings"""

HOUSING_UNIT_TYPE_SESSIONS_QUERY_TEMPLATE = f"""
    WITH sub_sessions_cte AS
    (
    SELECT
        state_code,
        person_id,
        housing_unit_type,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
    AND state_code != 'US_TN'

    UNION ALL

    SELECT 
        state_code,
        person_id,
        housing_unit_type,
        start_date,
        end_date_exclusive
    FROM `{{project_id}}.{{analyst_dataset}}.us_tn_segregation_stays_materialized` 
    )
    ,
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='sub_sessions_cte',
                       attribute='housing_unit_type',
                       session_id_output_name='housing_unit_type_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    SELECT
        *,
    FROM sessionized_cte
"""

HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=HOUSING_UNIT_TYPE_SESSIONS_VIEW_NAME,
    view_query_template=HOUSING_UNIT_TYPE_SESSIONS_QUERY_TEMPLATE,
    description=HOUSING_UNIT_TYPE_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER.build_and_print()
