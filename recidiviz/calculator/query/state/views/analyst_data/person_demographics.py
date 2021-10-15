# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Person level demographics - age, race, gender"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_DEMOGRAPHICS_VIEW_NAME = "person_demographics"

PERSON_DEMOGRAPHICS_VIEW_DESCRIPTION = (
    """Person level demographics - age, race, gender, external ID"""
)

PERSON_DEMOGRAPHICS_PRIORITIZED_ID_TYPES = (
    """('US_ID_DOC', 'US_MO_DOC', 'US_ND_SID', 'US_PA_INMATE')"""
)

PERSON_DEMOGRAPHICS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH race_or_ethnicity_cte AS 
    (
    SELECT 
        state_code,
        person_id,
        race as race_or_ethnicity,
    FROM `{project_id}.{base_dataset}.state_person_race`
    UNION ALL
    SELECT 
        state_code,
        person_id,
        ethnicity as race_or_ethnicity,
    FROM `{project_id}.{base_dataset}.state_person_ethnicity`
    )
    ,
    prioritized_race_ethnicity_cte AS
    (
    SELECT DISTINCT
        state_code,
        person_id,
        FIRST_VALUE(race_or_ethnicity) OVER (PARTITION BY state_code, person_id ORDER BY COALESCE(representation_priority, 100)) as prioritized_race_or_ethnicity,
    FROM race_or_ethnicity_cte
    LEFT JOIN `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
            USING (state_code, race_or_ethnicity)
    )
    ,
    prioritized_external_id_table AS
    (
    SELECT DISTINCT
        state_code,
        person_id,
        external_id AS prioritized_external_id,
        ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY external_id) AS id_number,
    FROM `{project_id}.{base_dataset}.state_person_external_id`
    WHERE id_type IN {prioritized_id_types}
    )
    SELECT 
        state_code,
        person_id,
        birthdate,
        gender,
        prioritized_race_or_ethnicity,
        prioritized_external_id,
    FROM `{project_id}.{base_dataset}.state_person`
    FULL OUTER JOIN prioritized_race_ethnicity_cte
        USING(state_code, person_id) 
    LEFT JOIN prioritized_external_id_table
        USING(state_code, person_id) 
    WHERE
        id_number = 1
    ORDER BY
        state_code,
        person_id
    """

PERSON_DEMOGRAPHICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=PERSON_DEMOGRAPHICS_QUERY_TEMPLATE,
    description=PERSON_DEMOGRAPHICS_VIEW_DESCRIPTION,
    prioritized_id_types=PERSON_DEMOGRAPHICS_PRIORITIZED_ID_TYPES,
    base_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
