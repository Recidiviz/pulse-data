# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Spans of time for which a person is a given age"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_AGE_SESSIONS_VIEW_NAME = "person_age_sessions"

PERSON_AGE_SESSIONS_VIEW_DESCRIPTION = """
Spans of time for which a person is a given age. This table contains sessions for each person bound by their birthdate. 
Rows are included for all ages between the first date that a person is in our population data and the current date. 
"""

PERSON_AGE_SESSIONS_QUERY_TEMPLATE = """
    WITH start_end_date_per_person AS
    -- This cte pulls the first date that a person appears in our data as well as the last day of available data
    (
    SELECT
        person_id,
        state_code,
        last_day_of_data,
        MIN(start_date) AS first_population_date,
        MAX(COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), DATE_ADD(last_day_of_data, INTERVAL 1 YEAR))) AS last_population_date,        
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`
    GROUP BY 1,2,3
    )
    SELECT 
        person_id,
        state_code,
        birthdate,
        start_date,
        DATE_ADD(start_date, INTERVAL 1 YEAR) AS end_date_exclusive,
        DATE_DIFF(start_date, birthdate, YEAR) AS age,
    FROM `{project_id}.{sessions_dataset}.person_demographics_materialized`
    JOIN start_end_date_per_person
        USING(person_id, state_code),
    -- Generate an array for all days between a person's birthdate and year from the current date and these for all 
    -- age sessions that start during or after a person first entering the system     
    UNNEST(GENERATE_DATE_ARRAY(birthdate, DATE_ADD(last_day_of_data, INTERVAL 1 YEAR), INTERVAL 1 YEAR)) AS start_date
    WHERE DATE_ADD(start_date, INTERVAL 1 YEAR)>=first_population_date
        AND start_date<=last_population_date
"""
PERSON_AGE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=PERSON_AGE_SESSIONS_VIEW_NAME,
    view_query_template=PERSON_AGE_SESSIONS_QUERY_TEMPLATE,
    description=PERSON_AGE_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_AGE_SESSIONS_VIEW_BUILDER.build_and_print()
