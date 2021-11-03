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
"""View counting daily distinct number of people on supervision within an officer's caseload over various rolling windows"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_NAME = (
    "supervision_population_by_officer_daily_windows"
)

SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_DESCRIPTION = "Captures supervision population as distinct number of people on supervision daily and over rolling date windows"

SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH population_date_array AS
    (
        SELECT DISTINCT 
            date_of_supervision, 
            EXTRACT(YEAR from date_of_supervision) year, 
            EXTRACT(MONTH from date_of_supervision) month,
            rolling_window_days
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR), CURRENT_DATE(), INTERVAL 1 DAY)) date_of_supervision,
            {rolling_window_days_dimension}
    )
    SELECT officers.state_code, pop.year, pop.month, pop.date_of_supervision as date, 
        supervising_officer_external_id,
        pop.rolling_window_days, 
        COUNT(DISTINCT officers.person_id) supervision_population,
        COUNT(DISTINCT IF(sessions.compartment_level_2 = 'PROBATION', sessions.person_id, NULL)) probation_population,
        COUNT(DISTINCT IF(sessions.compartment_level_2 = 'PAROLE', sessions.person_id, NULL)) parole_population,
    FROM population_date_array pop
    JOIN `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` officers
        ON pop.date_of_supervision BETWEEN officers.start_date AND COALESCE(DATE_ADD(officers.end_date, INTERVAL rolling_window_days DAY), CURRENT_DATE())
    JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        ON officers.person_id = sessions.person_id
        AND pop.date_of_supervision BETWEEN sessions.start_date AND COALESCE(DATE_ADD(sessions.end_date, INTERVAL rolling_window_days DAY), CURRENT_DATE())
    GROUP BY state_code, pop.year, pop.month, pop.date_of_supervision, supervising_officer_external_id, rolling_window_days
    """

SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    rolling_window_days_dimension=bq_utils.unnest_rolling_window_days(),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER.build_and_print()
