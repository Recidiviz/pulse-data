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

"""A view which provides a person / day level comparison of annual sessions incarceration population to dataflow"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "session_incarceration_population_to_dataflow_disaggregated"
)

SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
    A view which provides a person / day level comparison of incarceration population on the first day of each year
    in dataflow vs sessions.
    """

SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH population_dates AS
    (
    SELECT 
        *
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 20 YEAR),
            DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 1 MONTH)) AS population_date
    )
    ,
    dataflow_population AS
    (
    SELECT DISTINCT
        state_code,
        population_date,
        person_id,
        1 AS in_dataflow
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized` metrics
    JOIN population_dates
        ON metrics.date_of_stay = population_dates.population_date
    )
    ,
    sessions_population AS
    (
    SELECT
        state_code,
        population_date,
        person_id,
        1 AS in_sessions
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
    JOIN population_dates 
        ON population_dates.population_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
    WHERE sessions.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')
    )
    SELECT 
        state_code,
        population_date,
        person_id,
        COALESCE(in_dataflow, 0) AS in_dataflow,
        COALESCE(in_sessions, 0) AS in_sessions,
    FROM dataflow_population
    FULL OUTER JOIN sessions_population
        USING(state_code, population_date, person_id)
    """

SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED.build_and_print()
