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
"""Current total population by compartment, outflow compartment, and months until transition will be made"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.dataset_config import POPULATION_PROJECTION_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REMAINING_SENTENCES_VIEW_NAME = 'remaining_sentences'

REMAINING_SENTENCES_VIEW_DESCRIPTION = \
    """"Current total population by compartment, outflow compartment, and months until transition will be made"""

REMAINING_SENTENCES_QUERY_TEMPLATE = \
    """
    WITH distribution_cte AS (
        SELECT
            run_date,
            state_code,
            compartment,
            outflow_to,
            total_population/SUM(total_population) OVER(PARTITION BY run_date, compartment) as pct_outflow
        FROM
          (
          SELECT
            run_date,
            state_code,
            compartment,
            outflow_to,
            sum(total_population) total_population,
          FROM `{project_id}.{population_projection_dataset}.population_transitions`
          WHERE compartment LIKE 'INCARCERATION%'
          GROUP BY 1,2,3,4
          ORDER BY 1,2,3,4
          )
    ),
    cte AS (
        SELECT
            sessions.person_id,
            sessions.state_code,
            sessions.session_id,
            CONCAT(sessions.compartment_level_1, ' - ', sessions.compartment_level_2) as compartment,
            sessions.gender,
            sessions.start_date AS session_start_date,
            sessions.end_date AS sessions_end_date,
            sentences.sentence_start_date,
            sentences.sentence_completion_date,
            sentences.projected_completion_date_max,
            sentences.parole_eligibility_date,
            CASE
                WHEN sessions.outflow_to_level_1 = 'RELEASE' THEN 'RELEASE - FULL'
                ELSE CONCAT(sessions.outflow_to_level_1, ' - ', sessions.outflow_to_level_2)
            END as outflow_to,
            COALESCE(dist.pct_outflow,1) pct_outflow,
            run_date_array.run_date,
            CASE 
                WHEN sessions.compartment_level_1 = 'INCARCERATION' and not run_date_array.run_date < parole_eligibility_date 
                    THEN CEILING(DATE_DIFF(projected_completion_date_max, run_date_array.run_date, DAY)/30)
                WHEN sessions.compartment_level_1 = 'INCARCERATION' and run_date_array.run_date < parole_eligibility_date 
                    THEN CEILING(DATE_DIFF(parole_eligibility_date, run_date_array.run_date, DAY)/30)
                WHEN sessions.compartment_level_1 = 'SUPERVISION' 
                    THEN CEILING(DATE_DIFF(projected_completion_date_max, run_date_array.run_date, DAY)/30)
            END AS compartment_duration
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`  sessions
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sentences_materialized`  sentences
          ON sentences.person_id = sessions.person_id
          and sentences.session_id = sessions.session_id
        JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_date_array
          ON run_date_array.run_date BETWEEN start_date AND coalesce(end_date, '9999-01-01')
        LEFT JOIN distribution_cte dist
          ON dist.run_date = run_date_array.run_date
          AND dist.compartment = compartment
          AND dist.state_code = sessions.state_code
        WHERE (sessions.compartment_level_1 = 'INCARCERATION' or sessions.compartment_level_1 = 'SUPERVISION')
            AND (sessions.outflow_to_level_1 is not NULL)
            AND (sessions.compartment_level_2 != 'OTHER')
            AND (sessions.outflow_to_level_2 != 'OTHER')

    )
    SELECT
        state_code,
        run_date,
        compartment,
        outflow_to,
        compartment_duration,
        gender,
        CAST(ROUND(SUM(pct_outflow)) AS INT64) AS total_population
    FROM cte
    WHERE state_code = 'US_ID'
        AND gender IN ('FEMALE', 'MALE')
        AND compartment_duration > 0
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1,2,3,4,5,6
    """

REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=POPULATION_PROJECTION_DATASET,
    view_id=REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=REMAINING_SENTENCES_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    population_projection_dataset=POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
