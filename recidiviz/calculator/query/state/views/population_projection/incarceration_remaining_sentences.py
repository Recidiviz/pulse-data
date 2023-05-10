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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    POPULATION_PROJECTION_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REMAINING_SENTENCES_VIEW_NAME = "incarceration_remaining_sentences"

REMAINING_SENTENCES_VIEW_DESCRIPTION = """"Current prison total population by compartment, outflow compartment, and months until transition will be made"""

REMAINING_SENTENCES_QUERY_TEMPLATE = """
    WITH incarceration_distribution_cte AS (
        SELECT
            run_date,
            state_code,
            compartment,
            outflow_to,
            total_population/SUM(total_population) OVER(PARTITION BY run_date, state_code, compartment) as pct_outflow
        FROM
          (
          SELECT
            run_date,
            state_code,
            compartment,
            outflow_to,
            SUM(total_population) total_population,
          FROM `{project_id}.{population_projection_dataset}.population_transitions_materialized`
          WHERE compartment LIKE 'INCARCERATION%'
            -- Union the rider transitions at the end
            AND compartment NOT IN ('INCARCERATION - TREATMENT_IN_PRISON', 'INCARCERATION - PAROLE_BOARD_HOLD')
            -- Do not include transitions to an unknown compartment (most often indicates dropped data)
            AND outflow_to != 'INTERNAL_UNKNOWN - INTERNAL_UNKNOWN'
          GROUP BY 1,2,3,4
          )
    ),
    incarceration_cte AS (
        SELECT
            sessions.person_id,
            sessions.state_code,
            sessions.session_id,
            CASE
              WHEN sessions.compartment = 'INCARCERATION - GENERAL' AND sessions.previously_incarcerated THEN 'INCARCERATION - RE-INCARCERATION'
              ELSE sessions.compartment
            END AS compartment,
            sessions.gender,
            sessions.start_date AS session_start_date,
            sessions.end_date AS sessions_end_date,
            sentences.effective_date AS sentence_start_date,
            sentences.completion_date AS sentence_completion_date,
            COALESCE(sentences.projected_completion_date_max, sentences.projected_completion_date_min) AS projected_max_release_date,
            sentences.parole_eligibility_date,
            run_date_array.run_date,
            CASE
                WHEN (parole_eligibility_date is null) or (parole_eligibility_date <= run_date_array.run_date)
                    THEN FLOOR(
                        -- Estimate the amount of time between the parole eligibility date and the min/max projected
                        -- release date that each client will be released. This logic could be improved and use more
                        -- historical state-specific info
                        DATE_DIFF(run_date_array.run_date,
                        DATE_ADD(
                          sentences.parole_eligibility_date,
                          INTERVAL CAST(FLOOR(0.33 * DATE_DIFF(
                            COALESCE(sentences.projected_completion_date_max, sentences.projected_completion_date_min),
                            sentences.parole_eligibility_date,
                            DAY
                          )) AS INT64) DAY
                        ),
                        DAY) / 30
                    )
                WHEN (parole_eligibility_date is not null) and (run_date_array.run_date < parole_eligibility_date)
                    THEN FLOOR(DATE_DIFF(parole_eligibility_date, run_date_array.run_date, DAY)/30)
            END AS compartment_duration,
        FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
        JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_date_array
            ON run_date_array.run_date BETWEEN sessions.start_date AND COALESCE(end_date, '9999-01-01')
        LEFT JOIN
            `{project_id}.{sessions_dataset}.sentence_spans_materialized` ss
            ON ss.person_id = sessions.person_id
            AND ss.state_code = sessions.state_code
            AND run_date_array.run_date BETWEEN ss.start_date
                AND COALESCE(DATE_SUB(ss.end_date, INTERVAL 1 DAY), '9999-01-01'),
        UNNEST(sentence_imposed_group_id_array) AS sentence_imposed_group_id
        LEFT JOIN
            `{project_id}.{sessions_dataset}.sentence_imposed_group_summary_materialized` sentences
            ON sentences.person_id = ss.person_id
            AND sentences.state_code = ss.state_code
            AND sentences.sentence_imposed_group_id = sentence_imposed_group_id
        WHERE sessions.compartment LIKE 'INCARCERATION%'
          -- Union these short-term incarceration transitions at the end
          AND compartment NOT IN ('INCARCERATION - TREATMENT_IN_PRISON', 'INCARCERATION - PAROLE_BOARD_HOLD')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY sessions.person_id, sessions.state_code, sessions.session_id, run_date_array.run_date
            ORDER BY COALESCE(sentences.projected_completion_date_max, sentences.projected_completion_date_min) DESC
        ) = 1
    )

    SELECT
        state_code,
        run_date,
        compartment,
        incarceration_distribution_cte.outflow_to,
        incarceration_cte.compartment_duration,
        incarceration_cte.gender,
        SUM(incarceration_distribution_cte.pct_outflow) AS total_population
    FROM incarceration_cte
    JOIN incarceration_distribution_cte
      USING (state_code, compartment, run_date)
    WHERE
        incarceration_cte.gender IN ('FEMALE', 'MALE')
        AND incarceration_cte.compartment_duration >= 0
    GROUP BY 1,2,3,4,5,6
    """

INCARCERATION_REMAINING_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=POPULATION_PROJECTION_DATASET,
    view_id=REMAINING_SENTENCES_VIEW_NAME,
    view_query_template=REMAINING_SENTENCES_QUERY_TEMPLATE,
    description=REMAINING_SENTENCES_VIEW_DESCRIPTION,
    population_projection_dataset=POPULATION_PROJECTION_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_REMAINING_SENTENCES_VIEW_BUILDER.build_and_print()
