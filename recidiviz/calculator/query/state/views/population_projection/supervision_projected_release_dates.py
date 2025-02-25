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
# =============================================================================
"""Projected supervision completion dates for each simulation run date"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_NAME = "supervision_projected_release_dates"

SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_DESCRIPTION = """Projected supervision completion dates for clients on
supervision used to compute the remaining sentence length for population projection."""

SUPERVISION_PROJECTED_RELEASE_DATES_QUERY_TEMPLATE = """
    SELECT 
        run_dates.run_date,
        sessions.state_code,
        sessions.person_id, 
        DATE_SUB(projected_completion_date_max, INTERVAL 1 DAY) AS projected_release_date
    FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
    INNER JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        ON run_dates.run_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, CURRENT_DATE)
    INNER JOIN `{project_id}.{sessions_dataset}.supervision_projected_completion_date_spans_materialized` sentences
        ON sessions.state_code = sentences.state_code
        AND sessions.person_id = sentences.person_id
        AND run_dates.run_date
            BETWEEN sentences.start_date
            AND COALESCE(DATE_SUB(sentences.end_date_exclusive, INTERVAL 1 DAY), CURRENT_DATE)
    WHERE
        -- Drop projected completion dates in the past to avoid negative remaining LOS
        run_dates.run_date < projected_completion_date_max
        -- Drop life sentences
        AND COALESCE(EXTRACT(YEAR FROM projected_completion_date_max), 2030) < 9999
    """

SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_NAME,
    view_query_template=SUPERVISION_PROJECTED_RELEASE_DATES_QUERY_TEMPLATE,
    description=SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id", "run_date"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_PROJECTED_RELEASE_DATES_VIEW_BUILDER.build_and_print()
