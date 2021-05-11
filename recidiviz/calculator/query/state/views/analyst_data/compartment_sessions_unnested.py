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
"""Sessionized view of each individual merged onto an array of dates at daily intervals,
 used to calculate person-based metrics such as population"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME = "compartment_sessions_unnested"

COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION = """Sessionized view of each individual merged onto an array of dates at daily intervals, used to calculate person-based metrics such as population"""

COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        sessions.state_code,
        sessions.person_id,
        sessions.start_date,
        sessions.end_date,
        sessions.session_id,
        sessions.dataflow_session_id_start,
        sessions.dataflow_session_id_end,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        sessions.session_length_days as session_length_days,
        population_date,
        DATE_DIFF(population_date,sessions.start_date,day) as time_served_in_compartment
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions,
    UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 9 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
    WHERE population_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
    """

COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER.build_and_print()
