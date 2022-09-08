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
"""Utils for creating candidate population view builders"""
from typing import List

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)


def state_agnostic_supervision_candidate_population_query(
    *, additional_filters: List[str]
) -> str:
    """Returns a query with the supervised population as tracked by `dataflow_sessions`
    with any additional filter conditions applied to the query WHERE clause."""

    # Format the filtering query fragment with 1 condition per indented line
    filter_string = "\n\t".join(
        [f"AND {filter_str}" for filter_str in additional_filters]
    )

    return f"""
SELECT DISTINCT
    state_code,
    person_id,
    start_date,
    -- Convert end_date from inclusive to exclusive
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
FROM `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized`,
UNNEST(session_attributes) AS attr
WHERE attr.compartment_level_1 = "SUPERVISION"
    {filter_string}
 """


def state_agnostic_supervision_candidate_population_view_builder(
    population_name: str, description: str, additional_filters: List[str]
) -> StateAgnosticTaskCandidatePopulationBigQueryViewBuilder:
    """Returns a state agnostic candidate population view builder representing the
    supervised population with additional filter conditions applied. The |additional_filters|
    argument should be a list of query fragments that can be appended to the supervision
    query to reduce the population by certain characteristics such as legal status."""
    query_template = state_agnostic_supervision_candidate_population_query(
        additional_filters=additional_filters
    )
    return StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
        population_name=population_name,
        population_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
    )
