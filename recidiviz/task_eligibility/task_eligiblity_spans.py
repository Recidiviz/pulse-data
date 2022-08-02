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
"""Defines a function that returns a list of view builders that union all task
eligiblity spans into central locations.
"""

import logging
from typing import List, Sequence

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.utils.string import StrictStringFormatter

# TODO(#14309): Remove this variable once we have defined a task for at least one
#  state.
TEMP_EMPTY_ALL_TASKS_TEMPLATE = """
SELECT 
    'FAKE_TASK' AS task_name,
    0 AS person_id,
    NULL AS start_date,
    NULL AS end_date,
    True AS is_eligible,
    [] AS ineligible_criteria,
    [] as reasons;
"""

ALL_TASKS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE = """
This view contains all task eligiblity spans for {state_code} tasks. It unions the 
results of all single-task views for this state, aka all the other views in this 
dataset (`{state_specific_spans_dataset_id}`).
"""

ALL_TASKS_ALL_STATES_DESCRIPTION = """
This view contains all task eligiblity spans for tasks across states. It unions the 
results of all single-state `all_tasks` views (e.g. `task_eligibility_us_xx.all_tasks`).
"""
TASK_ELIGIBILITY_DATASET_ID = "task_eligibility"
TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID = "all_tasks"


# TODO(#14309): Write tests for this function
def get_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders containing
    a) one view per state, which unions task eligiblity spans for that state into
        a single 'all_tasks' view for that state, and
    b) one view that unions all the data from the state-specific 'all_tasks' views
    into one place.
    """
    state_specific_unioned_view_builders = []
    view_collector = SingleTaskEligibilityBigQueryViewCollector()
    for (
        state_code,
        task_view_builders,
    ) in view_collector.collect_view_builders_by_state().items():
        if not task_view_builders:
            raise ValueError(
                f"Found no defined SingleTaskEligibilityBigQueryView for "
                f"[{state_code}] - is there an empty module for this state?"
            )

        dataset_id = f"task_eligibility_spans_{state_code.value.lower()}"
        state_specific_unioned_view_builders.append(
            _build_union_all_view_builder(
                dataset_id=dataset_id,
                view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_TASKS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_spans_dataset_id=dataset_id,
                ),
                view_builders=task_view_builders,
            )
        )

    if not state_specific_unioned_view_builders:
        # TODO(#14317): Remove this clause once we have defined a task for at least one
        #  state.
        logging.error("No task views defined for any state")
        return [
            SimpleBigQueryViewBuilder(
                dataset_id=TASK_ELIGIBILITY_DATASET_ID,
                view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
                description=ALL_TASKS_ALL_STATES_DESCRIPTION,
                view_query_template=TEMP_EMPTY_ALL_TASKS_TEMPLATE,
            )
        ]

    return state_specific_unioned_view_builders + [
        _build_union_all_view_builder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
            description=ALL_TASKS_ALL_STATES_DESCRIPTION,
            view_builders=state_specific_unioned_view_builders,
        )
    ]


SELECT_STATEMENT_TEMPLATE = """
SELECT * FROM `{{project_id}}.{{{dataset_id}_dataset}}.{view_id}`
"""


def _build_union_all_view_builder(
    dataset_id: str,
    view_id: str,
    description: str,
    view_builders: Sequence[BigQueryViewBuilder],
) -> BigQueryViewBuilder:
    """Returns a view at address (dataset_id, view_id) that unions together 'SELECT *'
    queries against the materialized location of each of the views in |view_builders|.
    """
    if not view_builders:
        raise ValueError(
            f"Found no view builders to union for view `{dataset_id}.{view_id}`"
        )

    select_queries = []
    query_format_args = {}
    for vb in view_builders:
        if not vb.materialized_address:
            raise ValueError(f"Expected view [{vb.address}] to be materialized.")

        select_queries.append(
            StrictStringFormatter().format(
                SELECT_STATEMENT_TEMPLATE,
                dataset_id=vb.materialized_address.dataset_id,
                view_id=vb.materialized_address.table_id,
            )
        )
        query_format_args[f"{vb.dataset_id}_dataset"] = vb.dataset_id

    view_query_template = "UNION ALL".join(select_queries)

    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_id,
        view_id=view_id,
        description=description,
        view_query_template=view_query_template,
        should_materialize=True,
        materialized_address_override=None,
        projects_to_deploy=None,
        should_deploy_predicate=None,
        clustering_fields=None,
        **query_format_args,
    )
