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
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Sequence

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
    SimpleBigQueryViewBuilder,
)
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_DATASET_ID,
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_collector import (
    TaskCandidatePopulationBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_collector import (
    TaskCriteriaBigQueryViewCollector,
)
from recidiviz.utils.string import StrictStringFormatter

SELECT_STATEMENT_TEMPLATE = """
SELECT * FROM `{{project_id}}.{{{dataset_id}_dataset}}.{view_id}`
"""

CRITERIA_SELECT_STATEMENT_TEMPLATE = """
SELECT '{criteria_name}' AS criteria_name, * FROM `{{project_id}}.{{{dataset_id}_dataset}}.{view_id}`
"""

POPULATION_SELECT_STATEMENT_TEMPLATE = """
SELECT '{population_name}' AS population_name, * FROM `{{project_id}}.{{{dataset_id}_dataset}}.{view_id}`
"""

ALL_CRITERIA_STATE_SPECIFIC_DESCRIPTION_TEMPLATE = """
This view contains all criteria spans for {state_code} criteria with state-specific
underlying logic. It unions the results of all state-specific single-criteria
views for this state, aka all the other views in this dataset 
(`{state_specific_criteria_dataset_id}`).
"""
CRITERIA_SPANS_ALL_STATE_SPECIFIC_CRITERIA_VIEW_ID = "all_state_specific_criteria"


ALL_CRITERIA_GENERAL_DESCRIPTION_TEMPLATE = """
This view contains all criteria spans for that do not use any state-specific logic. It
unions the results of all general single-criteria views for this state,
aka all the other views in this dataset (`{general_criteria_dataset_id}`).
"""
CRITERIA_SPANS_ALL_GENERAL_CRITERIA_VIEW_ID = "all_general_criteria"


ALL_CRITERIA_DESCRIPTION = """
This view contains all criteria spans used in any for task elibility spans view. It 
unions the results of all single-state `all_state_specific_criteria` views (e.g. 
`task_eligibility_us_xx.all_state_specific_criteria`) as well as the 
`all_general_criteria` view.
"""

ALL_CRITERIA_VIEW_ID = "all_criteria"


ALL_POPULATIONS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE = """
This view contains all candidate population spans for {state_code} populations with
state-specific underlying logic. It unions the results of all state-specific
single-population views for this state, aka all the other views in this dataset 
(`{state_specific_populations_dataset_id}`).
"""
POPULATION_SPANS_ALL_STATE_SPECIFIC_POPULATIONS_VIEW_ID = (
    "all_state_specific_candidate_populations"
)


ALL_POPULATIONS_GENERAL_DESCRIPTION_TEMPLATE = """
This view contains all candidate population spans for that do not use any state-specific
logic. It unions the results of all general single-population views for this state,
aka all the other views in this dataset (`{general_population_dataset_id}`).
"""
POPULATION_SPANS_ALL_GENERAL_POPULATIONS_VIEW_ID = "all_general_candidate_populations"


ALL_POPULATIONS_DESCRIPTION = """
This view contains all criteria spans used in any for task elibility spans view. It 
unions the results of all single-state `all_state_specific_criteria` views (e.g. 
`task_eligibility_us_xx.all_state_specific_criteria`) as well as the 
`all_general_criteria` view.
"""

ALL_POPULATIONS_VIEW_ID = "all_candidate_populations"


ALL_TASKS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE = """
This view contains all task eligiblity spans for {state_code} tasks. It unions the 
results of all single-task views for this state, aka all the other views in this 
dataset (`{state_specific_spans_dataset_id}`).
"""

ALL_TASKS_ALL_STATES_DESCRIPTION = """
This view contains all task eligiblity spans for tasks across states. It unions the 
results of all single-state `all_tasks` views (e.g. `task_eligibility_us_xx.all_tasks`).
"""

TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID = "all_tasks"


def _get_eligiblity_spans_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders containing:
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

        dataset_id = task_eligibility_spans_state_specific_dataset(state_code)
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
        raise ValueError(
            "Found no defined SingleTaskEligibilityBigQueryViews defined for any state."
        )

    return state_specific_unioned_view_builders + [
        _build_union_all_view_builder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
            description=ALL_TASKS_ALL_STATES_DESCRIPTION,
            view_builders=state_specific_unioned_view_builders,
        )
    ]


def _get_criteria_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders containing:
    a) one view per state, which unions criteria spans with state-specific logic for
     that state into a single 'all_state_specific_criteria' view for that state, and
    b) one view that unions all state-agnostic criteria spans into a single
        `all_general_criteria` view.
    b) one view that unions all the data from the views listed above into one
        'all_criteria' view.
    """
    view_collector = TaskCriteriaBigQueryViewCollector()
    general_builders = []
    state_specific_builders = defaultdict(list)
    for view_builder in view_collector.collect_view_builders():
        if isinstance(view_builder, StateAgnosticTaskCriteriaBigQueryViewBuilder):
            general_builders.append(view_builder)
        elif isinstance(view_builder, StateSpecificTaskCriteriaBigQueryViewBuilder):
            state_specific_builders[view_builder.state_code].append(view_builder)
        else:
            raise ValueError(f"Unexpected view builder type: {view_builder}")

    def select_statement_format_args_fn(
        vb: TaskCriteriaBigQueryViewBuilder,
    ) -> Dict[str, str]:
        return {"criteria_name": vb.criteria_name}

    subpart_unioned_view_builders = []
    for (
        state_code,
        criteria_view_builders,
    ) in state_specific_builders.items():
        if not criteria_view_builders:
            raise ValueError(
                f"Found no defined StateSpecificTaskCriteriaBigQueryViewBuilder for "
                f"[{state_code}] - is there an empty module for this state?"
            )

        dataset_id = f"task_eligibility_criteria_{state_code.value.lower()}"
        subpart_unioned_view_builders.append(
            _build_union_all_view_builder(
                dataset_id=dataset_id,
                view_id=CRITERIA_SPANS_ALL_STATE_SPECIFIC_CRITERIA_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_CRITERIA_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_criteria_dataset_id=dataset_id,
                ),
                view_builders=criteria_view_builders,
                select_statement_template=CRITERIA_SELECT_STATEMENT_TEMPLATE,
                select_statement_format_args_fn=select_statement_format_args_fn,
            )
        )

    dataset_id = "task_eligibility_criteria_general"
    subpart_unioned_view_builders.append(
        _build_union_all_view_builder(
            dataset_id=dataset_id,
            view_id=CRITERIA_SPANS_ALL_GENERAL_CRITERIA_VIEW_ID,
            description=StrictStringFormatter().format(
                ALL_CRITERIA_GENERAL_DESCRIPTION_TEMPLATE,
                general_criteria_dataset_id=dataset_id,
            ),
            view_builders=general_builders,
            select_statement_template=CRITERIA_SELECT_STATEMENT_TEMPLATE,
            select_statement_format_args_fn=select_statement_format_args_fn,
        )
    )

    return subpart_unioned_view_builders + [
        _build_union_all_view_builder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=ALL_CRITERIA_VIEW_ID,
            description=ALL_CRITERIA_DESCRIPTION,
            view_builders=subpart_unioned_view_builders,
        )
    ]


def _get_candidate_population_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders containing:
    a) one view per state, which unions candidate population spans with state-specific
     logic for that state into a single 'all_state_specific_candidate_populations' view for that
     state, and
    b) one view that unions all state-agnostic candidate population spans into a single
        `all_general_candidate_populations` view.
    b) one view that unions all the data from the views listed above into one
        'all_candidate_populations' view.
    """
    view_collector = TaskCandidatePopulationBigQueryViewCollector()
    general_builders = []
    state_specific_builders = defaultdict(list)
    for view_builder in view_collector.collect_view_builders():
        if isinstance(
            view_builder, StateAgnosticTaskCandidatePopulationBigQueryViewBuilder
        ):
            general_builders.append(view_builder)
        elif isinstance(
            view_builder, StateSpecificTaskCandidatePopulationBigQueryViewBuilder
        ):
            state_specific_builders[view_builder.state_code].append(view_builder)
        else:
            raise ValueError(f"Unexpected view builder type: {view_builder}")

    def select_statement_format_args_fn(
        vb: TaskCandidatePopulationBigQueryViewBuilder,
    ) -> Dict[str, str]:
        return {"population_name": vb.population_name}

    subpart_unioned_view_builders = []
    for (
        state_code,
        population_view_builders,
    ) in state_specific_builders.items():
        if not population_view_builders:
            raise ValueError(
                f"Found no defined StateSpecificTaskCandidatePopulationBigQueryViewBuilder for "
                f"[{state_code}] - is there an empty module for this state?"
            )

        dataset_id = f"task_eligibility_candidates_{state_code.value.lower()}"
        subpart_unioned_view_builders.append(
            _build_union_all_view_builder(
                dataset_id=dataset_id,
                view_id=POPULATION_SPANS_ALL_STATE_SPECIFIC_POPULATIONS_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_POPULATIONS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_populations_dataset_id=dataset_id,
                ),
                view_builders=population_view_builders,
                select_statement_template=POPULATION_SELECT_STATEMENT_TEMPLATE,
                select_statement_format_args_fn=select_statement_format_args_fn,
            )
        )

    dataset_id = "task_eligibility_candidates_general"
    subpart_unioned_view_builders.append(
        _build_union_all_view_builder(
            dataset_id=dataset_id,
            view_id=POPULATION_SPANS_ALL_GENERAL_POPULATIONS_VIEW_ID,
            description=StrictStringFormatter().format(
                ALL_POPULATIONS_GENERAL_DESCRIPTION_TEMPLATE,
                general_population_dataset_id=dataset_id,
            ),
            view_builders=general_builders,
            select_statement_template=POPULATION_SELECT_STATEMENT_TEMPLATE,
            select_statement_format_args_fn=select_statement_format_args_fn,
        )
    )

    return subpart_unioned_view_builders + [
        _build_union_all_view_builder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=ALL_POPULATIONS_VIEW_ID,
            description=ALL_POPULATIONS_DESCRIPTION,
            view_builders=subpart_unioned_view_builders,
        )
    ]


def get_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders for views that union together results from
    disparate criteria, population, and task eligibility spans views.
    """

    return [
        *_get_criteria_unioned_view_builders(),
        *_get_candidate_population_unioned_view_builders(),
        *_get_eligiblity_spans_unioned_view_builders(),
    ]


def _build_union_all_view_builder(
    dataset_id: str,
    view_id: str,
    description: str,
    view_builders: Sequence[BigQueryViewBuilderType],
    select_statement_template: str = SELECT_STATEMENT_TEMPLATE,
    select_statement_format_args_fn: Optional[
        Callable[[BigQueryViewBuilderType], Dict[str, str]]
    ] = None,
) -> BigQueryViewBuilder:
    """Returns a view at address (dataset_id, view_id) that unions together 'SELECT *'
    queries against the materialized location of each of the views in |view_builders|.

    If a custom |select_statement_template| is provided, that query will be used instead
    of a simple SELECT * query. The user may also define a function that provides
    additional format args for that custom template.
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

        additional_format_args = (
            select_statement_format_args_fn(vb)
            if select_statement_format_args_fn
            else {}
        )
        select_queries.append(
            StrictStringFormatter().format(
                select_statement_template,
                dataset_id=vb.materialized_address.dataset_id,
                view_id=vb.materialized_address.table_id,
                **additional_format_args,
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
