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
from typing import List, Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.task_eligibility.dataset_config import (
    TASK_COMPLETION_EVENTS_DATASET_ID,
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
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
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


ALL_COMPLETION_EVENTS_DESCRIPTION_TEMPLATE = """
This view contains all task completion events for all people across all states. It 
unions the results of all the completion event views in `{completion_event_dataset_id}`.
"""
ALL_COMPLETION_EVENTS_VIEW_ID = "all_completion_events"

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


def _get_eligiblity_spans_unioned_view_builders() -> Sequence[BigQueryViewBuilder]:
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
            UnionAllBigQueryViewBuilder(
                dataset_id=dataset_id,
                view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_TASKS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_spans_dataset_id=dataset_id,
                ),
                parent_view_builders=task_view_builders,
            )
        )

    if not state_specific_unioned_view_builders:
        raise ValueError(
            "Found no defined SingleTaskEligibilityBigQueryViews defined for any state."
        )

    return state_specific_unioned_view_builders + [
        UnionAllBigQueryViewBuilder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
            description=ALL_TASKS_ALL_STATES_DESCRIPTION,
            parent_view_builders=state_specific_unioned_view_builders,
        )
    ]


def _get_criteria_unioned_view_builders() -> Sequence[BigQueryViewBuilder]:
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

    def get_criteria_select_statement(vb: TaskCriteriaBigQueryViewBuilder) -> str:
        # TODO(#15132): Select columns explicitly to be tolerant of different column orders
        return f"SELECT '{vb.criteria_name}' AS criteria_name, *"

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
            UnionAllBigQueryViewBuilder(
                dataset_id=dataset_id,
                view_id=CRITERIA_SPANS_ALL_STATE_SPECIFIC_CRITERIA_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_CRITERIA_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_criteria_dataset_id=dataset_id,
                ),
                parent_view_builders=criteria_view_builders,
                builder_to_select_statement=get_criteria_select_statement,
            )
        )

    dataset_id = "task_eligibility_criteria_general"
    subpart_unioned_view_builders.append(
        UnionAllBigQueryViewBuilder(
            dataset_id=dataset_id,
            view_id=CRITERIA_SPANS_ALL_GENERAL_CRITERIA_VIEW_ID,
            description=StrictStringFormatter().format(
                ALL_CRITERIA_GENERAL_DESCRIPTION_TEMPLATE,
                general_criteria_dataset_id=dataset_id,
            ),
            parent_view_builders=general_builders,
            builder_to_select_statement=get_criteria_select_statement,
        )
    )

    return subpart_unioned_view_builders + [
        UnionAllBigQueryViewBuilder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=ALL_CRITERIA_VIEW_ID,
            description=ALL_CRITERIA_DESCRIPTION,
            parent_view_builders=subpart_unioned_view_builders,
        )
    ]


def _get_candidate_population_unioned_view_builders() -> Sequence[BigQueryViewBuilder]:
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

    def get_population_select_statement(
        vb: TaskCandidatePopulationBigQueryViewBuilder,
    ) -> str:
        # TODO(#15132): Select columns explicitly to be tolerant of different column orders
        return f"SELECT '{vb.population_name}' AS population_name, *"

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
            UnionAllBigQueryViewBuilder(
                dataset_id=dataset_id,
                view_id=POPULATION_SPANS_ALL_STATE_SPECIFIC_POPULATIONS_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_POPULATIONS_STATE_SPECIFIC_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_populations_dataset_id=dataset_id,
                ),
                parent_view_builders=population_view_builders,
                builder_to_select_statement=get_population_select_statement,
            )
        )

    dataset_id = "task_eligibility_candidates_general"
    subpart_unioned_view_builders.append(
        UnionAllBigQueryViewBuilder(
            dataset_id=dataset_id,
            view_id=POPULATION_SPANS_ALL_GENERAL_POPULATIONS_VIEW_ID,
            description=StrictStringFormatter().format(
                ALL_POPULATIONS_GENERAL_DESCRIPTION_TEMPLATE,
                general_population_dataset_id=dataset_id,
            ),
            parent_view_builders=general_builders,
            builder_to_select_statement=get_population_select_statement,
        )
    )

    return subpart_unioned_view_builders + [
        UnionAllBigQueryViewBuilder(
            dataset_id=TASK_ELIGIBILITY_DATASET_ID,
            view_id=ALL_POPULATIONS_VIEW_ID,
            description=ALL_POPULATIONS_DESCRIPTION,
            parent_view_builders=subpart_unioned_view_builders,
        )
    ]


def get_completion_events_unioned_view_builder() -> UnionAllBigQueryViewBuilder:
    view_collector = TaskCompletionEventBigQueryViewCollector()

    def get_completion_event_select_statement(
        vb: TaskCompletionEventBigQueryViewBuilder,
    ) -> str:
        # TODO(#15132): Select columns explicitly to be tolerant of different column orders
        return f"SELECT '{vb.completion_event_type}' AS completion_event_type, *"

    return UnionAllBigQueryViewBuilder(
        dataset_id=TASK_COMPLETION_EVENTS_DATASET_ID,
        view_id=ALL_COMPLETION_EVENTS_VIEW_ID,
        description=StrictStringFormatter().format(
            ALL_COMPLETION_EVENTS_DESCRIPTION_TEMPLATE,
            completion_event_dataset_id=TASK_COMPLETION_EVENTS_DATASET_ID,
        ),
        parent_view_builders=view_collector.collect_view_builders(),
        builder_to_select_statement=get_completion_event_select_statement,
    )


def get_unioned_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of view builders for views that union together results from
    disparate criteria, population, completion event, and task eligibility spans views.
    """

    return [
        *_get_criteria_unioned_view_builders(),
        *_get_candidate_population_unioned_view_builders(),
        get_completion_events_unioned_view_builder(),
        *_get_eligiblity_spans_unioned_view_builders(),
    ]
