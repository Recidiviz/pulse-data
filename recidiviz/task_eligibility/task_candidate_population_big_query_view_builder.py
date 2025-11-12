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
"""Defines BigQueryViewBuilders that can be used to define single population span views.
These views are used as inputs to a task eligibility spans view.
"""

from typing import Optional, Union

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
)


def aggregate_adjacent_candidate_population_spans(query_template: str) -> str:
    """
    Wraps a candidate population query template to aggregate adjacent spans
    for the same person into a single span."""

    return f"""WITH candidate_population_query_base AS (
    {query_template.rstrip().rstrip(";")}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date
    FROM ({aggregate_adjacent_spans(table_name = 'candidate_population_query_base')})
"""


def _get_candidate_population_query_from_criteria_group(
    criteria_group: Union[
        StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
        StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
    ]
) -> str:
    return f"""
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
        FROM (
            {criteria_group.view_query_template}
        )
        WHERE
            meets_criteria
    """


def _get_criteria_query_from_candidate_population(
    view_query_template: str, population_name: str
) -> str:
    return f"""
        SELECT
            *,
            TRUE AS meets_criteria,
            "{population_name}" AS population_name,
            TO_JSON(STRUCT("{population_name}" AS population_name)) AS reason,
        FROM ({view_query_template})
    """


class StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
    SimpleBigQueryViewBuilder
):
    """A builder for a view that defines spans of time during which someone is part of
    a population that could be eligible for a particular task. This builder should only
    be used for views that contain state-specific logic that could not be applied
    generally as a population in multiple states (e.g. if it queries raw data).
    """

    def __init__(
        self,
        state_code: StateCode,
        population_name: str,
        population_spans_query_template: str,
        description: str,
        criteria_group: Optional[
            StateSpecificTaskCriteriaGroupBigQueryViewBuilder
        ] = None,
        **query_format_kwargs: str,
    ) -> None:
        if population_name.upper() != population_name:
            raise ValueError(f"Population name [{population_name}] must be upper case.")

        state_code_prefix = f"{state_code.value}_"
        if not population_name.startswith(state_code_prefix):
            raise ValueError(
                f"Found state-specific task candidate population [{population_name}] "
                f"whose name does not start with [{state_code_prefix}]."
            )
        view_id = population_name.removeprefix(state_code_prefix).lower()
        super().__init__(
            dataset_id=f"task_eligibility_candidates_{state_code.value.lower()}",
            view_id=view_id,
            description=description,
            view_query_template=aggregate_adjacent_candidate_population_spans(
                query_template=population_spans_query_template
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.population_name = population_name
        self.criteria_group = criteria_group

    @classmethod
    def from_criteria_group(
        cls,
        criteria_group: StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
        population_name: str,
        **query_format_kwargs: str,
    ) -> "StateSpecificTaskCandidatePopulationBigQueryViewBuilder":
        query_template = _get_candidate_population_query_from_criteria_group(
            criteria_group
        )

        return cls(
            state_code=criteria_group.state_code,
            population_name=population_name,
            population_spans_query_template=query_template,
            description=__doc__,
            criteria_group=criteria_group,
            **query_format_kwargs,
        )

    def get_descendant_criteria(self) -> set[TaskCriteriaBigQueryViewBuilder]:
        if self.criteria_group:
            return self.criteria_group.get_descendant_criteria()
        return set()

    def as_criteria(
        self,
        criteria_name: Optional[str] = None,
        **query_format_kwargs: str,
    ) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
        """Creates a StateSpecificTaskCriteriaBigQueryViewBuilder that uses this candidate
        population as its criteria."""

        name = criteria_name or self.population_name
        query_template = _get_criteria_query_from_candidate_population(
            self.view_query_template, self.population_name
        )

        return StateSpecificTaskCriteriaBigQueryViewBuilder(
            state_code=self.state_code,
            criteria_name=f"{self.state_code.value}_{name}",
            criteria_spans_query_template=query_template,
            description=__doc__,
            reasons_fields=[
                ReasonsField(
                    name="population_name",
                    type=bigquery.enums.StandardSqlTypeNames.STRING,
                    description="Candidate population name",
                ),
            ],
            meets_criteria_default=False,
            **query_format_kwargs,
        )


class StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
    SimpleBigQueryViewBuilder
):
    """A builder for a view that defines spans of time during which someone is part of
    a population that could be eligible for a particular task. This builder should only
    be used for views that contain NO state-specific logic that could not be applied
    generally as a population in multiple states.
    """

    def __init__(
        self,
        population_name: str,
        population_spans_query_template: str,
        description: str,
        criteria_group: Optional[
            StateAgnosticTaskCriteriaGroupBigQueryViewBuilder
        ] = None,
        # All keyword args must have string values
        **query_format_kwargs: str,
    ) -> None:
        if population_name.upper() != population_name:
            raise ValueError(f"Population name [{population_name}] must be upper case.")
        super().__init__(
            dataset_id="task_eligibility_candidates_general",
            view_id=population_name.lower(),
            description=description,
            view_query_template=aggregate_adjacent_candidate_population_spans(
                query_template=population_spans_query_template
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.population_name = population_name
        self.criteria_group = criteria_group

    @classmethod
    def from_criteria_group(
        cls,
        criteria_group: StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
        population_name: str,
        **query_format_kwargs: str,
    ) -> "StateAgnosticTaskCandidatePopulationBigQueryViewBuilder":
        query_template = _get_candidate_population_query_from_criteria_group(
            criteria_group
        )

        return cls(
            population_name=population_name,
            population_spans_query_template=query_template,
            description=__doc__,
            criteria_group=criteria_group,
            **query_format_kwargs,
        )

    def get_descendant_criteria(
        self,
    ) -> set[StateAgnosticTaskCriteriaBigQueryViewBuilder]:
        if self.criteria_group:
            return self.criteria_group.get_descendant_criteria()
        return set()

    def as_criteria(
        self,
        criteria_name: Optional[str] = None,
        **query_format_kwargs: str,
    ) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
        """Creates a StateAgnosticTaskCriteriaBigQueryViewBuilder that uses this candidate
        population as its criteria."""

        name = criteria_name or self.population_name

        query_template = _get_criteria_query_from_candidate_population(
            self.view_query_template, self.population_name
        )

        return StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name=name,
            criteria_spans_query_template=query_template,
            description=__doc__,
            reasons_fields=[
                ReasonsField(
                    name="population_name",
                    type=bigquery.enums.StandardSqlTypeNames.STRING,
                    description="Candidate population name",
                ),
            ],
            meets_criteria_default=False,
            **query_format_kwargs,
        )


TaskCandidatePopulationBigQueryViewBuilder = Union[
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
]
