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
from typing import Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode


# TODO(#14309): Write tests for this class
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
        **query_format_kwargs: str,
    ) -> None:

        if not population_name.startswith(state_code.value):
            raise ValueError(
                f"Found state-specific task criteria [{population_name}] whose name "
                f"does not start with [{state_code.value}]."
            )
        view_id = population_name.removeprefix(f"{state_code.value}_").lower()
        super().__init__(
            dataset_id=f"task_eligibility_candidates_{state_code.value.lower()}",
            view_id=view_id,
            description=description,
            view_query_template=population_spans_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.population_name = population_name


# TODO(#14309): Write tests for this class
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
        # All keyword args must have string values
        **query_format_kwargs: str,
    ) -> None:
        super().__init__(
            dataset_id="task_eligibility_candidates_general",
            view_id=population_name.lower(),
            description=description,
            view_query_template=population_spans_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.population_name = population_name


TaskCandidatePopulationBigQueryViewBuilder = Union[
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
]
