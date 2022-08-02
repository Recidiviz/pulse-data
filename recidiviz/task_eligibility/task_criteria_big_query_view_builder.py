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
"""Defines BigQueryViewBuilders that can be used to define single criteria span views.
These views are used as inputs to a task eligibility spans view.
"""
from typing import Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode


# TODO(#14309): Write tests for this class
class StateSpecificTaskCriteriaBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """A builder for a view that defines spans of time during which someone does (or
    does not) satisfy a single criteria. This should only be used for views that contain
    state-specific logic that could not be applied generally as a criteria in multiple
    states.
    """

    def __init__(
        self,
        state_code: StateCode,
        criteria_name: str,
        criteria_spans_query_template: str,
        description: str,
        # TODO(#14311): Add arguments to allow bounding the policy to specific dates
        #  and use those values in the span-collapsing logic in the
        #  SingleTaskEligibilitySpansBigQueryViewBuilder.
        **query_format_kwargs: str,
    ) -> None:
        state_code_prefix = f"{state_code.value}_"
        if not criteria_name.startswith(state_code_prefix):
            raise ValueError(
                f"Found state-specific task criteria [{criteria_name}] whose name "
                f"does not start with [{state_code_prefix}]."
            )
        view_id = criteria_name.removeprefix(state_code_prefix).lower()
        super().__init__(
            dataset_id=f"task_eligibility_criteria_{state_code.value.lower()}",
            view_id=view_id,
            description=description,
            view_query_template=criteria_spans_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.criteria_name = criteria_name


# TODO(#14309): Write tests for this class
class StateAgnosticTaskCriteriaBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """A builder for a view that defines spans of time during which someone does (or
    does not) satisfy a single criteria. This should only be used for views that contain
    NO state-specific logic and could be reused as a criteria in multiple states.
    """

    def __init__(
        self,
        criteria_name: str,
        criteria_spans_query_template: str,
        description: str,
        **query_format_kwargs: str,
    ) -> None:
        super().__init__(
            dataset_id="task_eligibility_criteria_general",
            view_id=criteria_name.lower(),
            description=description,
            view_query_template=criteria_spans_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.criteria_name = criteria_name


TaskCriteriaBigQueryViewBuilder = Union[
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
]
