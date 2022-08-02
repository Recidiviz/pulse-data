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
"""View builder that auto-generates task eligiblity spans view from component criteria
and candidate population views.
"""
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.string import StrictStringFormatter

# CTE that can be formatted with criteria-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     my_criteria_state_specific AS (
#         SELECT *
#         FROM `{project_id}.{task_eligibility_criteria_xxx_dataset}.my_criteria`
#         WHERE state_code = 'US_XX'
#     )
STATE_SPECIFIC_CRITERIA_CTE = """{criteria_name}_state_specific AS (
    SELECT *
    FROM `{{project_id}}.{{{criteria_dataset_id}_dataset}}.{criteria_view_id}`
    WHERE state_code = '{state_code}'
)"""

# CTE that can be formatted with population-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     my_population_state_specific AS (
#         SELECT *
#         FROM `{project_id}.{task_eligibility_candidates_xxx_dataset}.my_population`
#         WHERE state_code = 'US_XX'
#     )
STATE_SPECIFIC_POPULATION_CTE = """{population_name}_state_specific AS (
    SELECT *
    FROM `{{project_id}}.{{{population_dataset_id}_dataset}}.{population_view_id}`
    WHERE state_code = '{state_code}'
)"""


# TODO(#14310): Write tests for this class
class SingleTaskEligibilitySpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates task eligiblity spans view from component
    criteria and candidate population views.
    """

    def __init__(
        self,
        state_code: StateCode,
        task_name: str,
        description: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        self._validate_builder_state_codes(
            state_code, candidate_population_view_builder, criteria_spans_view_builders
        )
        view_query_template = self._build_query_template(
            state_code,
            task_name,
            candidate_population_view_builder,
            criteria_spans_view_builders,
        )
        query_format_kwargs = self._dataset_query_format_args(
            candidate_population_view_builder, criteria_spans_view_builders
        )
        super().__init__(
            dataset_id=f"task_eligibility_spans_{state_code.value.lower()}",
            view_id=task_name.lower(),
            description=description,
            view_query_template=view_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.state_code = state_code

    @staticmethod
    def _build_query_template(
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> str:
        """Builds the view query template that does span collapsing logic to generate
        task eligibility spans from component criteria and population spans views.
        """
        if not candidate_population_view_builder.materialized_address:
            raise ValueError(
                f"Expected materialized_address for view [{candidate_population_view_builder.address}]"
            )
        all_span_ctes = [
            StrictStringFormatter().format(
                STATE_SPECIFIC_POPULATION_CTE,
                state_code=state_code.value,
                population_name=candidate_population_view_builder.population_name.lower(),
                population_dataset_id=candidate_population_view_builder.materialized_address.dataset_id,
                population_view_id=candidate_population_view_builder.materialized_address.table_id,
            )
        ]
        for criteria_view_builder in criteria_spans_view_builders:
            if not criteria_view_builder.materialized_address:
                raise ValueError(
                    f"Expected materialized_address for view [{criteria_view_builder.address}]"
                )
            all_span_ctes.append(
                StrictStringFormatter().format(
                    STATE_SPECIFIC_CRITERIA_CTE,
                    state_code=state_code.value,
                    criteria_name=criteria_view_builder.criteria_name.lower(),
                    criteria_dataset_id=criteria_view_builder.materialized_address.dataset_id,
                    criteria_view_id=criteria_view_builder.materialized_address.table_id,
                )
            )
        all_span_ctes_str = ",\n".join(all_span_ctes)
        # TODO(#14310): Actually collapse spans here.
        return f"""WITH {all_span_ctes_str}
SELECT 
    '{task_name}' AS task_name,
    0 AS person_id,
    NULL AS start_date,
    NULL AS end_date,
    True AS is_eligible,
    [] AS ineligible_criteria,
    [] as reasons;
"""

    @staticmethod
    def _dataset_query_format_args(
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> Dict[str, str]:
        """Returns the query format args for the datasets in the query template. All
        datasets are injected as query format args so that these views work when
        deployed to sandbox datasets.
        """
        datasets = [candidate_population_view_builder.dataset_id] + [
            vb.dataset_id for vb in criteria_spans_view_builders
        ]

        return {f"{dataset_id}_dataset": dataset_id for dataset_id in datasets}

    @staticmethod
    def _validate_builder_state_codes(
        task_state_code: StateCode,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        """Validates that the state code for this task eligiblity view matches the
        state codes on all component criteria / population view builders (if one
        exists).
        """
        if isinstance(
            candidate_population_view_builder,
            StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
        ):
            if candidate_population_view_builder.state_code != task_state_code:
                raise ValueError(
                    f"Found candidate population "
                    f"[{candidate_population_view_builder.population_name}] with "
                    f"state_code [{candidate_population_view_builder.state_code}] which"
                    f"does not match the task state_code [{task_state_code}]"
                )

        for criteria_view_builder in criteria_spans_view_builders:
            if isinstance(
                criteria_view_builder,
                StateSpecificTaskCriteriaBigQueryViewBuilder,
            ):
                if criteria_view_builder.state_code != task_state_code:
                    raise ValueError(
                        f"Found criteria [{criteria_view_builder.criteria_name}] with "
                        f"state_code [{criteria_view_builder.state_code}] which does "
                        f"not match the task state_code [{task_state_code}]"
                    )
