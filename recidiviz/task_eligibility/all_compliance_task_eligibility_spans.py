# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Builds views that combine compliance task eligibility spans into unioned views"""

from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_collector import (
    ComplianceTaskEligibilitySpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.dataset_config import (
    COMPLIANCE_TASK_ELIGIBILITY_SPANS_DATASET_ID,
    compliance_task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.string import StrictStringFormatter

ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_DESCRIPTION_TEMPLATE = """
This view contains all compliance task eligiblity spans for {state_code} tasks. It unions the 
results of all single-task views for this state, aka all the other views in this 
dataset (`{state_specific_spans_dataset_id}`).
"""

ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_ALL_STATES_DESCRIPTION = """
This view contains all COMPLIANCE task eligiblity spans for tasks across states. It unions the 
results of all single-state `all_compliance_task_eligibility_spans` views (e.g. `compliance_task_eligibility_spans_us_xx.all_compliance_task_eligibility_spans`).
"""

ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID = (
    "all_compliance_task_eligibility_spans"
)


def get_compliance_eligiblity_spans_unioned_view_builders() -> Sequence[
    BigQueryViewBuilder
]:
    """Returns a list of view builders containing:
    a) one view per state, which unions compliance task eligibility spans for that state into
        a single 'all_compliance_task_eligibility_spans' view for that state, and
    b) one view that unions all the data from the state-specific 'all_compliance_task_eligibility_spans'
    views into one place.
    """
    clustering_fields = ["state_code", "task_name"]

    state_specific_unioned_view_builders = []
    view_collector = ComplianceTaskEligibilitySpansBigQueryViewCollector()
    for (
        state_code,
        task_view_builders,
    ) in view_collector.collect_view_builders_by_state().items():
        if not task_view_builders:
            raise ValueError(
                f"Found no defined ComplianceTaskEligibilitySpansBigQueryView for "
                f"[{state_code}] - is there an empty module for this state?"
            )

        def get_criteria_select_statement(
            vb: ComplianceTaskEligibilitySpansBigQueryViewBuilder,
        ) -> str:
            return f"SELECT state_code, person_id, '{vb.task_name}' AS task_name, start_date, end_date, is_eligible, reasons, reasons_v2, ineligible_criteria, due_date, last_task_completed_date"

        dataset_id = compliance_task_eligibility_spans_state_specific_dataset(
            state_code
        )
        state_specific_unioned_view_builders.append(
            UnionAllBigQueryViewBuilder(
                dataset_id=dataset_id,
                view_id=ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
                description=StrictStringFormatter().format(
                    ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_DESCRIPTION_TEMPLATE,
                    state_code=state_code.value,
                    state_specific_spans_dataset_id=dataset_id,
                ),
                parents=task_view_builders,
                clustering_fields=clustering_fields,
                parent_view_to_select_statement=get_criteria_select_statement,
            )
        )

    if not state_specific_unioned_view_builders:
        raise ValueError(
            "Found no defined ComplianceTaskEligibilitySpansBigQueryViews defined for any state."
        )

    return state_specific_unioned_view_builders + [
        UnionAllBigQueryViewBuilder(
            dataset_id=COMPLIANCE_TASK_ELIGIBILITY_SPANS_DATASET_ID,
            view_id=ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_ALL_TASKS_VIEW_ID,
            description=ALL_COMPLIANCE_TASK_ELIGIBILITY_SPANS_ALL_STATES_DESCRIPTION,
            parents=state_specific_unioned_view_builders,
            clustering_fields=clustering_fields,
        ),
    ]
