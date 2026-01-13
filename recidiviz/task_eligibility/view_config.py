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
"""Task Eligibility view configuration."""

import itertools
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.task_eligibility import task_eligibility_spans
from recidiviz.task_eligibility.all_compliance_task_eligibility_spans import (
    get_compliance_eligibility_spans_unioned_view_builders,
)
from recidiviz.task_eligibility.almost_eligible_task_spans_big_query_view_collector import (
    AlmostEligibleSpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_collector import (
    BasicSingleTaskEligibilitySpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_task_eligibility_spans_view_for_tes_builder,
)
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_collector import (
    ComplianceTaskEligibilitySpansBigQueryViewCollector,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_collector import (
    TaskCandidatePopulationBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_collector import (
    TaskCriteriaBigQueryViewCollector,
)


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    """Collects and returns a list of builders for all views related to task
    eligibility (i.e. views in task_eligibility* datasets).
    """
    tes_builders = SingleTaskEligibilityBigQueryViewCollector().collect_view_builders()
    basic_tes_builders = BasicSingleTaskEligibilitySpansBigQueryViewCollector(
        tes_builders
    ).collect_view_builders()
    almost_eligible_tes_builders = AlmostEligibleSpansBigQueryViewCollector(
        basic_tes_builders, tes_builders
    ).collect_view_builders()
    collapsed_tes_builders = [
        build_collapsed_task_eligibility_spans_view_for_tes_builder(tes_builder)
        for tes_builder in tes_builders
    ]

    return list(
        itertools.chain.from_iterable(
            (
                TaskCriteriaBigQueryViewCollector().collect_view_builders(),
                TaskCandidatePopulationBigQueryViewCollector().collect_view_builders(),
                TaskCompletionEventBigQueryViewCollector().collect_view_builders(),
                basic_tes_builders,
                almost_eligible_tes_builders,
                tes_builders,
                collapsed_tes_builders,
                task_eligibility_spans.get_unioned_view_builders(),
                ComplianceTaskEligibilitySpansBigQueryViewCollector().collect_view_builders(),
                get_compliance_eligibility_spans_unioned_view_builders(),
            )
        )
    )
