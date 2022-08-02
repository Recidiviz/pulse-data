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
from recidiviz.task_eligibility import task_eligiblity_spans
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_collector import (
    TaskCandidatePopulationBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_collector import (
    TaskCriteriaBigQueryViewCollector,
)


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    """Collects and returns a list of builders for all views related to task
    eligibility (i.e. views in task_eligibility* datasets).
    """
    return list(
        itertools.chain.from_iterable(
            (
                TaskCriteriaBigQueryViewCollector().collect_view_builders(),
                TaskCandidatePopulationBigQueryViewCollector().collect_view_builders(),
                SingleTaskEligibilityBigQueryViewCollector().collect_view_builders(),
                task_eligiblity_spans.get_unioned_view_builders(),
            )
        )
    )
