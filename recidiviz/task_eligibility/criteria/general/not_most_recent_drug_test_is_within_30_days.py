# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Describes the spans of time when a client's most recent drug test did NOT occur
within the past 30 days, or when the client has no drug tests on record.
"""

from recidiviz.task_eligibility.criteria.general.most_recent_drug_test_is_within_30_days import (
    VIEW_BUILDER as MOST_RECENT_DRUG_TEST_IS_WITHIN_30_DAYS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER: StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
        sub_criteria=MOST_RECENT_DRUG_TEST_IS_WITHIN_30_DAYS_VIEW_BUILDER,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
