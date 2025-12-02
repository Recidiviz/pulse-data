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
# ============================================================================
"""Describes the spans of time when a resident is in a custody level that is lower than
the one recommended.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    custody_level_compared_to_recommended,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODY_LEVEL_LOWER_THAN_RECOMMENDED"

_QUERY_TEMPLATE = f"""
     -- Lower levels are prioritized lower for deduplication, so they have a higher priority "number"
     -- So if the recommended number is higher than the current number, the recommended custody level is lower
    {custody_level_compared_to_recommended(
        criteria=
        "recommended_cl.custody_level_priority < current_cl.custody_level_priority"
    )}
"""


VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="recommended_custody_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Recommended custody level",
            ),
            ReasonsField(
                name="custody_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Custody level",
            ),
            ReasonsField(
                name="upcoming_eligibility_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when client becomes eligible",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
