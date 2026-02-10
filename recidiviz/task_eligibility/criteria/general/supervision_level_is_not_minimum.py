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
# ============================================================================
"""This criteria view builder defines spans of time where clients are not on MINIMUM
supervision level as tracked by our `sessions` dataset.
"""
from google.cloud import bigquery

from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    custody_or_supervision_level_criteria_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "SUPERVISION_LEVEL_IS_NOT_MINIMUM"

_REASONS_COLUMNS = """supervision_level AS supervision_level,
    start_date AS minimum_start_date"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    custody_or_supervision_level_criteria_builder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        levels_lst=["MINIMUM"],
        reasons_columns=_REASONS_COLUMNS,
        reasons_fields=[
            ReasonsField(
                name="supervision_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Client's supervision level",
            ),
            ReasonsField(
                name="minimum_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date that a client started on minimum supervision",
            ),
        ],
        level_meets_criteria=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
