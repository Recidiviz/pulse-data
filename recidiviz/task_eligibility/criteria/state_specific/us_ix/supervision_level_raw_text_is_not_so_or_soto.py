# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""This criteria view builder defines spans of time where clients do not have a supervision level
raw text value involving a sex offense (SO or SO TO '') as tracked by our `sessions` dataset.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ix_query_fragments import (
    supervision_level_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IX_SUPERVISION_LEVEL_RAW_TEXT_IS_NOT_SO_OR_SOTO"

_DESCRIPTION = """This criteria view builder defines spans of time where clients do not have a supervision level
raw text value involving a sex offense (SO or SO TO '') as tracked by our `sessions` dataset."""

_CORRECTIONAL_LEVELS_TO_EXCLUDE = [
    "SO HIGH",
    "SO MODERATE",
    "SO LOW",
    "SO TO GENERAL LOW",
    "SO TO GENERAL MOD",
    "SO TO GENERAL HIGH",
]
_QUERY_TEMPLATE = supervision_level_criteria_query(_CORRECTIONAL_LEVELS_TO_EXCLUDE)

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        state_code=StateCode.US_IX,
        reasons_fields=[
            ReasonsField(
                name="supervision_level_is_so",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the supervision level is sexual offense related",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
