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
"""Spans of time during which a client in TN has not had any supervision sanctions in
the past year.
"""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    no_supervision_sanction_within_time_interval,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    supervision_sanctions_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_SUPERVISION_SANCTION_WITHIN_1_YEAR"

_QUERY_TEMPLATE = no_supervision_sanction_within_time_interval(
    date_interval=1,
    date_part="YEAR",
    supervision_sanctions_cte=supervision_sanctions_cte,
)

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="sanction_dates",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Date(s) of any sanction(s) within the past year",
        ),
        ReasonsField(
            name="latest_sanction_expiration_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date on which latest sanction(s) will age out of lookback period",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
