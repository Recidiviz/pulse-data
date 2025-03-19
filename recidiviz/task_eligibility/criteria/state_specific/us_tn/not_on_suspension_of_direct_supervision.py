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
"""Spans of time when someone in TN is not on Suspension of Direct Supervision.

NB: this criterion looks at SDS specifically (the parole version of release from active
supervision in TN) and not the parallel probation version, Judicial Suspension of Direct
Supervision (JSS).

In TN, changes to supervision levels are generally backdated or postdated to the 1st of
the month. To work around the backdating (which can make it appear in the data as if a
client's level was changed before they actually experienced that change in real life),
we use supervision-level sessions with inferred start/end dates to more accurately
identify periods of time when we think a client was on SDS.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    SDS_SUPERVISION_LEVELS_RAW_TEXT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_ON_SUSPENSION_OF_DIRECT_SUPERVISION"

_QUERY_TEMPLATE = f"""
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(
            supervision_level_raw_text AS supervision_level_raw_text
        )) AS reason,
        supervision_level_raw_text,
    /* Pull data from the `analyst_data` view for sessions for TN raw-text supervision
    levels, which has inferred start/end dates that attempt to more accurately reflect
    the dates on which line staff actually initiated supervision-level changes. */
    FROM `{{project_id}}.{{analyst_dataset}}.us_tn_supervision_level_raw_text_sessions_inferred_materialized`
    WHERE supervision_level_raw_text IN ({list_to_query_string(SDS_SUPERVISION_LEVELS_RAW_TEXT, quoted=True)})
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the query
    # above, and we want to assume that folks are eligible by default otherwise.
    meets_criteria_default=True,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="supervision_level_raw_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Raw-text supervision level",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
