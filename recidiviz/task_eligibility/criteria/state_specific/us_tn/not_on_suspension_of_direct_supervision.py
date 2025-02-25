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
"""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NOT_ON_SUSPENSION_OF_DIRECT_SUPERVISION"

# TODO(#38727): Update SDS eligibility logic (presumably by adjusting this criterion) to
# work around TN backdating.
_QUERY_TEMPLATE = f"""
    WITH sds_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive AS end_date,
            FALSE AS meets_criteria,
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized`
        WHERE state_code='US_TN'
            AND compartment_level_1='SUPERVISION'
            /* Suspension of Direct Supervision (for parole): '9SD' (current code) and
            'SDS' (previous code). */
            AND correctional_level_raw_text IN ('9SD', 'SDS')
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        CAST(NULL AS JSON) AS reason,
    FROM (
        -- aggregate adjacent spans to reduce number of rows
        {aggregate_adjacent_spans(
            "sds_spans",
            attribute=['meets_criteria'],
        )}
    )
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    # Set default to True because we only created spans of *ineligibility* in the
    # query above, and we want to assume that folks are eligible by default
    # otherwise.
    meets_criteria_default=True,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
