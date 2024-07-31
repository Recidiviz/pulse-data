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

"""Defines a criteria view that shows spans of time where
clients are not 'INTERSTATE COMPACT IN'.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#20819) replace with state agnostic version if appropriate once interstate schema is updated

_CRITERIA_NAME = "US_ME_SUPERVISION_IS_NOT_IC_IN"

_DESCRIPTION = """Defines a criteria view that shows spans of time where
clients are not 'INTERSTATE COMPACT IN'.
"""

_QUERY_TEMPLATE = f"""
WITH sup_period_curr_status AS (
    SELECT 
        state_code,
        person_id, 
        start_date,
        termination_date AS end_date, 
        SPLIT(supervision_type_raw_text, '@@')[OFFSET(1)] AS raw_current_status
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
    WHERE state_code = 'US_ME'
        AND SPLIT(supervision_type_raw_text, '@@')[OFFSET(1)] = 'INTERSTATE COMPACT IN'
        AND start_date != {nonnull_end_date_clause('termination_date')}
),

{create_sub_sessions_with_attributes('sup_period_curr_status')},

distinct_ic_spans AS (
    SELECT 
        DISTINCT 
        state_code,
        person_id, 
        start_date,
        end_date, 
        False AS meets_criteria,
        raw_current_status AS current_status
    FROM sub_sessions_with_attributes
)

SELECT 
    state_code,
    person_id, 
    start_date,
    end_date, 
    False AS meets_criteria,
    TO_JSON(STRUCT(current_status AS current_status)) AS reason,
    current_status,
FROM distinct_ic_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="current_status",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="Current status of the supervision period.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
