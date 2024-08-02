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
"""This criteria view builder defines spans of time that clients are not on IC-IN supervision in Michigan
defined as supervised in Michigan but sentenced in a different state.
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
_CRITERIA_NAME = "US_MI_SUPERVISION_IS_NOT_IC_IN"

_DESCRIPTION = """This criteria view builder defines spans of time that clients are not on IC-IN supervision in Michigan
defined as supervised in Michigan but sentenced in a different state.
"""

_QUERY_TEMPLATE = f"""
#TODO(#22511) refactor to build off of a general criteria view builder
WITH ic_in_spans AS (
    SELECT 
        state_code,
        person_id, 
        start_date,
        termination_date AS end_date, 
        'IC_IN' AS current_status,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` ssp
    WHERE state_code = 'US_MI'
        AND ssp.supervision_type_raw_text LIKE 'ORDER_TYPE_ID_LIST%'
    -- 1719: Interstate Compact Parole, 1720: Interstate Compact Probation
        AND ('1719' IN UNNEST(SPLIT((SPLIT(ssp.supervision_type_raw_text, "-"))[offset(1)], ","))
                OR '1720' IN UNNEST(SPLIT((SPLIT(ssp.supervision_type_raw_text, "-"))[offset(1)],",")))
        AND start_date != {nonnull_end_date_clause('termination_date')}
),
{create_sub_sessions_with_attributes('ic_in_spans')},
distinct_ic_spans AS (
    SELECT 
        DISTINCT 
        state_code,
        person_id, 
        start_date,
        end_date, 
        False AS meets_criteria,
        current_status AS current_status
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
        state_code=StateCode.US_MI,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="current_status",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Current interstate compact status",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
