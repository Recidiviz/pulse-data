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
"""This criteria view builder defines spans of time that clients are on supervision and under the
custodial authority of supervision authority or other state.
Therefore, federal and other country custodial authorities are excluded.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "CUSTODIAL_AUTHORITY_IS_SUPERVISION_AUTHORITY_OR_OTHER_STATE"

_QUERY_TEMPLATE = f"""
WITH custodial_authority_spans AS (
SELECT
        state_code,
        person_id,
        start_date,
        termination_date AS end_date,
        TRUE as eligible_custodial_authority
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
    WHERE custodial_authority IN ('SUPERVISION_AUTHORITY', 'OTHER_STATE') 
),
{create_sub_sessions_with_attributes('custodial_authority_spans')}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(eligible_custodial_authority) AS meets_criteria,
    TO_JSON(STRUCT(LOGICAL_OR(eligible_custodial_authority) AS eligible_custodial_authority)) AS reason,
    LOGICAL_OR(eligible_custodial_authority) AS eligible_custodial_authority,
FROM sub_sessions_with_attributes
WHERE start_date != {nonnull_end_date_clause('end_date')}
GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="eligible_custodial_authority",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Specifies whether a client is under the custodial authority of a supervision authority or another state authority. Federal and other country authorities are excluded.",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
