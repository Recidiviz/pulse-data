# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Spans of time when someone hasn't had a supervision sanction in the past 6 months."""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_OR_NO_SUPERVISION_SANCTIONS_WITHIN_6_MONTHS"

# TODO(#35405): Move this criterion logic into a general criterion builder or shared
# query fragment, where it can be generalized/parameterized and made state-agnostic.
_QUERY_TEMPLATE = f"""
    WITH supervision_sanctions AS (
        SELECT
            state_code,
            person_id,
            response_date AS start_date,
            DATE_ADD(response_date, INTERVAL 6 MONTH) AS end_date,
            response_date AS sanction_date,
            DATE_ADD(response_date, INTERVAL 6 MONTH) AS sanction_expiration_date,
            FALSE AS meets_criteria,
        FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_violation_response`
        WHERE state_code='US_OR'
            -- only count sanctions (not interventions) for this criterion
            AND JSON_EXTRACT_SCALAR(violation_response_metadata, '$.SANCTION_OR_INTERVENTION')='S'
    ),
    {create_sub_sessions_with_attributes('supervision_sanctions')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        LOGICAL_AND(meets_criteria) AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC)[OFFSET(0)] AS latest_sanction_date,
            MAX(sanction_expiration_date) AS violation_expiration_date
        )) AS reason,
        ARRAY_AGG(sanction_date IGNORE NULLS ORDER BY sanction_date DESC)[OFFSET(0)] AS latest_sanction_date,
        MAX(sanction_expiration_date) AS violation_expiration_date,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_OR,
    meets_criteria_default=True,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reasons_fields=[
        ReasonsField(
            name="latest_sanction_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the violation occurred",
        ),
        ReasonsField(
            name="violation_expiration_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the violations will age out of the time interval",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
