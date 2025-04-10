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
# =============================================================================
"""Defines a criteria view that shows spans of time for which supervision clients
are on a case type and supervision level that requires assessments
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_MEETS_RISK_ASSESSMENT_APPLICABLE_CONDITIONS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
are on a case type and supervision level that requires assessments.
"""

_QUERY_TEMPLATE = f"""
WITH
-- Create periods with case type and supervision level information
supervision_periods AS (
    SELECT
        state_supervision_period.person_id,
        state_supervision_period.state_code,
        start_date,
        termination_date as end_date_exclusive,
        case_type_raw_text as case_type,
        supervision_level
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` AS state_supervision_period
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry`
        USING (supervision_period_id)
),
-- Aggregate above periods by case_type
supervision_period_agg AS (
    {aggregate_adjacent_spans(
        table_name='supervision_periods',
        attribute=['case_type', 'supervision_level'],
        session_id_output_name='supervision_period_agg',
        end_date_field_name='end_date_exclusive'
    )}
)
SELECT
    person_id,
    state_code,
    start_date,
    end_date_exclusive as end_date,
    (
        supervision_level NOT IN ('IN_CUSTODY', 'LIMITED', 'UNSUPERVISED')
        -- Substance is only for while we don't support TC Phases. Remove once TC support is added. (#39288)
        AND case_type NOT IN (
            'ANNUAL', 'DAY/DISTRICT RESOURCE CENTER', 'NON-REPORTING', 'ELECTRONIC MONITORING', 'SUBSTANCE ABUSE'
            )
    ) AS meets_criteria,
    case_type,
    supervision_level,
    TO_JSON(STRUCT(
        case_type,
        supervision_level
        )) AS reason
FROM supervision_period_agg
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        normalized_state_dataset="us_tx_normalized_state",
        reasons_fields=[
            ReasonsField(
                name="case_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Supervision case type of the client.",
            ),
            ReasonsField(
                name="supervision_level",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Supervision level of the client.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
