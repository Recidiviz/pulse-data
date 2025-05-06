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
"""Shows spans of time during which a client has paid off their most recent supervision fee"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    MAGIC_END_DATE,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import sessionize_ledger_data
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_IA_SUPERVISION_FEES_PAID"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH supervision_fees AS (
/* pulls all applicable supervision fees and their current status/balances */ 
    SELECT state_code,
        person_id, 
        DATE(IntakeDate) AS IntakeDate, 
        InitialBalance, 
        CurrentBalance, 
        /* if the fee was paid in full, assume it was paid on the close date (or the last update date if close date is missing) */
        CASE WHEN FeeStatusDesc = 'Paid in Full' THEN COALESCE(DATE(CloseDate), DATE(DateOfLastUpdate)) END AS close_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.IA_DOC_Fees_latest` fee
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei 
        ON fee.IconNumber = pei.external_id
        AND pei.state_code = 'US_IA'
        AND pei.id_type = 'US_IA_OFFENDERCD'
    WHERE UPPER(FeeDescription) LIKE '%SUPERVISION%'
        AND FeeStatusDesc IN ('Active', 'Paid in Full') -- do not include deleted fees
        # TODO(#41880) figure out how to deal with legitimate fee waivers
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, IntakeDate ORDER BY FeeStatusDesc DESC) = 1 -- if two supervision fees were imposed on the same date, assume that they are duplicates and preferentially take the fee that was paid in full
    # TODO(#41884) validate cases with TTs and figure out if we want to set a threshold here to exclude old supervision fees 
), supervision_fees_clean AS (
/* cleans up edge cases where the fee was marked paid on or before the intake date by making a fake span from the intake date -> 1 day after */
    SELECT 
        state_code,
        person_id,
        IntakeDate,
        InitialBalance, 
        CurrentBalance,
        IF(close_date IS NOT NULL AND close_date <= IntakeDate, DATE_ADD(IntakeDate, INTERVAL 1 DAY), close_date) AS close_date
    FROM supervision_fees
), supervision_fee_spans AS (
/* sessionizes by intake date so that only the most recent supervision fee applies at a time */
    {sessionize_ledger_data(
        table_name = 'supervision_fees_clean',
        index_columns = ['state_code', 'person_id'],
        update_column_name = 'IntakeDate',
        attribute_columns = ['InitialBalance', 'CurrentBalance', 'close_date']
    )}
)
SELECT state_code,
    person_id,
    start_date, 
    /* they should stop being ineligible when the fee is paid or when the span ends, whichever comes sooner */
    {revert_nonnull_end_date_clause(f'LEAST(IFNULL(close_date, "{MAGIC_END_DATE}"), IFNULL(end_date_exclusive, "{MAGIC_END_DATE}"))')} AS end_date,
    FALSE AS meets_criteria,
    InitialBalance AS initial_balance,
    CurrentBalance AS current_balance,
    TO_JSON(STRUCT(InitialBalance AS initial_balance, CurrentBalance AS current_balance)) AS reason,
FROM supervision_fee_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_IA,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_IA,
            instance=DirectIngestInstance.PRIMARY,
        ),
        criteria_spans_query_template=_REASON_QUERY,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="initial_balance",
                type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
                description="Initial balance on most recent supervision fee",
            ),
            ReasonsField(
                name="current_balance",
                type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
                description="Current balance on most recent supervision fee",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
