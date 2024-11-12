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
# =============================================================================
"""
Preprocessed fines and fees sessions view of restitution cases in ME. Unique on state code, 
person_id, external_id (client_id + client_case ID), fee_type, start_date, end_date 
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_me_fines_fees_sessions_preprocessed"
)

US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = """
Preprocessed fines and fees sessions view of restitution cases in ME. Unique on state code, 
person_id, external_id (client_id + client_case ID), fee_type, start_date, end_date
"""

# Status codes for closed restitution cases
CLOSED_REST_STATUSES = """ '51', '50', '46', '47', '48' """

US_ME_FINES_FEES_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
WITH payments_n_invoices AS (
  -- Combine payments and invoices, group if they happened on the same date
      SELECT 
        state_code,
        person_id,
        external_id, 
        fee_type,
        change_date,
        SUM(invoice_amount) AS invoice_amount,
        SUM(activity_amount) AS activity_amount,
        STRING_AGG(transaction_type ORDER BY transaction_type) AS transaction_type,
      FROM (
          SELECT 
            state_code,
            person_id,
            external_id, 
            fee_type,
            invoice_date AS change_date,
            invoice_amount_adjusted AS invoice_amount,
            invoice_amount_adjusted AS activity_amount,
            'INVOICE' AS transaction_type 
          FROM
            `{{project_id}}.{{analyst_dataset}}.invoices_preprocessed_materialized`
          WHERE
            state_code = 'US_ME'
        
          UNION ALL
        
          SELECT
            state_code,
            person_id,
            external_id, 
            fee_type,
            payment_date AS change_date,
            0 AS invoice_amount,
            -1 * payment_amount AS activity_amount,
            'PAYMENT' AS transaction_type
          FROM
            `{{project_id}}.{{analyst_dataset}}.payments_preprocessed_materialized`
          WHERE
            state_code = 'US_ME'
      )
      GROUP BY 1,2,3,4,5
),

calc_unpaid_amount AS (
  -- Calculate remaining unpaid amount/balance for each date
  SELECT 
    *,
    SUM(activity_amount) 
        OVER(PARTITION BY state_code, person_id, external_id, fee_type 
        ORDER BY change_date)
    AS unpaid_balance,
  FROM payments_n_invoices
),

status_updates AS (
-- Pulls latest status updates for each restitution case. This way we can manually
--    close restitution cases that were arbitrarily closed. 
  SELECT 
    'US_ME' AS state_code,
    ei.person_id,
    CONCAT(ccd.cis_100_client_id, '-', ccd.client_case_id) AS external_id,
    'RESTITUTION' AS fee_type,
    Cis_5001_5740_Client_Stat_Cd AS status,
    closed_date,
    'STATUS_CHANGE' AS stat_change
  FROM (
    -- Latest restitution case statuses (CIS_574) with external IDs and formatted dates
    SELECT 
      *,
      DATE(
        CAST(SUBSTR(Modified_On_Date, 0,4) AS INT64), 
        CAST(SUBSTR(Modified_On_Date, 6,2) AS INT64),
        CAST(SUBSTR(Modified_On_Date, 9,2) AS INT64)
      ) AS modified_date,
      DATE(
        CAST(SUBSTR(Created_On_Date, 0,4) AS INT64), 
        CAST(SUBSTR(Created_On_Date, 6,2) AS INT64),
        CAST(SUBSTR(Created_On_Date, 9,2) AS INT64)
      ) AS closed_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_574_CLIENT_CASE_STATUS_latest`
  ) ccs
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.CIS_573_CLIENT_CASE_DETAIL_latest` ccd
    ON ccs.Cis_573_Client_Case_Id = ccd.Client_Case_Id
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` ei
    ON ccd.cis_100_client_id = ei.external_id
      AND id_type = 'US_ME_DOC'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ccd.cis_100_client_id, Cis_573_Client_Case_Id 
                            ORDER BY modified_date DESC, 
                                     Cis_5001_5740_Client_Stat_Cd DESC) = 1
),

unpaid_amount_with_stat_updates AS (

  -- Adds the last status update for each restitution case
  --    when the most recent update suggests the restitution
  --    case is closed. This way restitution owed is manually
  --    reset to zero when a case is closed.

  SELECT 
    state_code,
    person_id, 
    external_id,
    fee_type,
    closed_date AS start_date,
    0 AS unpaid_balance,
    stat_change AS transaction_type
  FROM status_updates
  WHERE status IN ({CLOSED_REST_STATUSES})

  UNION ALL

  -- If there were payments or invoices that happened after
  --    the status change, we set the unpaid balance to zero

  SELECT 
    cua.state_code, 
    cua.person_id,
    cua.external_id,
    cua.fee_type,
    COALESCE(stat.closed_date, change_date) AS start_date,
    IF(stat.closed_date IS NULL, unpaid_balance, 0) AS unpaid_balance,
    COALESCE(stat.stat_change, transaction_type) AS transaction_type
  FROM calc_unpaid_amount cua
  LEFT JOIN (
    SELECT 
      *,
    FROM status_updates
    WHERE status IN ({CLOSED_REST_STATUSES})
  ) stat
    ON cua.external_id = stat.external_id
      AND stat.closed_date <= cua.change_date

)

SELECT 
    state_code, 
    person_id,
    external_id,
    fee_type,
    start_date,
    LEAD(start_date)
        OVER(PARTITION BY state_code, person_id, external_id, fee_type 
        ORDER BY start_date)
    AS end_date,
    ROUND(unpaid_balance, 1) AS unpaid_balance,
    transaction_type,
FROM unpaid_amount_with_stat_updates

"""

US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ME_FINES_FEES_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
