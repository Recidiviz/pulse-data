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
"""Preprocessed view of fines/fees invoices in TN, unique on person, date, and state code"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_INVOICES_PREPROCESSED_VIEW_NAME = "us_tn_invoices_preprocessed"

US_TN_INVOICES_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of fines/fees invoices in TN, unique on person, date, and
state code"""

US_TN_INVOICES_PREPROCESSED_QUERY_TEMPLATE = f"""
    WITH account_info AS (
        SELECT
            pei.person_id,
            pei.state_code,
            acc.AccountSAK,
        FROM
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.OffenderAccounts_latest` acc
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei            
        ON
            pei.external_id = acc.OffenderID
            AND id_type = "US_TN_DOC"
    )
    , 
    /*
        us_tn_exemptions_preprocessed has non overlapping spans. We keep distinct values of
        person-span-FeeItemID. If there are different exempt amounts, we take the lower one to be more conservative
    */
    exemptions_dedup AS (
        SELECT 
            person_id,
            state_code,
            start_date,
            end_date,
            FeeItemID,
            IFNULL(exempt_amount,0) AS exempt_amount,
        FROM `{{project_id}}.{{analyst_dataset}}.us_tn_exemptions_preprocessed`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id, start_date, end_date, FeeItemID
            ORDER BY exempt_amount ASC) = 1 
    )
    , inv AS (
        SELECT
            person_id,
            AccountSAK AS external_id,
            state_code,
            FeeItemID,
            CAST(SPLIT(InvoiceDate,' ')[OFFSET(0)] AS DATE) AS invoice_date,
            IFNULL(CAST(InvoiceAmount AS FLOAT64),0) AS invoice_amount,
            COALESCE(CAST(GPVoidedInvStatus AS INT64),0) AS voided_status
        FROM
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.OffenderInvoices_latest`
        INNER JOIN
            account_info           
        USING
            (AccountSAK)
    )
    , inv_with_exemptions AS (
        /* Clients shouldn't have invoices for the fee types / time periods when they are exempt, but sometimes
            they do or they are billed $0. So we remove the exempt amount from the invoice amount */
        SELECT 
            inv.person_id,
            inv.state_code,
            inv.external_id,
            inv.invoice_date,
            inv.invoice_amount,
            GREATEST(invoice_amount - IFNULL(exempt_amount, 0),0) AS invoice_amount_adjusted,
        FROM
            inv
        LEFT JOIN
            exemptions_dedup exemptions
        ON
            inv.person_id = exemptions.person_id
            AND inv.state_code = exemptions.state_code
            AND inv.FeeItemID = exemptions.FeeItemID
            AND inv.invoice_date BETWEEN exemptions.start_date
                AND {nonnull_end_date_exclusive_clause('exemptions.end_date')} 
    )
    # sum over days in case multiple payments on same day
    SELECT
        state_code,
        person_id,
        external_id,
        "SUPERVISION_FEES" AS fee_type,
        invoice_date,
        SUM(invoice_amount) AS invoice_amount,
        SUM(invoice_amount_adjusted) AS invoice_amount_adjusted,
    FROM
        inv_with_exemptions
    GROUP BY 1,2,3,4,5
"""

US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_INVOICES_PREPROCESSED_VIEW_NAME,
    description=US_TN_INVOICES_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_INVOICES_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER.build_and_print()
