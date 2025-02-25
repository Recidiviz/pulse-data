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
"""Preprocessed view of fines/fees payments in TN, unique on person, date, and state code"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_PAYMENTS_PREPROCESSED_VIEW_NAME = "us_tn_payments_preprocessed"

US_TN_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of fines/fees payments in TN,
unique on person, date, and state code"""

US_TN_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE = """
    WITH account_info AS (
        SELECT
            pei.person_id,
            pei.state_code,
            acc.AccountSAK,
        FROM
            `{project_id}.{raw_data_up_to_date_views_dataset}.OffenderAccounts_latest` acc
        INNER JOIN
            `{project_id}.{normalized_state_dataset}.state_person_external_id` pei            
        ON
            pei.external_id = acc.OffenderID
            AND id_type = "US_TN_DOC"
    )
    , payments AS (
      SELECT
          state_code,
          person_id,
          AccountSAK AS external_id,
          CAST(SPLIT(PaymentDate,' ')[OFFSET(0)] AS DATE) AS payment_date,
          CAST(PaidAmount AS FLOAT64) - CAST(UnAppliedAmount AS FLOAT64) AS payment_amount,
      FROM
          `{project_id}.{raw_data_up_to_date_views_dataset}.OffenderPayments_latest` p
      INNER JOIN
          account_info           
      USING
          (AccountSAK)
    )
    # sum over days in case multiple payments on same day
    SELECT
        state_code,
        person_id,
        external_id,
        "SUPERVISION_FEES" AS fee_type,
        payment_date,
        SUM(payment_amount) AS payment_amount
    FROM
        payments
    GROUP BY 1,2,3,4,5
    
"""

US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_PAYMENTS_PREPROCESSED_VIEW_NAME,
    description=US_TN_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
