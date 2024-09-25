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
"""Preprocessed view of restitution payments in ME, unique on person, date, and state code"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_PAYMENTS_PREPROCESSED_VIEW_NAME = "us_me_payments_preprocessed"

US_ME_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of restitution payments in ME,
unique on person, date, and state code"""

US_ME_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE = """
WITH rest_payments AS (
  SELECT
    ei.person_id,
    ei.state_code,
    DATE(
      CAST(SUBSTR(rt.trans_datetime, 0,4) AS INT64), 
      CAST(SUBSTR(rt.trans_datetime, 6,2) AS INT64),
      CAST(SUBSTR(rt.trans_datetime, 9,2) AS INT64)
    ) AS payment_date,
    "RESTITUTION" AS fee_type,
    CAST(rt.amount_num AS FLOAT64) AS amount,
    CONCAT(cc.cis_100_client_id, '-', cc.client_case_id) AS external_id,
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_580_REST_TRANSACTION_latest` rt
  INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_573_CLIENT_CASE_DETAIL_latest` cc
    ON rt.cis_573_client_case_id = cc.client_case_id
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` ei
    ON cc.cis_100_client_id = ei.external_id
      AND id_type = 'US_ME_DOC'
  WHERE cis_100_client_id IS NOT NULL
    AND rejected_ind = 'N'
)

SELECT 
  state_code,
  person_id,
  external_id, 
  fee_type, 
  payment_date,
  SUM(amount) AS payment_amount,
FROM rest_payments
GROUP BY 1,2,3,4,5
"""

US_ME_PAYMENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ME_PAYMENTS_PREPROCESSED_VIEW_NAME,
    description=US_ME_PAYMENTS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ME_PAYMENTS_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_PAYMENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
