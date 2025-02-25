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
"""Preprocessed view of restitution invoices in ME, unique on state code, person_id,
external_id (client_id + client_case ID), fee_type, invoice_date """

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_INVOICES_PREPROCESSED_VIEW_NAME = "us_me_invoices_preprocessed"

US_ME_INVOICES_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of restitution invoices in ME, unique on person, date, and
state code"""

US_ME_INVOICES_PREPROCESSED_QUERY_TEMPLATE = """
WITH invoices_cte AS (
  SELECT 
    pei.state_code,
    pei.person_id, 
    "RESTITUTION" AS fee_type,
    DATE(
      CAST(SUBSTR(cc.Created_On_Date, 0,4) AS INT64), 
      CAST(SUBSTR(cc.Created_On_Date, 6,2) AS INT64),
      CAST(SUBSTR(cc.Created_On_Date, 9,2) AS INT64)
      ) AS invoice_date,
    CAST(r.Rest_Amount_Num AS FLOAT64) AS invoice_amount,
    CAST(cc.total_owing_num AS FLOAT64) AS invoice_amount_adjusted,
    CONCAT(cc.cis_100_client_id, '-', cc.client_case_id) AS external_id,
  # CIS_573 holds the adjusted restitution amount (which is the one we're interested in)
  #   and CIS_589 holds the initial unadjusted restitution amount (which we don't fully understand). 
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.CIS_573_CLIENT_CASE_DETAIL_latest` cc
  #TODO(#19735): query from raw_data_up_to_date_views_dataset instead of static reference 
  LEFT JOIN `{project_id}.{static_reference_tables}.CIS_589_RESTITUTION` r
    ON cc.Cis_100_Client_Id = r.Cis_100_Client_Id
      AND cc.Cis_589_Restitution_Id = r.Restitution_Id
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON cc.cis_100_client_id = pei.external_id
      AND id_type = 'US_ME_DOC'
)

SELECT 
  state_code, 
  person_id, 
  external_id,
  fee_type,
  invoice_date,
  SUM(invoice_amount) AS invoice_amount,
  SUM(invoice_amount_adjusted) AS invoice_amount_adjusted,
FROM invoices_cte
GROUP BY 1,2,3,4,5
"""

US_ME_INVOICES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ME_INVOICES_PREPROCESSED_VIEW_NAME,
    description=US_ME_INVOICES_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ME_INVOICES_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    static_reference_tables=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_INVOICES_PREPROCESSED_VIEW_BUILDER.build_and_print()
