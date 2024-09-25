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
"""Preprocessed view of fines/fees exemptions in TN"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_EXEMPTIONS_PREPROCESSED_VIEW_NAME = "us_tn_exemptions_preprocessed"

US_TN_EXEMPTIONS_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of fines/fees exemptions in TN, which does some
basic clean up of fields and applies sub-sessionization logic to deal with overlapping spans. This view retains both
FeeItemID (what the exemption is for) and ReasonCode (why the exemption was given). Downstream views can then further
deduplicate as needed"""

US_TN_EXEMPTIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
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
    , exemptions AS (
        SELECT
            person_id,
            AccountSAK AS external_id,
            state_code,
            CAST(SPLIT(StartDate,' ')[OFFSET(0)] AS DATE) AS start_date,
            /* Convert to exclusive end date since it is MUCH more frequent for the previous end date to be 1 day
               less than the start date rather than equal to
            */
            DATE_ADD(CAST(SPLIT(EndDate,' ')[OFFSET(0)] AS DATE), INTERVAL 1 DAY) AS end_date,
            FeeItemID,
            ReasonCode,
            CAST(ExemptAmount AS FLOAT64) AS exempt_amount,
        FROM
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.OffenderExemptions_latest`
        INNER JOIN
            account_info           
        USING
            (AccountSAK)
        -- Remove 1% of spans where end_date (original) is <= start_date. This is much less common (0.2%) for recent data
        WHERE {nonnull_end_date_exclusive_clause("CAST(SPLIT(EndDate,' ')[OFFSET(0)] AS DATE)")} > CAST(SPLIT(StartDate,' ')[OFFSET(0)] AS DATE)
    ),
    /*
        There are some people with the same FeeItemID during overlapping spans of time (~15%). Often this is explained 
        by having a different ReasonCode. It's not totally clear if this is expected behavior, but having these overlaps
        means invoices may get erroneously duplicated in the next CTE (though invoice_amount_adjusted should be correct
        if the invoice amount = exempt amount). We use the create_sub_sessions_with_attributes method which outputs 
        non overlapping spans.
    */
    {create_sub_sessions_with_attributes('exemptions')}
    
    SELECT *
    FROM sub_sessions_with_attributes
"""

US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_EXEMPTIONS_PREPROCESSED_VIEW_NAME,
    description=US_TN_EXEMPTIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_EXEMPTIONS_PREPROCESSED_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
