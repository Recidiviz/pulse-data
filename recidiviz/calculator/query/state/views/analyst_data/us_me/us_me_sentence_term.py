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
"""US_ME - Clean term information for residents of MEDOC. Taken from CIS_319

    Returns a view with the CIS_319_TERM_latest table and the following:
        - Merged with recidiviz ids
        - Dates instead of datetimes
        - Drops zero-day sessions
        - Drops start dates that come after end_dates
        - Transform magic start and end dates to NULL
        - In case a revocation happened in between the term, it uses the revocation
            admission date as the intake_date/start_date
    """

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_SENTENCE_TERM_VIEW_NAME = "us_me_sentence_term"

US_ME_SENTENCE_TERM_VIEW_DESCRIPTION = """
    US_ME - Clean term information for residents of MEDOC. Taken from CIS_319
        
        Returns a view with the CIS_319_TERM_latest table and the following:
        - Merged with recidiviz ids
        - Dates instead of datetimes
        - Drops zero-day sessions
        - Drops start dates that come after end_dates
        - Transform magic start and end dates to NULL
        - In case a revocation happened in between the term, it uses the revocation
            admission date as the intake_date/start_date
    """

US_ME_SENTENCE_TERM_QUERY_TEMPLATE = f"""
WITH revocations AS (
    SELECT 
        person_id,
        admission_date,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` 
    WHERE state_code = 'US_ME'
        AND admission_reason = 'REVOCATION'
    ),
    
    term_cte AS (
    -- Term data with intake date and expected release date
    SELECT
        t.state_code, 
        t.person_id,
        Cis_100_Client_Id,
        {revert_nonnull_start_date_clause('start_date')} AS start_date,
        {revert_nonnull_end_date_clause('end_date')} AS end_date,
        {revert_nonnull_end_date_clause('end_date_max')} AS end_date_max,
        t.Cis_1200_Term_Status_Cd AS status,
        term_id,
    FROM (
            -- Cast dates, join with recidiviz ids and make sure intake_date and 
            --   expected release date are not null
            SELECT
                *,
                {nonnull_start_date_clause('DATE(SAFE_CAST(intake_date AS DATETIME))')} AS start_date,
                {nonnull_end_date_clause('DATE(SAFE_CAST(Curr_Cust_Rel_Date AS DATETIME))')} AS end_date, 
                {nonnull_end_date_clause('DATE(SAFE_CAST(Max_Cust_Rel_Date AS DATETIME))')} AS end_date_max,
                -- TODO(#16175) should ingest the expected release date sometime soon
            FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_319_TERM_latest`
            -- TODO(#17653) INNER JOIN drops a handful of people who don't have `person_id` values
            INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id`
                ON Cis_100_Client_Id = external_id
                AND id_type = 'US_ME_DOC'
            WHERE intake_date IS NOT NULL OR Curr_Cust_Rel_Date IS NOT NULL
         ) t
    WHERE start_date < end_date -- Drop if end_date is before start_datetime
    ),

    term_cte_with_revocation AS (
        -- If a person was returned to incarceration after a revocation, we use the
        --   the return date as the new admission_date
        SELECT 
            state_code, 
            person_id,
            Cis_100_Client_Id,
            start_date,
            end_date,
            end_date_max,
            status,
            term_id,
        FROM term_cte

        UNION ALL

        SELECT 
            t.state_code, 
            t.person_id,
            t.Cis_100_Client_Id,
            COALESCE(r.admission_date, t.start_date) AS start_date,
            t.end_date,
            t.end_date_max,
            t.status,
            t.term_id,
        FROM term_cte t
        INNER JOIN revocations r
            ON r.admission_date BETWEEN t.start_date AND DATE_SUB(t.end_date, INTERVAL 1 DAY)
            AND r.person_id = t.person_id
        QUALIFY ROW_NUMBER() OVER(PARTITION BY Term_Id ORDER BY admission_date DESC) = 1
    ),

    liberty_sessions AS (
        SELECT 
            person_id,
            end_date_exclusive AS liberty_start_date,
        FROM `{{project_id}}.sessions.compartment_sessions_materialized`
        WHERE state_code = 'US_ME' 
            AND outflow_to_level_1 = 'LIBERTY'
            AND end_reason != 'TEMPORARY_RELEASE'
    ),

    term_cte_with_revocation_and_filled_nulls AS (
        -- If a person was released to liberty, but there's no current_release_date,
        --    we impute his/her release date
        SELECT 
            t.* EXCEPT(end_date, end_date_max),
            COALESCE(liberty_start_date, end_date) AS end_date,
            COALESCE(liberty_start_date, end_date_max) AS end_date_max
        FROM term_cte_with_revocation t
        LEFT JOIN liberty_sessions l
        ON t.person_id = l.person_id
            AND l.liberty_start_date > t.start_date
            AND t.end_date IS NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY t.person_id, t.term_id, t.start_date ORDER BY l.liberty_start_date) = 1
    )

-- finally, if someone spent time in county, we adjust their start date to reflect that
SELECT 
  t.state_code,
  t.person_id,
  DATE_SUB(t.start_date,
           INTERVAL IFNULL(county.days_spent_in_county, 0) DAY) AS start_date_from_county_jail,
  t.start_date,
  t.end_date,
  t.end_date_max, 
  t.status,
  t.term_id,
FROM term_cte_with_revocation_and_filled_nulls t
LEFT JOIN (
    -- Join with days in county jail
    SELECT 
      Cis_400_Cis_100_Client_Id,
      Cis_319_Term_Id,
      SAFE_CAST(LEFT(Court_Order_Sent_Date, 10) AS DATE) AS crt_order_sent_date,
      MAX(SAFE_CAST(Detention_Days_Num AS INT64)) AS days_spent_in_county,
    FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_401_CRT_ORDER_HDR_latest`
    -- only include days in county if the person was sentenced to county jail
    WHERE Apply_Det_Days_Ind = 'Y'
    GROUP BY 1,2,3
) county
  ON t.Cis_100_Client_Id = county.Cis_400_Cis_100_Client_Id
    AND t.term_id = county.Cis_319_Term_Id
    AND t.start_date = county.crt_order_sent_date
"""

US_ME_SENTENCE_TERM_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_ME_SENTENCE_TERM_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_ME_SENTENCE_TERM_VIEW_DESCRIPTION,
    view_query_template=US_ME_SENTENCE_TERM_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_SENTENCE_TERM_VIEW_BUILDER.build_and_print()
