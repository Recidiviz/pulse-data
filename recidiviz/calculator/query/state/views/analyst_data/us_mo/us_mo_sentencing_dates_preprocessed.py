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
"""Preprocessing file pulls on raw data from MO to get sentencing-relevant dates not yet ingested"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_NAME = "us_mo_sentencing_dates_preprocessed"

US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessing file pulls on raw data from MO to get
sentencing-relevant dates not yet ingested"""

US_MO_SENTENCING_DATES_PREPROCESSED_QUERY_TEMPLATE = """

WITH ME_date AS (
  -- Pulls latest Minimum Eligibility Date for each person-cycle. Only keeps ME dates that are non-zero and then
  -- dedups to the latest one in a given person-cycle
  -- TODO(#19221): Use ingested value when available
  SELECT CG_DOC AS doc_id,
         CG_CYC AS cycle_num,
         SAFE.PARSE_DATE('%Y%m%d',NULLIF(CG_MD,'0')) AS minimum_eligibility_date 
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK044_latest`
  WHERE CG_MD !='0'
  QUALIFY ROW_NUMBER() OVER(PARTITION BY CG_DOC, CG_CYC ORDER BY CAST(CG_ESN AS INT) DESC) = 1

), 
-- TODO(#19222): Use ingested values when available
PPR_date AS (
  WITH processed AS (
      /* This CTE flags valid "board determined dates" as those where the next action type is REL (Release)
      but that hearing is not followed by a hearing of type "CAN" (Cancel) or "CRE" (Extension) */
      SELECT doc_id, 
            cycle_num,
            seq_num,
            parole_hearing_date, 
            next_action_type,
            next_board_action,
            CASE WHEN next_action_type = 'REL' 
                 AND COALESCE(next_board_action,'NA') NOT IN ('CAN','CRE') 
                 THEN next_action_date
                 END AS board_determined_release_date,
           type_of_release,
           special_condition_needed,
      FROM (
        -- This CTE cleans up some date columns
        SELECT BQ_DOC AS doc_id,
             BQ_CYC AS cycle_num,
             BQ_BSN AS seq_num,
             BQ_PBA AS board_action,
             BQ_PBN AS next_action_type,
             BQ_RTC AS type_of_release,
             BQ_SCN AS special_condition_needed,
             -- Find the type of action in the next parole hearing
             LEAD(BQ_PBA) OVER(PARTITION BY BQ_DOC ORDER BY SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_PH,'0')) ASC) AS next_board_action,
             SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_NA,'0')) AS next_action_date,
             SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_PH,'0')) AS parole_hearing_date,
             SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_PR,'0')) AS parole_presumptive_date,
            FROM ( 
                SELECT * 
                        EXCEPT(BQ_NA,BQ_PH,BQ_PR),
                        /* Some dates have 00 in the day field. MO confirms that this happens on their end too,
                        and is usually for "placeholder" dates that eventually get corrected. They similarly turn these
                        into the first of the month */
                        CASE WHEN BQ_NA LIKE '%00' THEN CONCAT(LEFT(BQ_NA,4),SUBSTR(BQ_NA,5,2),'01') ELSE BQ_NA END AS BQ_NA,
                        CASE WHEN BQ_PH LIKE '%00' THEN CONCAT(LEFT(BQ_PH,4),SUBSTR(BQ_PH,5,2),'01') ELSE BQ_PH END AS BQ_PH,
                        CASE WHEN BQ_PR LIKE '%00' THEN CONCAT(LEFT(BQ_PR,4),SUBSTR(BQ_PR,5,2),'01') ELSE BQ_PR END AS BQ_PR,
                FROM 
                    `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK020_latest`
              ) 
        )
  )
    -- This final step only keeps the last available hearing per person-cycle where the next action was release.
    -- However if that was followed by a CAN or CRE hearing, the board_determined_release_date will be null
    SELECT *
    FROM processed
    WHERE next_action_type = 'REL'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY doc_id, cycle_num ORDER BY parole_hearing_date DESC) = 1
), MAX_CR AS (
  -- This table is already unique on person-cycle so no further deduping is done
  SELECT CV_DOC AS doc_id,
         CV_CYC As cycle_num,
         SAFE.PARSE_DATE('%Y%m%d',NULLIF(CV_AP,'0')) AS max_discharge, 
         SAFE.PARSE_DATE('%Y%m%d',NULLIF(CV_MR,'0')) AS conditional_release
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK071_latest`
)
SELECT ME_date.*, 
      PPR_date.board_determined_release_date, 
      PPR_date.special_condition_needed,
      PPR_date.seq_num,
      MAX_CR.max_discharge, 
      MAX_CR.conditional_release,
      pei.person_id
FROM ME_date
FULL OUTER JOIN PPR_date 
  USING(doc_id, cycle_num)
FULL OUTER JOIN MAX_CR 
  USING(doc_id, cycle_num)
INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
  ON doc_id = pei.external_id
  AND pei.state_code = 'US_MO'
"""

US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_MO_SENTENCING_DATES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO,
        instance=DirectIngestInstance.PRIMARY,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER.build_and_print()
