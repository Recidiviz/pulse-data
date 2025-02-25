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
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_NAME = "us_mo_sentencing_dates_preprocessed"

US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessing file pulls on raw data from MO to get
sentencing-relevant dates not yet ingested"""

US_MO_SENTENCING_DATES_PREPROCESSED_QUERY_TEMPLATE = """
WITH ME_MM_dates AS (
  -- Pulls latest Minimum Eligibility Date and Minimum Mandatory Release Date for each 
  -- person-cycle. For each date type, only keeps dates that are non-zero and then
  -- dedups to the latest one of that date type in a given person-cycle.
  -- TODO(#19221): Use ingested value when available
  SELECT 
    CG_DOC AS doc_id,
    CG_CYC AS cycle_num,
    SAFE.PARSE_DATE('%Y%m%d',CG_MD) AS minimum_eligibility_date,
    SAFE.PARSE_DATE('%Y%m%d',CG_MM) AS minimum_mandatory_release_date 
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK044_latest`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CG_DOC, CG_CYC ORDER BY CAST(CG_ESN AS INT64) DESC) = 1
), 
cleaned_hearing_data AS (
  SELECT BQ_DOC AS doc_id,
        BQ_CYC AS cycle_num,
        BQ_BSN AS seq_num,
        BQ_PBA AS board_action,
        BQ_PBN AS next_action_type,
        BQ_RTC AS type_of_release,
        BQ_SCN AS special_condition_needed,
        -- Find the type of action in the next parole hearing
        LEAD(BQ_PBA) OVER(PARTITION BY BQ_DOC ORDER BY SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_PH,'0')) ASC) AS next_board_action,
        LEAD(BQ_PBN) OVER(PARTITION BY BQ_DOC ORDER BY SAFE.PARSE_DATE('%Y%m%d',NULLIF(BQ_PH,'0')) ASC) AS next_next_action_type,
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
),
rch_cycles AS (
  -- Reconsideration dates can, under certain conditions, be used for prioritized dates,
  -- but only if the most recent parole hearing set a date for a reconsideration hearing 
  -- and the person has already had a first parole hearing or a reconsideration hearing.
  SELECT 
    doc_id, 
    cycle_num, 
    FIRST_VALUE(next_action_date) OVER (
      PARTITION BY doc_id, cycle_num
      ORDER BY parole_hearing_date DESC
    ) AS rch_date
  FROM (
    SELECT *
    FROM cleaned_hearing_data
    QUALIFY FIRST_VALUE(next_action_type) OVER (
      PARTITION BY doc_id, cycle_num 
      ORDER BY parole_hearing_date DESC
    ) = 'RCH'
  )
  QUALIFY LOGICAL_OR(board_action IN ('FPH','RCH')) OVER (PARTITION BY doc_id,cycle_num)
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
                -- If the next action type is 'REL' and the next parole board action (obtained via LEAD) isn't a cancellation/extension,
                -- it's still possible that the next parole board action sets a new next_action_type that replaces the planned release action.
                -- If the BQ_PBH value that follows a release date being set is FPH (first parole hearing), NPR (no parole), CRE (extension),
                -- or RCH (reconsideration hearing), then we can infer that the board determined release date has been replaced. Note that
                -- this approach risks overlooking correct board determined release dates in historical data, as data from subsequent cycles
                -- may be treated as release-overriding events. Therefore, when/if ingesting this data, keep in mind that this is only
                -- accurate for determining someone's CURRENT board determined release date. 
                AND COALESCE(next_next_action_type,'NA') NOT IN ('FPH','NPR','CRE','RCH') 
                THEN next_action_date
                END AS board_determined_release_date,
           type_of_release,
           special_condition_needed,
      FROM cleaned_hearing_data
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
         SAFE.PARSE_DATE('%Y%m%d',NULLIF(CV_MR,'0')) AS conditional_release,
         CASE WHEN CV_AP = '99999999' THEN TRUE ELSE FALSE END AS life_flag
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.LBAKRDTA_TAK071_latest`
)

SELECT 
COALESCE(ME_MM_dates.doc_id, PPR_date.doc_id, MAX_CR.doc_id) AS doc_id,
COALESCE(ME_MM_dates.cycle_num, PPR_date.cycle_num, MAX_CR.cycle_num) AS cycle_num,
ME_MM_dates.minimum_eligibility_date, 
ME_MM_dates.minimum_mandatory_release_date,
      PPR_date.board_determined_release_date, 
      PPR_date.special_condition_needed,
      PPR_date.seq_num,
      MAX_CR.max_discharge, 
      MAX_CR.conditional_release,
      MAX_CR.life_flag,
      rch_cycles.rch_date,
      pei.person_id
FROM ME_MM_dates
FULL OUTER JOIN PPR_date 
  USING(doc_id, cycle_num)
FULL OUTER JOIN MAX_CR 
  USING(doc_id, cycle_num)
LEFT JOIN rch_cycles 
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
