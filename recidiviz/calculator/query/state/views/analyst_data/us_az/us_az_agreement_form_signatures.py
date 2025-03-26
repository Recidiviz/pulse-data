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
# =============================================================================
"""A view that gathers information about TPR & DTP Agreement Forms, whether residents
have signed them, and the date on which they signed when applicable."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_NAME = "us_az_agreement_form_signatures"

US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_DESCRIPTION = """A view that gathers information about TPR & DTP Agreement Forms, whether residents
have signed them, and the date on which they signed when applicable."""

US_AZ_AGREEMENT_FORM_SIGNATURES_QUERY_TEMPLATE = f"""
WITH tpr_base AS (
  SELECT DISTINCT 
    ep.PERSON_ID,
    ep.DOC_ID,
    CASE  
    -- Resident refused to sign
        WHEN agrmt.IS_REFUSE_TO_SIGN = 'Y' THEN 'REFUSED TO SIGN' 
    -- Resident declined program participation
        WHEN agrmt.INMATE_DECLINES = 'Y' THEN 'DECLINED TO PARTICIPATE'
    -- Resident has signed
        WHEN agrmt.INMATE_SIGNATURE_ID IS NOT NULL AND agrmt.INMATE_SIG_NA != 'Y' THEN 'SIGNED'
        ELSE 'NOT SIGNED, NOT DECLINED'
    END AS SIG_STATUS,
    PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p',agrmt.CREATE_DTM) AS status_dtm,
    'TPR' AS program
  FROM
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` ep
  -- Only including DOC_IDs that appear inside the eligibility table
  INNER JOIN
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_ELIG_latest` elig
  USING
    (DOC_ID)
  LEFT JOIN
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_TRANSITION_PRG_AGRMNT_latest` agrmt
  USING
    (TRANSITION_PRG_ELIGIBILITY_ID)
  WHERE
    CAST(ep.ADMISSION_DATE AS DATETIME) > '1900-01-01'
),
dtp_base AS (
    SELECT DISTINCT 
    ep.PERSON_ID,
    ep.DOC_ID,
    CASE  
    -- Resident refused to sign
        WHEN agrmt.IS_REFUSE_TO_SIGN = 'Y' THEN 'REFUSED TO SIGN' 
    -- Resident declined program participation
        WHEN agrmt.INMATE_DECLINES = 'Y' THEN 'DECLINED TO PARTICIPATE'
    -- Resident has signed
        WHEN agrmt.INMATE_SIGNATURE_ID IS NOT NULL AND agrmt.INMATE_SIG_NA != 'Y' THEN 'SIGNED'
        ELSE 'NOT SIGNED, NOT DECLINED'
    END AS SIG_STATUS,
    PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p',agrmt.CREATE_DTM) AS status_dtm,
    'DTP' AS program
  FROM
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.DOC_EPISODE_latest` ep
  -- Only including DOC_IDs that appear inside the eligibility table
  INNER JOIN
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_DRUG_TRANSITION_PRG_AGRMNT_latest` agrmt
  USING
    (DOC_ID)
  WHERE
    CAST(ep.ADMISSION_DATE AS DATETIME) > '1900-01-01'
),
both_programs AS (
    SELECT * FROM tpr_base
    UNION ALL 
    SELECT * FROM dtp_base
),
status_joined_to_sessions AS (
SELECT 
    sjip.person_id, 
    'US_AZ' AS state_code,
    base.doc_id,
    cs.session_id,
    base.program,
    base.SIG_STATUS,
    base.status_dtm,
    cs.start_date, 
    cs.end_date_exclusive
FROM 
    both_programs base
JOIN 
    `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` sjip
ON
    (SPLIT(sjip.external_id, '-')[SAFE_OFFSET(1)] = base.DOC_ID)
JOIN 
    `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
ON 
    (cs.person_id = sjip.person_id 
    AND cs.start_date = sjip.admission_date)
-- Remove 0-day periods
WHERE 
   sjip.admission_date != {nonnull_end_date_clause("sjip.release_date")}
)
SELECT DISTINCT
    * 
FROM
    status_joined_to_sessions
-- Only keep the most recent update
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID, DOC_ID, SESSION_ID, PROGRAM ORDER BY STATUS_DTM DESC) = 1
"""

US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_NAME,
    description=US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_DESCRIPTION,
    view_query_template=US_AZ_AGREEMENT_FORM_SIGNATURES_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ,
        instance=DirectIngestInstance.PRIMARY,
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_BUILDER.build_and_print()
