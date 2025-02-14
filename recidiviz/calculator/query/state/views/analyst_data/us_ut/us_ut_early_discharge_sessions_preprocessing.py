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
"""Utah state-specific preprocessing for early discharge sessions.

We do not observe an Early Termination (ET) flag in the Utah data, so we infer
early discharges by doing the following:
- The person had a successful termination
- The person had an early termination report submitted within 12 months of their
  release date
- The person was discharged to liberty"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
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

US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_ut_early_discharge_sessions_preprocessing"
)

US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """Utah state-specific preprocessing for early discharge sessions"""
)

DISCHARGE_REPORT_DIFF_MONTHS = "12"
# TODO(#38326) - Don't count folks terminated within 30 days of their projected date
US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH early_termination_reports AS (
  SELECT 
    DISTINCT
      peid.state_code,
      peid.person_id,
      SAFE_CAST(LEFT(r.rpt_dt, 10) AS DATE) AS report_date,
  FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.wf_rpt_latest` r
  INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.wf_sbjct_cd_latest`
    USING(rpt_sbjct_id)
  INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.wf_typ_cd_latest`
    USING(rpt_typ_id)
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = r.ofndr_num
      AND peid.state_code = 'US_UT'
      AND peid.id_type = 'US_UT_DOC'
  -- Subject is SUPERVISION GUIDELINE - EARLY TERMINATION REVIEW
  WHERE IFNULL(rpt_sbjct_id, '') = '11'
    -- Type is TERMINATION OF PAROLE REQUEST
    OR IFNULL(rpt_typ_id, '') = '9'
),

successful_terminations AS (
  SELECT 
    DISTINCT 
      state_code, 
      person_id, 
      termination_date,
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period`
  WHERE state_code = 'US_UT'
    AND termination_reason_raw_text = 'DISCHARGED/SUCCESSFUL'
)

SELECT 
  cs.state_code,
  cs.person_id,
  cs.session_id,
  cs.end_date AS ed_date,
  cs.end_date,
  0 AS discharge_to_session_end_days, 
  cs.outflow_to_level_1,
FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
-- We only count it as an ET if there is an ET report
INNER JOIN early_termination_reports etr
  ON etr.person_id = cs.person_id
    AND etr.state_code = cs.state_code
    AND etr.report_date BETWEEN start_date AND end_date
-- We only count it as an ET if the person was successfully terminated
INNER JOIN successful_terminations st
  ON st.person_id = cs.person_id
    AND st.state_code = cs.state_code
    AND st.termination_date = end_date_exclusive
WHERE cs.state_code = 'US_UT'
  AND cs.outflow_to_level_1 = 'LIBERTY'
  AND cs.compartment_level_1 = 'SUPERVISION'
  -- We only count it as an ET if a report was submitted less than 12 months from release
  AND DATE_DIFF(cs.end_date, etr.report_date, DAY)/30 < {DISCHARGE_REPORT_DIFF_MONTHS}
-- If there's more than one report per session, keep the latest one
QUALIFY ROW_NUMBER() OVER(PARTITION BY cs.state_code, cs.person_id, cs.session_id ORDER BY etr.report_date DESC)= 1
"""

US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_UT, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()
