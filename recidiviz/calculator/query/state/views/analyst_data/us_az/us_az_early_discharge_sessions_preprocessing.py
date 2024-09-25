# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Arizona state-specific preprocessing for early releases from incarceration sessions"""

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

US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_az_early_discharge_sessions_preprocessing"
)

US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = """Arizona state-specific preprocessing for early releases from incarceration sessions"""

DISCHARGE_SESSION_DIFF_DAYS = "7"

US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH state_sentence_with_raw_data AS (
  SELECT
    sent.state_code,
    sent.external_id, 
    sent.person_id,
    sent.imposed_date,
    sc_episode.SC_EPISODE_ID, 
    sc_episode.DOC_ID, 
    sc_episode.MANDATORY_RELEASE_FLAG,
    CASE 
      WHEN MANUAL_LOCK_FLAG = 'Y' AND ADJ_RLS_DTS_FLAG = 'Y' THEN sc_episode.CURR_RLS_TYPE_ID_ARD
      WHEN MANUAL_LOCK_FLAG = 'Y' AND ADJ_RLS_DTS_FLAG != 'Y' THEN sc_episode.CURR_RLS_TYPE_ID_ML
      WHEN ADJ_RLS_DTS_FLAG = 'Y' AND MANUAL_LOCK_FLAG != 'Y' THEN CURR_RLS_TYPE_ID_ARD
      ELSE CURR_RELEASE_TYPE_ID
    END AS CURR_RELEASE_TYPE_ID
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_sentence` sent
  LEFT JOIN `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.AZ_DOC_SC_OFFENSE_latest` off
  ON(sent.external_id = off.OFFENSE_ID)
  LEFT JOIN `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.AZ_DOC_SC_COMMITMENT_latest` commit ON (off.COMMITMENT_ID = commit.COMMITMENT_ID)
  LEFT JOIN `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.AZ_DOC_SC_EPISODE_latest` sc_episode ON (commit.SC_EPISODE_ID = sc_episode.SC_EPISODE_ID)
  WHERE sent.state_code = 'US_AZ'
),

tpr_releases AS (
  -- Releases that happened earlier than expected
  SELECT 
    base.state_code,
    base.person_id,
    inc_period.release_date,
    lookups.DESCRIPTION AS release_type,
  FROM state_sentence_with_raw_data base
  LEFT JOIN `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.LOOKUPS_latest` lookups
    ON(CURR_RELEASE_TYPE_ID = LOOKUP_ID)
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` inc_period
  ON(inc_period.person_id=base.person_id AND
    base.DOC_ID = SPLIT(inc_period.external_id, '-')[SAFE_OFFSET(1)] 
    AND inc_period.state_code = 'US_AZ')
  WHERE lookups.DESCRIPTION IN ("TPR")
)

SELECT 
    ses.state_code,
    ses.person_id,
    ses.session_id,
    tpr.release_date,
    ses.end_date,
    ABS(DATE_DIFF(tpr.release_date, ses.end_date, DAY)) AS discharge_to_session_end_days,
    ses.outflow_to_level_1,
FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` ses
INNER JOIN tpr_releases tpr
  USING(state_code, person_id)
WHERE ses.state_code = 'US_AZ'
  AND ses.compartment_level_1 = 'INCARCERATION'
  AND ses.outflow_to_level_1 = 'SUPERVISION'
  AND tpr.release_date BETWEEN ses.start_date AND {nonnull_end_date_clause('ses.end_date_exclusive')}
  AND ABS(DATE_DIFF(tpr.release_date, ses.end_date, DAY)) <= CAST({{discharge_session_diff_days}} AS INT64)
QUALIFY ROW_NUMBER() OVER(PARTITION BY ses.person_id, ses.session_id, ses.state_code
    ORDER BY tpr.release_date DESC) = 1
"""

US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    discharge_session_diff_days=DISCHARGE_SESSION_DIFF_DAYS,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()
