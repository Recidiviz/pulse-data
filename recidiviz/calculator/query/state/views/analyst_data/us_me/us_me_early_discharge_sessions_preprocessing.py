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
"""Maine state-specific preprocessing for early discharge sessions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME = (
    "us_me_early_discharge_sessions_preprocessing"
)

US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION = (
    """Maine state-specific preprocessing for early discharge sessions"""
)

_DEATH_REGEX_FILTER = "|".join(["DIED", "PASSED", "DECEASED", "DEATH"])

DISCHARGE_SESSION_DIFF_DAYS = "7"

US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE = f"""
WITH early_terminations_me AS (
  SELECT 
    DISTINCT 
      Cis_400_Cis_100_Client_Id, 
      SAFE_CAST(LEFT(Comm_Override_Rel_Date, 10) AS DATE) AS ed_date,
  FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_401_CRT_ORDER_HDR_latest` crt
  LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_400_CHARGE_latest` ch
    ON ch.Charge_Id = crt.Cis_400_Charge_Id 
    AND ch.Cis_100_Client_Id = crt.Cis_400_Cis_100_Client_Id
  -- Only keep 'Early Termination' cases (code 119) and exclude Juvenile cases
  WHERE Cis_4009_Comm_Override_Rsn_Cd = '119'
    AND ch.Juvenile_Ind !='Y'
    -- Some POs have filed ETs when the cause was death
    AND NOT REGEXP_CONTAINS(UPPER(COALESCE(Comm_Override_Notes_Tx, '')), r'{_DEATH_REGEX_FILTER}')
),

probation_sessions_me AS (
  SELECT 
    sessions.*,
    pei.external_id
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
      ON sessions.person_id = pei.person_id
      AND sessions.state_code = pei.state_code
      AND pei.id_type = 'US_ME_DOC'
  WHERE compartment_level_1 = 'SUPERVISION'
    AND compartment_level_2 = 'PROBATION'
    AND sessions.state_code = 'US_ME'
),

probation_sessions_ed AS (
  SELECT 
    ses.state_code,
    ses.person_id,
    ses.session_id,
    et.ed_date,
    ses.end_date,
    ABS(DATE_DIFF(et.ed_date, ses.end_date, DAY)) AS discharge_to_session_end_days,
    ses.outflow_to_level_1,
  FROM early_terminations_me et
  INNER JOIN probation_sessions_me ses
    ON ses.external_id = et.Cis_400_Cis_100_Client_Id
    AND ed_date BETWEEN start_date AND {nonnull_end_date_clause('ses.end_date_exclusive')}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY ses.person_id, ses.session_id, ses.state_code
    ORDER BY et.ed_date DESC) = 1
)

SELECT 
  *
FROM probation_sessions_ed s
WHERE discharge_to_session_end_days <= CAST({{discharge_session_diff_days}} AS INT64) 
"""

US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_NAME,
    description=US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_DESCRIPTION,
    view_query_template=US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_QUERY_TEMPLATE,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    discharge_session_diff_days=DISCHARGE_SESSION_DIFF_DAYS,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.build_and_print()
