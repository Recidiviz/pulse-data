# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates a view for collapsing raw ID urine analysis data into contiguous periods where positive urine analysis was the most recent result"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_NAME = (
    "us_id_positive_urine_analysis_sessions"
)

US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_DESCRIPTION = """View of periods of time over which the most recent urine analysis test had a positive result"""

US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_QUERY_TEMPLATE = """
    WITH
      deduped_urine_analysis_data AS (
      SELECT
        p.state_code,
        ofndr_num AS person_external_id,
        person_id,
        SAFE_CAST(smpl_rqst_dt AS DATE) urine_analysis_date,
        LOGICAL_OR(sbstnc_found_flg = 'Y') AS is_positive_result,
      FROM `{project_id}.us_id_raw_data_up_to_date_views.sbstnc_tst_latest` s
      LEFT JOIN `{project_id}.us_id_raw_data_up_to_date_views.sbstnc_rslt_latest`
      USING (tst_id)
      LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` p
      ON s.ofndr_num = p.external_id
        AND p.state_code = 'US_ID'
      WHERE smpl_typ_cd = 'U'
      GROUP BY p.state_code, person_id, person_external_id, urine_analysis_date 
      )
    SELECT
      state_code,
      person_external_id,
      person_id,
      urine_analysis_date AS positive_urine_analysis_date,
      result_end_date
    FROM (
      SELECT
        state_code,
        person_external_id,
        person_id,
        urine_analysis_date,
        is_positive_result,
        LEAD(urine_analysis_date) OVER(PARTITION BY person_id ORDER BY urine_analysis_date) result_end_date
      FROM
        deduped_urine_analysis_data 
    )
    WHERE is_positive_result
      AND urine_analysis_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 20 YEAR)

"""

US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_NAME,
    base_dataset=STATE_BASE_DATASET,
    description=US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_BUILDER.build_and_print()
