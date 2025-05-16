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
"""Projected sentence dates and EGT credits in MA"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MA_PROJECTED_DATES_VIEW_NAME = "us_ma_projected_dates"
US_MA_PROJECTED_DATES_VIEW_DESCRIPTION = """Updates to projected dates and credits earned
based on the MA EGT table. Used to hydrate person_projected_date_sessions instead of standard
sentence ingest, which isn't practical given the shape of MA EGT data."""
US_MA_PROJECTED_DATES_QUERY_TEMPLATE = """
/*
A person (as identified by COMMIT_NO) can have multiple rows of EGT date for a given report 
(as identified by RPT_RUN_DATE), one for each activity that may contribute to EGT credits 
over the relevant period. However, within a given report, the total credit columns (TOTAL_STATE_CREDIT 
and TOTAL_COMPLETION_CREDIT) and projected date columns (RTS_DATE, ORIGINAL_MAX_RELEASE_DATE, 
and ADJUSTED_MAX_RELEASE_DATE) will all be the same in each row an individual has, so we 
can retrieve the credit totals and projected dates for a given report simply by using a 
SELECT DISTINCT over the relevant columns.
*/
SELECT DISTINCT
  'US_MA' AS state_code,
  person_id,
  COMMIT_NO AS external_id,
  CAST(RPT_RUN_DATE AS DATETIME) AS update_date,
  CAST(EGT_INITIAL_DATE AS DATETIME) AS egt_initial_date,
  -- If someone hasn't earned any EGT credit at the time of a report, the total credit values 
  -- will be '0' unless the person hasn't started earning EGT whatsoever, in which case
  -- the total credit will be blank. The latter case still means that the person has 0 days
  -- of EGT credit, so the blank values get set to 0 here for consistency.
  IF(TOTAL_STATE_CREDIT='',0,CAST(TOTAL_STATE_CREDIT AS NUMERIC)) AS TOTAL_STATE_CREDIT,
  IF(TOTAL_COMPLETION_CREDIT='',0,CAST(TOTAL_COMPLETION_CREDIT AS NUMERIC)) AS TOTAL_COMPLETION_CREDIT,
  CAST(RTS_DATE AS DATETIME) AS RTS_DATE,
  CAST(ORIGINAL_MAX_RELEASE_DATE AS DATETIME) AS ORIGINAL_MAX_RELEASE_DATE,
  CAST(ADJUSTED_MAX_RELEASE_DATE AS DATETIME) AS ADJUSTED_MAX_RELEASE_DATE
FROM `{project_id}.{us_ma_raw_data_up_to_date_dataset}.egt_report_latest` egt
LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
ON egt.COMMIT_NO = pei.external_id AND pei.state_code='US_MA'
"""
US_MA_PROJECTED_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_MA_PROJECTED_DATES_VIEW_NAME,
    view_query_template=US_MA_PROJECTED_DATES_QUERY_TEMPLATE,
    description=US_MA_PROJECTED_DATES_VIEW_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ma_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MA,
        instance=DirectIngestInstance.PRIMARY,
    ),
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MA_PROJECTED_DATES_VIEW_BUILDER.build_and_print()
