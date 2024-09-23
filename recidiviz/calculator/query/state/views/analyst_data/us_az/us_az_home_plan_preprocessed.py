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
"""A view that gathers information about home plan preparedness for people incarcerated
in Arizona and attaches it to the relevant incarceration period."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
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

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_NAME = "us_az_home_plan_preprocessed"

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_DESCRIPTION = """A view that gathers information about home plan preparedness for people incarcerated
in Arizona and attaches it to the relevant incarceration period."""

US_AZ_HOME_PLAN_PREPROCESSED_QUERY_TEMPLATE = """
WITH base AS (
SELECT * FROM (
    SELECT DISTINCT
        DOC_ID,
        HOME_PLAN_ID, 
        HOME_PLAN_DETAIL_ID,
        MAX(HOME_PLAN_DETAIL_ID) OVER (PARTITION BY DOC_ID) AS MAX_HOME_PLAN_DETAIL_ID,
        IS_HOMELESS_REQUEST, 
        lookups.DESCRIPTION AS APPROVAL_STATUS, 
        det.CREATE_DTM,
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_latest` hp
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_DETAIL_latest` det
    USING(HOME_PLAN_ID)
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.LOOKUPS_latest` lookups
    ON(det.APPROVAL_STATUS_ID = lookups.LOOKUP_ID)
    AND hp.active_flag = 'Y'
    )
WHERE HOME_PLAN_DETAIL_ID = MAX_HOME_PLAN_DETAIL_ID
), 
base_with_dates AS (
SELECT DISTINCT
  base.* EXCEPT (CREATE_DTM),
  CASE WHEN 
    APPROVAL_STATUS = 'Address Approved'
        THEN MAX(app.CREATE_DTM) OVER (PARTITION BY HOME_PLAN_DETAIL_ID) 
    ELSE base.CREATE_DTM
  END AS UPDATE_DATE
FROM base 
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_APPROVAL_latest` app
USING(HOME_PLAN_DETAIL_ID)
),
dates_with_status AS (
SELECT 
    DOC_ID, 
    HOME_PLAN_DETAIL_ID,
    IS_HOMELESS_REQUEST,
    CASE WHEN 
        APPROVAL_STATUS IN ('CCO SUP', 'COIII', 'CCO', 'COIV', 'Release Unit', 'CCL') THEN 'HOME PLAN IN PROGRESS'
        WHEN APPROVAL_STATUS IS NULL THEN 'HOME PLAN NOT STARTED'
        WHEN APPROVAL_STATUS = 'Address Approved' THEN 'HOME PLAN APPROVED'
        WHEN APPROVAL_STATUS = 'Address Denied' THEN 'HOME PLAN DENIED'
        WHEN APPROVAL_STATUS = 'Cancelled' THEN 'HOME PLAN CANCELLED'
    END AS PLAN_STATUS,
    UPDATE_DATE
FROM base_with_dates
), 
status_joined_to_IP AS (
SELECT 
    dates_with_status.*, 
    ip.admission_date AS ip_adm_date, 
    ip.person_id
FROM dates_with_status
LEFT JOIN `{project_id}.{normalized_state_dataset}.state_incarceration_period` ip
ON(SPLIT(ip.external_id, '-')[SAFE_OFFSET(1)] = dates_with_status.DOC_ID)
),
status_joined_to_sessions AS (
SELECT 
    sjip.person_id, 
    sjip.doc_id,
    cs.session_id,
    sjip.is_homeless_request,
    sjip.plan_status,
    sjip.update_date,
    cs.start_date, 
    cs.end_date_exclusive
FROM status_joined_to_IP sjip
INNER JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
ON (cs.person_id = sjip.person_id 
AND cs.start_date = sjip.ip_adm_date)
)
SELECT DISTINCT * 
FROM status_joined_to_sessions
"""

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_HOME_PLAN_PREPROCESSED_VIEW_NAME,
    description=US_AZ_HOME_PLAN_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_AZ_HOME_PLAN_PREPROCESSED_QUERY_TEMPLATE,
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
        US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER.build_and_print()
