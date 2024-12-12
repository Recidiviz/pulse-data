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

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_NAME = "us_az_home_plan_preprocessed"

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_DESCRIPTION = """A view that gathers information about home plan preparedness for people incarcerated
in Arizona and attaches it to the relevant incarceration period."""

US_AZ_HOME_PLAN_PREPROCESSED_QUERY_TEMPLATE = f"""
WITH base AS (
SELECT DISTINCT
    DOC_ID,
    hp.HOME_PLAN_ID, 
    CAST(det.HOME_PLAN_DETAIL_ID AS INT64) AS HOME_PLAN_DETAIL_ID,
    MAX(CAST(det.HOME_PLAN_DETAIL_ID AS INT64)) OVER (PARTITION BY DOC_ID) AS MAX_HOME_PLAN_DETAIL_ID,
    IS_HOMELESS_REQUEST, 
    CASE  
        WHEN status.DESCRIPTION = 'Address Approved' THEN 'Home Plan Approved'
        WHEN status.DESCRIPTION = 'Cancelled' THEN 'Home Plan Cancelled'
        WHEN app.IS_RETURN_TO_COIII = 'Y' THEN 'Return to COIII'
        WHEN app.IS_RE_INVESTIGATE = 'Y' THEN 'Return to CC Supervisor'
        WHEN status.DESCRIPTION = 'Address Denied' AND (app.IS_RETURN_TO_COIII !='Y' or app.IS_RETURN_TO_COIII IS NULL) THEN 'Denied' 
        WHEN status.description IN ('CCO SUP', 'COIII', 'CCO', 'COIV', 'Release Unit', 'CCL') THEN 'Home Plan In Progress'
        WHEN status.DESCRIPTION IS NULL THEN 'Home Plan Not Started'
    END AS PLAN_STATUS,
    CASE  
    -- AZ told us to use the date value from the APPROVAL table if and only if the plan was approved. 
        WHEN status.DESCRIPTION = 'Address Approved' AND COALESCE(app.UPDT_DTM, app.CREATE_DTM) IS NOT NULL THEN COALESCE(app.UPDT_DTM, app.CREATE_DTM) 
    -- In cases where there are no dates in the APPROVAL table for approved plans, use dates from the DETAIL table.
        WHEN status.DESCRIPTION = 'Address Approved' AND COALESCE(app.UPDT_DTM, app.CREATE_DTM) IS NULL THEN COALESCE(det.UPDT_DTM, det.CREATE_DTM) 
    -- If a plan has not been started, there will not be a row in the DETAIL table. Use dates from the base table.
        WHEN status.DESCRIPTION IS NULL THEN COALESCE(hp.UPDT_DTM, hp.CREATE_DTM)
    -- Otherwise, we get the date from the DETAIL table that signifies a plan was submitted.
        ELSE COALESCE(det.UPDT_DTM, det.CREATE_DTM) 
    END AS UPDATE_DATE
FROM 
-- Base home plan table
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_HOME_PLAN_latest` hp
LEFT JOIN 
-- There will be a new row in this table each time an individual home plan is resubmitted
-- for consideration after correction. 
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_HOME_PLAN_DETAIL_latest` det
ON
    (hp.HOME_PLAN_ID = det.HOME_PLAN_ID 
    AND hp.ACTIVE_FLAG = 'Y' 
    AND (det.ACTIVE_FLAG = 'Y' OR det.ACTIVE_FLAG IS NULL))
LEFT JOIN 
-- Rows in this table are relevant if and only if a plan is approved. 
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.AZ_DOC_HOME_PLAN_APPROVAL_latest` app
ON
    (det.HOME_PLAN_DETAIL_ID = app.HOME_PLAN_DETAIL_ID
    AND (app.ACTIVE_FLAG = 'Y' OR app.ACTIVE_FLAG IS NULL)
    AND (det.ACTIVE_FLAG = 'Y' OR det.ACTIVE_FLAG IS NULL))
LEFT JOIN 
    `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.LOOKUPS_latest` status
ON
    (det.APPROVAL_STATUS_ID = status.LOOKUP_ID)
QUALIFY 
-- This is the most recent set of details for this person's home plan
   (CAST(det.HOME_PLAN_DETAIL_ID AS INT64) = MAX(CAST(det.HOME_PLAN_DETAIL_ID AS INT64)) OVER (PARTITION BY DOC_ID) OR det.HOME_PLAN_DETAIL_ID IS NULL)
-- Limits to the most recent set of dates associated with a plan's approval, where applicable.
-- This is necessary because there can be many rows in the APPROVAL table for each HOME_PLAN_DETAIL_ID entry. 
    AND (COALESCE(app.UPDT_DTM, app.CREATE_DTM) = MAX(COALESCE(app.UPDT_DTM, app.CREATE_DTM)) OVER (PARTITION BY det.HOME_PLAN_DETAIL_ID) 
    OR COALESCE(app.UPDT_DTM, app.CREATE_DTM) IS NULL)
),
status_joined_to_sessions AS (
SELECT 
    sjip.person_id, 
    base.doc_id,
    cs.session_id,
    base.is_homeless_request,
    base.plan_status,
    base.update_date,
    cs.start_date, 
    cs.end_date_exclusive
FROM 
    base
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
