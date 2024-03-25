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
    STATE_BASE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_NAME = "us_az_home_plan_preprocessed"

US_AZ_HOME_PLAN_PREPROCESSED_VIEW_DESCRIPTION = """A view that gathers information about home plan preparedness for people incarcerated
in Arizona."""

US_AZ_HOME_PLAN_PREPROCESSED_QUERY_TEMPLATE = """
WITH base AS (
SELECT DISTINCT 
    DOC_ID, 
    HOME_PLAN_DETAIL_ID, 
    HOME_PLAN_ID, 
    IS_HOMELESS_REQUEST, 
    lookups.DESCRIPTION AS APPROVAL_STATUS,
    MAX(app.CREATE_DTM) OVER (PARTITION BY HOME_PLAN_DETAIL_ID) AS APPROVAL_DATE
FROM {project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_latest hp
LEFT JOIN {project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_DETAIL_latest det
USING(HOME_PLAN_ID)
LEFT JOIN {project_id}.{raw_data_up_to_date_views_dataset}.AZ_DOC_HOME_PLAN_APPROVAL_latest app
USING(HOME_PLAN_DETAIL_ID)
LEFT JOIN {project_id}.{raw_data_up_to_date_views_dataset}.LOOKUPS_latest lookups
ON(det.APPROVAL_STATUS_ID = LOOKUP_ID)
), base_with_status AS (
SELECT 
    DOC_ID, 
    HOME_PLAN_DETAIL_ID,
    IS_HOMELESS_REQUEST,
    CASE WHEN 
        APPROVAL_STATUS IN ('CCO SUP', 'COIII', 'CCO', 'COIV', 'Release Unit') THEN 'HOME PLAN IN PROGRESS'
        WHEN APPROVAL_STATUS IS NULL THEN 'HOME PLAN NOT STARTED'
        WHEN APPROVAL_STATUS = 'Address Approved' THEN 'HOME PLAN APPROVED'
        WHEN APPROVAL_STATUS = 'Address Denied' THEN 'HOME PLAN DENIED'
        WHEN APPROVAL_STATUS = 'Cancelled' THEN 'HOME PLAN CANCELLED'
    END AS PLAN_STATUS,
    UPDATE_DATE
FROM base
), status_joined_to_IP AS (
    SELECT *, ip.incarceration_period_id, ip.person_id
    FROM base_with_status
    -- Normalized state dataset is empty in AZ still, so use base state instead.
    LEFT JOIN {project_id}.{state_dataset}.state_incarceration_period ip
    ON(SPLIT(ip.external_id, '-')[OFFSET(1)] = base_with_status.DOC_ID)
)
SELECT * FROM status_joined_to_IP
"""

INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_HOME_PLAN_PREPROCESSED_VIEW_NAME,
    description=US_AZ_HOME_PLAN_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_AZ_HOME_PLAN_PREPROCESSED_QUERY_TEMPLATE,
    state_dataset=STATE_BASE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
