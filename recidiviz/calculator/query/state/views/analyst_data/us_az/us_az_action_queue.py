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
"""A view that decodes and gathers the most relevant pieces of raw data that feed the 
Action Queue screen in ACIS."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_ACTION_QUEUE_VIEW_NAME = "us_az_action_queue"

US_AZ_ACTION_QUEUE_VIEW_DESCRIPTION = """A view that decodes and gathers
the most relevant pieces of raw data that feed the Action Queue screen in ACIS."""

US_AZ_ACTION_QUEUE_QUERY_TEMPLATE = """
WITH action_queue AS (
SELECT
  RECORD_ID, 
  PERSON_ID, 
  DOC_ID, 
  ADC_NUMBER,
  COALESCE(SAFE.PARSE_DATETIME('%m/%d/%Y', CREATE_DATE), CAST(CREATE_DATE AS DATETIME)) AS CREATE_DATE, 
  COALESCE(SAFE.PARSE_DATETIME('%m/%d/%Y', DUE_DATE), CAST(DUE_DATE AS DATETIME)) AS DUE_DATE, 
  COALESCE(SAFE.PARSE_DATETIME('%m/%d/%Y', CLOSE_DATE), CAST(CLOSE_DATE AS DATETIME)) AS CLOSE_DATE, 
  rec.ACTIVE_FLAG,type.description AS type,  
  cat.description AS category, 
  reasons.description AS reason, 
  due_day_lookup.description as due_day_type, 
  source_table_name
FROM `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_AQ_RECORD_latest` rec
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_AQ_TEMPLATE_latest` temp USING (TEMPLATE_ID)
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_AQ_REASON_latest`  reasons USING (REASON_ID)
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_AQ_TYPE_latest` type USING (TYPE_ID)
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.AZ_AQ_CATEGORY_latest`  cat USING(CATEGORY_ID)
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.LOOKUPS_latest` due_day_lookup ON (DUE_DAY_TYPE_ID = LOOKUP_ID)
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.PERSON_latest` person USING (PERSON_ID)
),
agent AS (
  SELECT
    dpp.PERSON_ID,
    dpp.DOC_ID,
    dpp.DPP_ID,
    dpp.office_location_id,
    loc.location_name,
    dpp.AGENT_ID,
    dpp_agent.SURNAME AS AGENT_SURNAME,
    dpp_agent.FIRST_NAME AS AGENT_FIRSTNAME
  FROM `{project_id}.{raw_data_up_to_date_views_dataset}.DPP_INTAKE_ASSIGNMENT_latest` dpp
  LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.PERSON_latest` dpp_agent 
  ON (dpp_agent.PERSON_ID = dpp.AGENT_ID)
  LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.DPP_OFFICE_LOCATION_latest` loc
  ON (dpp.OFFICE_LOCATION_ID = loc.OFFICE_LOCATION_ID)
  -- only keep the most recently assigned DPP_ID. There is no way to differentiate between
  -- supervision stints in the Action Queue tables, so we have to limit these results
  -- to what is current.
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY dpp.PERSON_ID ORDER BY CAST(dpp.CREATE_DTM AS DATETIME) DESC) = 1 
)

SELECT *
FROM action_queue
LEFT JOIN agent
USING(PERSON_ID, DOC_ID)
"""

US_AZ_ACTION_QUEUE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_ACTION_QUEUE_VIEW_NAME,
    description=US_AZ_ACTION_QUEUE_VIEW_DESCRIPTION,
    view_query_template=US_AZ_ACTION_QUEUE_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_ACTION_QUEUE_VIEW_BUILDER.build_and_print()
