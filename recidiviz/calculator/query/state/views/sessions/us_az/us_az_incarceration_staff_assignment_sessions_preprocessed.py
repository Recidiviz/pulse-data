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
"""Sessions for incarceration staff caseloads in AZ"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_az_incarceration_staff_assignment_sessions_preprocessed"
)

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Sessions for incarceration staff caseloads in AZ"""
)

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
#TODO(#32753): Ingest these mappings
WITH officer_assignments AS (
SELECT 
    INMATE_PERSON_ID, 
    AGENT_PERSON_ID, 
    CAST(DATE_ASSIGNED AS DATETIME) AS start_date, 
    CAST(DATE_DEASSIGNED AS DATETIME) AS end_date, 
    ACTIVE_FLAG
FROM `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.AZ_CM_INMATE_ASSIGNMENT_latest`
),
officer_assignments_with_staff_and_person_ids AS (
SELECT
  "US_AZ" AS state_code,
  pei.person_id AS person_id,
  INMATE_PERSON_ID AS person_external_id,
  start_date,
  end_date,
  sei.staff_id AS incarceration_staff_assignment_id,
  AGENT_PERSON_ID AS incarceration_staff_assignment_external_id,
  ACTIVE_FLAG
  FROM officer_assignments
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
  ON(INMATE_PERSON_ID = pei.external_id AND pei.id_type = 'US_AZ_PERSON_ID')
  INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` sei
  ON(AGENT_PERSON_ID = sei.external_id AND sei.id_type = 'US_AZ_PERSON_ID')
),
{create_sub_sessions_with_attributes('officer_assignments_with_staff_and_person_ids')}

SELECT 
    state_code,
    person_id,
    person_external_id,
    start_date,
    end_date AS end_date_exclusive,
    incarceration_staff_assignment_id,
    incarceration_staff_assignment_external_id,
    'INCARCERATION_STAFF' AS incarceration_staff_assignment_role_type,
    'COUNSELOR' AS incarceration_staff_assignment_role_subtype,
    ROW_NUMBER() OVER (PARTITION BY person_id, start_date, end_date
        /* Prioritize cases in the following order 
                1) Staff is active (as opposed to inactive)
                2) NULL end_date 
                3) Later assignment dates */
                ORDER BY
                        IF(ACTIVE_FLAG ='Y', 0, 1),
                        IF(end_date IS NULL, 0, 1),
                        start_date DESC
                ) AS case_priority
FROM sub_sessions_with_attributes
"""

US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
