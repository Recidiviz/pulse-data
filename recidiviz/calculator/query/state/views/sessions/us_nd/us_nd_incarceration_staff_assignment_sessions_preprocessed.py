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
"""Sessions for incarceration staff caseloads in ND"""
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

US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_nd_incarceration_staff_assignment_sessions_preprocessed"
)

US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Sessions for incarceration staff caseloads in ND"""
)

US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
#TODO(#32753): Ingest these mappings
WITH elite_OffenderProgramProfiles_generated_view AS (
    SELECT * FROM `{{project_id}}.{{usnd_raw_data_up_to_date_dataset}}.elite_OffenderProgramProfiles_latest`
),
elite_ProgramServices_generated_view AS (
    SELECT * FROM `{{project_id}}.{{usnd_raw_data_up_to_date_dataset}}.elite_ProgramServices_latest`
),
recidiviz_elite_CourseActivities_generated_view AS (
    SELECT * FROM `{{project_id}}.{{usnd_raw_data_up_to_date_dataset}}.recidiviz_elite_CourseActivities_latest`
),
officer_assignments AS (
  -- Combine all of the views above and get case manager assignments
  SELECT
      REPLACE(REPLACE(OFFENDER_BOOK_ID, '.00',''), ',', '') AS external_id,
      SAFE_CAST(LEFT(OFFENDER_START_DATE, 10) AS DATE) AS start_date,
      SAFE_CAST(LEFT(OFFENDER_END_DATE, 10) AS DATE) AS end_date,
      SPLIT(ca.DESCRIPTION, ', ')[ORDINAL(2)] AS given_names,
      SPLIT(ca.DESCRIPTION, ', ')[ORDINAL(1)] AS surname,
  FROM
    elite_OffenderProgramProfiles_generated_view opp
  LEFT JOIN
    elite_ProgramServices_generated_view ps
  USING
    (PROGRAM_ID)
  LEFT JOIN
    recidiviz_elite_CourseActivities_generated_view ca
  USING
    (CRS_ACTY_ID)
  WHERE ps.DESCRIPTION = 'ASSIGNED CASE MANAGER'
), 

staff_ids AS (
  -- Get staff names and IDs
  SELECT 
        REPLACE(REPLACE(STAFF_ID, '.00',''), ',', '') AS staff_id,
        FIRST_NAME AS given_names,
        LAST_NAME AS surname,
        USER_ID AS user_id,
        PERSONNEL_TYPE,
        POSITION,
        STATUS,
  FROM `{{project_id}}.{{usnd_raw_data_up_to_date_dataset}}.recidiviz_elite_staff_members_latest`
  WHERE PERSONNEL_TYPE = 'STAFF'
),

officer_assignments_with_staff_and_person_ids AS (
    SELECT 
        peid2.*,
        oa.* EXCEPT(external_id),
        oa.start_date AS original_start_date,
        si.*,
        sei.staff_id AS incarceration_staff_assignment_id,
    FROM officer_assignments oa
    LEFT JOIN staff_ids si
    # TODO(#31389) Find another way to join other than string matching, 
        USING(given_names, surname)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON oa.external_id = peid.external_id
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
    ON peid.person_id = peid2.person_id
        AND peid2.state_code = 'US_ND'
        AND peid2.id_type = 'US_ND_ELITE'
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` sei
    ON si.staff_id = sei.external_id
        AND sei.state_code = "US_ND"
        AND sei.id_type = 'US_ND_ELITE_OFFICER'
    WHERE si.staff_id IS NOT NULL
),
{create_sub_sessions_with_attributes('officer_assignments_with_staff_and_person_ids')}

SELECT 
    "US_ND" AS state_code,
    person_id,
    external_id AS person_external_id,
    start_date,
    end_date AS end_date_exclusive,
    incarceration_staff_assignment_id,
    staff_id AS incarceration_staff_assignment_external_id,
    "INCARCERATION_STAFF" AS incarceration_staff_assignment_role_type,
    -- We are only surfacing folks who have a person assigned, so we can assume they are counselors/CMs
    "COUNSELOR" AS incarceration_staff_assignment_role_subtype,
    ROW_NUMBER() OVER (PARTITION BY person_id, start_date, end_date
                /* Prioritize cases in the following order 
                        1) Staff is active (as opposed to inactive)
                        2) NULL end_date 
                        3) Later assignment dates */
                        ORDER BY
                                IF(STATUS ='ACTIVE', 0, 1),
                                IF(end_date IS NULL, 0, 1),
                                original_start_date DESC
                        ) AS case_priority 
FROM sub_sessions_with_attributes
"""

US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    usnd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
