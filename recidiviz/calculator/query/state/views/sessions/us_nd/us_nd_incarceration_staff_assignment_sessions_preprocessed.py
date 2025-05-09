# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
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
facility_sessions AS (
    SELECT 
        state_code,
        person_id,
        facility,
        MIN(start_date) AS start_date,
        MAX(end_date_exclusive) AS end_date_exclusive,  
    FROM `{{project_id}}.sessions.housing_unit_sessions_materialized`
    WHERE state_code = 'US_ND'
    GROUP BY 1,2,3
),
officer_assignments AS (
  -- Combine all of the views above and get case manager assignments
    SELECT
        peid.person_id,
        REPLACE(REPLACE(OFFENDER_BOOK_ID, '.00',''), ',', '') AS external_id,
        SAFE_CAST(LEFT(OFFENDER_START_DATE, 10) AS DATE) AS start_date,
        -- If the end date is NULL and the person was released from a prior incarceration 
        -- after the assignment started, we set the assignment’s end date to that release date.
        LEAST(
            {nonnull_end_date_clause('SAFE_CAST(LEFT(OFFENDER_END_DATE, 10) AS DATE)')},
            {nonnull_end_date_clause('iss.end_date')}
        ) AS end_date,
        SPLIT(ca.DESCRIPTION, ', ')[ORDINAL(2)] AS given_names,
        SPLIT(ca.DESCRIPTION, ', ')[ORDINAL(1)] AS surname,
        ca.AGY_LOC_ID AS facility,
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
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
        ON REPLACE(REPLACE(OFFENDER_BOOK_ID, '.00',''), ',', '') = peid.external_id
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    LEFT JOIN `{{project_id}}.sessions.incarceration_super_sessions_materialized` iss
        USING(state_code, person_id)
    WHERE ps.DESCRIPTION = 'ASSIGNED CASE MANAGER'
        AND SAFE_CAST(LEFT(OFFENDER_START_DATE, 10) AS DATE) BETWEEN iss.start_date AND {nonnull_end_date_clause('iss.end_date')}
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
        peid2.state_code,
        peid2.person_id,
        peid2.external_id,
        oa.start_date,
        oa.end_date,
        oa.start_date AS original_start_date,
        si.staff_id,
        si.STATUS,
        sei.staff_id AS incarceration_staff_assignment_id,
        'OFFICER' AS row_type,
        oa.facility,
    FROM officer_assignments oa
    LEFT JOIN staff_ids si
    # TODO(#31389) Find another way to join other than string matching, 
        USING(given_names, surname)
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid2
    ON oa.person_id = peid2.person_id
        AND peid2.state_code = 'US_ND'
        AND peid2.id_type = 'US_ND_ELITE'
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` sei
    ON si.staff_id = sei.external_id
        AND sei.state_code = "US_ND"
        AND sei.id_type = 'US_ND_ELITE_OFFICER'
    WHERE si.staff_id IS NOT NULL

    UNION ALL 

    -- When someone is in a CJ, we should override the original assignment
    SELECT 
        hu.state_code,
        hu.person_id,
        peid.external_id,
        hu.start_date,
        hu.end_date_exclusive AS end_date,
        hu.start_date AS original_start_date,
        IF(
            REGEXP_CONTAINS(hu.housing_unit, r'(CJ-LRJ|CJ-BAR|CJ-HACTC)'),
            'CJ2', -- Lake Region, Barnes and Heart of America Correctional and Treatment Center in Rugby
            'CJ1' -- Burleigh, Ward, and McKenzie
        ) AS staff_id,
        'ACTIVE' AS STATUS,
        CAST(NULL AS INT64) AS incarceration_staff_assignment_id,
        'COUNTY_JAIL' AS row_type,
        hu.facility,
    FROM `{{project_id}}.sessions.housing_unit_sessions_materialized` hu
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON hu.state_code = peid.state_code
        AND hu.person_id = peid.person_id
        AND peid.id_type = 'US_ND_ELITE'
    WHERE hu.state_code = 'US_ND'
    AND hu.facility = 'CJ'
    AND REGEXP_CONTAINS(
        hu.housing_unit,
        r'(CJ-LRJ|CJ-BAR|CJ-HACTC|CJ-BUR|CJ-WAR|CJ-MCK)'
    )

),

officer_assignments_with_staff_and_person_ids_and_facility AS (
    SELECT 
        oaws.state_code,
        oaws.person_id,
        oaws.external_id,
        oaws.start_date,
        oaws.end_date,
        oaws.original_start_date,
        oaws.staff_id,
        oaws.STATUS,
        oaws.incarceration_staff_assignment_id,
        oaws.row_type,
        LOGICAL_OR(oaws.facility = fs.facility) AS jii_facility_matches_staff,
    FROM officer_assignments_with_staff_and_person_ids oaws
    LEFT JOIN facility_sessions fs
    ON fs.person_id = oaws.person_id
        AND fs.start_date < {nonnull_end_date_clause('oaws.end_date')}
        AND {nonnull_end_date_clause('fs.end_date_exclusive')} > oaws.start_date
    GROUP BY 1,2,3,4,5,6,7,8,9,10

),
{create_sub_sessions_with_attributes('officer_assignments_with_staff_and_person_ids_and_facility')}

SELECT 
    "US_ND" AS state_code,
    ss.person_id,
    ss.external_id AS person_external_id,
    ss.start_date,
    ss.end_date AS end_date_exclusive,
    ss.incarceration_staff_assignment_id,
    ss.staff_id AS incarceration_staff_assignment_external_id,
    "INCARCERATION_STAFF" AS incarceration_staff_assignment_role_type,
    -- We are only surfacing folks who have a person assigned, so we can assume they are counselors/CMs
    "COUNSELOR" AS incarceration_staff_assignment_role_subtype,
    ROW_NUMBER() OVER (PARTITION BY ss.person_id, ss.start_date, ss.end_date, ss.external_id
                /* Prioritize cases in the following order 
                        1) County Jail cases
                        2) Staff is active (as opposed to inactive)
                        3) NULL end_date 
                        4) Staff is assigned to the same facility as the person
                        5) Later assignment dates */
                        ORDER BY
                                IF(ss.row_type = 'COUNTY_JAIL', 0, 1),
                                IF(ss.STATUS ='ACTIVE', 0, 1),
                                IF(ss.end_date IS NULL, 0, 1),
                                ss.jii_facility_matches_staff DESC,
                                ss.original_start_date DESC
                        ) AS case_priority,
FROM sub_sessions_with_attributes ss
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
