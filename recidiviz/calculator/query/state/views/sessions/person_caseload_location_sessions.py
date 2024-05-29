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
"""
View representing assignments for all states using default logic. For all default
states, we should validate that the results of this assignment table for the current
day snapshot align with the location and caseload information in person_record."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_NAME = "person_caseload_location_sessions"

PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_DESCRIPTION = """
View representing assignments for all states using default logic. For all default
states, we should validate that the results of this assignment table for the current
day snapshot align with the location and caseload information in person_record.

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	compartment_level_1	|	Level 1 Compartment	|
|	state_code	|	State	|
|	start_date	|	Start day of session	|
|	end_date_exclusive	|	Day that session ends	|
|	caseload_id	|	Caseload under which the person may be surfaced for a Workflows opportunity (see below for further details)	|
|	location_id	|	Location to which the person is associated (see below for further details)	|

The logic for deriving caseload and location differs by compartment:
* Supervision: caseload_id = supervising_officer_external_id; location_id = supervision_district
* Incarceration: caseload_id = facility; location_id = "<facility>, <housing_unit>"
    * Some states (US_IX, US_ME, US_TN) don't use housing_unit in person_record.location, so for those states, we use location_id = facility 
"""

NO_HOUSING_UNIT_STATES = ["US_IX", "US_ME", "US_TN"]
INCARCERATION_STAFF_STATES = ["US_ME"]


PERSON_CASELOAD_LOCATION_SESSIONS_QUERY_TEMPLATE = f"""
WITH sub_sessions AS (
    SELECT 
        person_id,
        state_code,
        start_date,
        end_date_exclusive,
        compartment_level_1,
        CASE
            WHEN compartment_level_1 LIKE 'SUPERVISION%' THEN supervising_officer_external_id
            WHEN compartment_level_1 LIKE 'INCARCERATION%' THEN facility
            END
            AS caseload_id,
        CASE
            WHEN compartment_level_1 LIKE 'SUPERVISION%' THEN supervision_district_name
            WHEN compartment_level_1 LIKE 'INCARCERATION%' THEN
                CASE
                    WHEN state_code IN ('{{no_housing_unit_states}}') THEN facility
                    ELSE ARRAY_TO_STRING([facility, housing_unit], ', ')
                END
            END
            AS location_id,
    FROM
        `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE NOT (
        state_code IN ('{{incarceration_staff_states}}')
        AND compartment_level_1 LIKE 'INCARCERATION%'
    )
    
    UNION ALL
    
    -- US_ME, INCARCERATION
    (
    WITH us_me_incarceration AS (
        -- facility and housing_unit come from compartment_sub_sessions
        SELECT
            person_id,
            state_code,
            start_date,
            end_date_exclusive,
            compartment_level_1,
            facility,
            housing_unit,
            NULL AS incarceration_staff_assignment_external_id,
        FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
            WHERE state_code = 'US_ME' AND compartment_level_1 LIKE 'INCARCERATION%'
    
        UNION ALL
    
        -- incarceration_staff_assignment_external_id comes from compartment_sub_sessions
        SELECT
            person_id,
            state_code,
            start_date,
            end_date_exclusive,
            CAST(NULL AS STRING) AS compartment_level_1,
            CAST(NULL AS STRING) AS facility,
            CAST(NULL AS STRING) AS housing_unit,
            incarceration_staff_assignment_external_id,
        FROM `{{project_id}}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized`
        WHERE state_code = 'US_ME'
    ),
    -- Merge the two sources of data, each of which contributes different columns
    {create_sub_sessions_with_attributes(table_name='us_me_incarceration', end_date_field_name='end_date_exclusive')}
    SELECT
        person_id,
        state_code,
        start_date,
        end_date_exclusive,
        -- this field should be non-NULL in the first table in the UNION ALL and NULL in the second table
        MAX(compartment_level_1) AS compartment_level_1,
        -- this field should be non-NULL in the second table in the UNION ALL and NULL in the first table 
        MAX(incarceration_staff_assignment_external_id) AS caseload_id,
        -- these fields should be non-NULL in the first table in the UNION ALL and NULL in the second table
        MAX(
            CASE
                WHEN state_code IN ('{{no_housing_unit_states}}') THEN facility
                ELSE ARRAY_TO_STRING([facility, housing_unit], ', ')
            END
        ) AS location_id
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    -- this ensures that we only select spans that are incarceration periods according to compartment_sub_sessions
    HAVING compartment_level_1 IS NOT NULL
    )
),
sessionized_cte AS
(
{aggregate_adjacent_spans(
    table_name='sub_sessions',
    attribute=['compartment_level_1', 'caseload_id', 'location_id'],
    end_date_field_name='end_date_exclusive'
)}
)
SELECT
    person_id,
    state_code,
    start_date,
    end_date_exclusive,
    compartment_level_1,
    caseload_id,
    location_id
FROM sessionized_cte
"""

PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_NAME,
    description=PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=PERSON_CASELOAD_LOCATION_SESSIONS_QUERY_TEMPLATE,
    no_housing_unit_states="', '".join(NO_HOUSING_UNIT_STATES),
    incarceration_staff_states="', '".join(INCARCERATION_STAFF_STATES),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
