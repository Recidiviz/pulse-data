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
"""Helpful constants for constructing sessions using product roster archives"""

# Indicate states where product roster uses office names instead of district id's
# as the location id
from datetime import datetime
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.workflows.types import WorkflowsSystemType

STATES_WITH_OFFICE_NAME_LOCATION_DEFAULT = ["US_ND", "US_CA"]


def get_query_fragment_for_role_types_by_system_type(
    role_types_by_system_type_dict: Dict[WorkflowsSystemType, List[str]]
) -> str:
    """Converts a dict of system types and their associated roster role types
    into a SQL CASE WHEN query fragment"""
    case_when_query_fragment = "\n            ".join(
        [
            f"""
            WHEN role IN ({list_to_query_string(role_types_by_system_type_dict[system_type], quoted = True)})
            THEN "{system_type.value}" """
            for system_type in role_types_by_system_type_dict
        ]
    )
    return f"""
        CASE
            {case_when_query_fragment}
        END"""


def get_provisioned_user_registration_sessions_view_builder(
    product_name: str,
    first_validated_roster_date: datetime,
    role_types_by_system_type_dict: Dict[WorkflowsSystemType, List[str]],
) -> SimpleBigQueryViewBuilder:
    """Generates a view builder that constructs contiguous periods of registration
    for a given product surface and its specified primary user role types."""

    # Throw an error if product_name is not a supported product
    supported_products = ["INSIGHTS", "WORKFLOWS"]
    if product_name.upper() not in supported_products:
        raise ValueError(
            "Primary user registration sessions not available for "
            f"{product_name}. Supported products: {supported_products}"
        )

    product_name_str = product_name.lower()
    first_validated_roster_date_str = first_validated_roster_date.strftime("%Y-%m-%d")
    role_types = sorted(set().union(*role_types_by_system_type_dict.values()))

    view_name = f"{product_name.lower()}_provisioned_user_registration_sessions"

    view_description = f"""View that represents attributes about every primary user
of the {product_name.title()} tool, with spans reflecting historical product roster
information where available"""

    query_template = f"""
# Get the first product archive export date for each user after the first validated
# product roster archive export date, and backfill over all time
WITH product_roster_sessions AS (
    SELECT
        * EXCEPT(email_address), email_address AS {product_name_str}_user_email_address
    FROM
        `{{project_id}}.analyst_data.product_roster_archive_sessions_materialized`
    WHERE
        has_{product_name_str}_access
        #TODO(#32772): Once we use validation for flagging null email addresses to clean
        # up product roster, this condition can be removed
        AND email_address IS NOT NULL
)
,
backfilled_product_roster_sessions AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        DATE("{MAGIC_START_DATE}") AS start_date,
        # Use first validated product roster date as the end date of backfill session
        DATE("{first_validated_roster_date_str}") AS end_date_exclusive,
        roles_as_string,
        location_id,
    FROM
        product_roster_sessions
    WHERE
        # Backfill any archive session that overlaps the roster validation date
        "{first_validated_roster_date_str}" BETWEEN start_date AND {nonnull_end_date_clause("end_date_exclusive")}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, {product_name_str}_user_email_address ORDER BY start_date) = 1
)
,
# Union together backfilled information with roster information
unioned_roster_sessions AS (
    SELECT
        * 
    FROM
        backfilled_product_roster_sessions

    UNION ALL

    SELECT
        state_code,
        {product_name_str}_user_email_address,
        # If start date is before first validated archive date, truncate
        # This ensures no overlap between backfilled sessions and roster sessions
        GREATEST(start_date, "{first_validated_roster_date_str}") AS start_date,
        end_date_exclusive,
        roles_as_string,
        location_id,
    FROM
        product_roster_sessions
    WHERE
        {nonnull_end_date_clause("end_date_exclusive")} > "{first_validated_roster_date_str}"
)
,
# Pull out flags for system_type and is_primary_user based on role type
roster_sessions_with_role_flags AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        {get_query_fragment_for_role_types_by_system_type(role_types_by_system_type_dict)} AS system_type,
        location_id,
        LOGICAL_OR(role IN ({list_to_query_string(role_types, quoted = True)})) AS is_primary_user,
    FROM
        unioned_roster_sessions,
        UNNEST(SPLIT(roles_as_string, ",")) AS role
    GROUP BY 1, 2, 3, 4, 5, 6
)
,
registration_sessions AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        DATE({product_name_str}_registration_date) AS start_date,
        CAST(NULL AS DATE) AS end_date_exclusive,
    FROM
        `{{project_id}}.analyst_data.{product_name_str}_user_auth0_registrations_materialized`
)
,
inferred_facility_location_sessions AS (
    SELECT
        a.state_code,
        LOWER(b.email) AS {product_name_str}_user_email_address,
        a.primary_facility AS facility_inferred,
        a.start_date,
        a.end_date_exclusive,
    FROM
        `{{project_id}}.sessions.incarceration_staff_inferred_location_sessions_materialized` a
    LEFT JOIN `{{project_id}}.normalized_state.state_staff` b
    ON
        incarceration_staff_id = staff_id
)
,
ingested_facility_location_sessions AS (
    SELECT
        a.state_code,
        LOWER(s.email) AS {product_name_str}_user_email_address,
        start_date,
        end_date AS end_date_exclusive,
        location_external_id AS facility_ingested,
    FROM
        `{{project_id}}.normalized_state.state_staff_location_period` a
    LEFT JOIN
        `{{project_id}}.reference_views.location_metadata_materialized` b
    USING
        (state_code, location_external_id)
    LEFT JOIN `{{project_id}}.normalized_state.state_staff` s
    USING
        (state_code, staff_id)
    WHERE
        location_type = "STATE_PRISON"
)
,
registration_sessions_with_inferred_location AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        system_type,
        TRUE AS is_provisioned,
        NULL AS is_registered,
        is_primary_user,
        location_id,
        CAST(NULL AS STRING) AS facility_inferred,
        CAST(NULL AS STRING) AS facility_ingested,
    FROM
        roster_sessions_with_role_flags
    UNION ALL
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        NULL AS system_type,
        NULL AS is_provisioned,
        TRUE is_registered,
        NULL AS is_primary_user,
        CAST(NULL AS STRING) location_id,
        CAST(NULL AS STRING) AS facility_inferred,
        CAST(NULL AS STRING) AS facility_ingested,
    FROM
        registration_sessions
    UNION ALL
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        CAST(NULL AS STRING) AS system_type,
        NULL AS is_provisioned,
        NULL AS is_registered,
        NULL AS is_primary_user,
        CAST(NULL AS STRING) AS location_id,
        facility_inferred,
        CAST(NULL AS STRING) AS facility_ingested,
    FROM
        inferred_facility_location_sessions
    UNION ALL
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        CAST(NULL AS STRING) AS system_type,
        NULL AS is_provisioned,
        NULL AS is_registered,
        NULL AS is_primary_user,
        CAST(NULL AS STRING) AS location_id,
        CAST(NULL AS STRING) AS facility_inferred,
        facility_ingested,
    FROM
        ingested_facility_location_sessions
)
,
{create_sub_sessions_with_attributes(
    table_name="registration_sessions_with_inferred_location",
    index_columns=["state_code", f"{product_name_str}_user_email_address"],
    end_date_field_name="end_date_exclusive",
)}
,
sub_sessions_dedup AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        MAX(system_type) AS system_type,
        MAX(location_id) AS location_id,
        MAX(is_registered) AS is_registered,
        MAX(is_primary_user) AS is_primary_user,
        MAX(facility_inferred) AS facility_inferred,
        MAX(facility_ingested) AS facility_ingested,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
    HAVING LOGICAL_OR(is_provisioned)
)
,
aggregated_registration_sessions_with_inferred_location AS (
    {aggregate_adjacent_spans(
        table_name='sub_sessions_dedup',
        index_columns=["state_code", f"{product_name_str}_user_email_address"],
        attribute=['system_type', 'location_id', 'is_registered', 'is_primary_user', 'facility_inferred', 'facility_ingested'],
        end_date_field_name='end_date_exclusive',
    )}
)
SELECT
    registration_sessions.state_code,
    registration_sessions.{f"{product_name_str}_user_email_address"},
    start_date,
    end_date_exclusive,
    system_type,
    IFNULL(is_registered, FALSE) AS is_registered,
    is_primary_user,
    staff.staff_id,
    staff_external_id.external_id AS staff_external_id,
    -- Convert the location_id to supervision district for states
    -- that use office as the location ID in the product roster
    ANY_VALUE(
        CASE system_type
        WHEN "SUPERVISION" THEN COALESCE(supervision_district, location_id, location_external_id)
        WHEN "INCARCERATION" THEN COALESCE(facility, facility_ingested, facility_inferred, location_id)
        ELSE location_id END
    ) AS location_id,
    ANY_VALUE(
        CASE system_type
        WHEN "SUPERVISION" THEN COALESCE(supervision_district_name, location_metadata.location_name)
        WHEN "INCARCERATION" THEN COALESCE(facility_name, location_metadata.location_name) END
    ) AS location_name,
FROM
    aggregated_registration_sessions_with_inferred_location registration_sessions
LEFT JOIN
    `{{project_id}}.normalized_state.state_staff` staff
ON
    registration_sessions.state_code = staff.state_code
    AND LOWER(registration_sessions.{product_name_str}_user_email_address) = LOWER(staff.email)
LEFT JOIN
    `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff_external_id
USING
    (staff_id)
LEFT JOIN
    `{{project_id}}.reference_views.location_metadata_materialized` location_metadata
ON
    location_metadata.state_code = registration_sessions.state_code
    AND location_metadata.location_external_id = COALESCE(location_id, facility_ingested, facility_inferred)
LEFT JOIN
    `{{project_id}}.sessions.session_location_names_materialized` AS sessions
ON
    registration_sessions.state_code = sessions.state_code
    AND (
        -- Supervision locations joining on district
        (
            system_type = "SUPERVISION" 
            AND location_id = supervision_district 
            AND registration_sessions.state_code NOT IN ({list_to_query_string(STATES_WITH_OFFICE_NAME_LOCATION_DEFAULT, quoted=True)})
        )
        -- Supervision locations joining on office name
        OR (
            system_type = "SUPERVISION" 
            AND location_id = supervision_office_name 
            AND registration_sessions.state_code IN ({list_to_query_string(STATES_WITH_OFFICE_NAME_LOCATION_DEFAULT, quoted=True)})
        )
        -- Incarceration locations joining on facility
        OR (
            system_type = "INCARCERATION" 
            AND (
                sessions.facility = COALESCE(location_id, facility_ingested, facility_inferred)
                OR sessions.facility_name = location_metadata.location_name
            )
        )
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    """
    return SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=view_name,
        description=view_description,
        view_query_template=query_template,
        should_materialize=True,
    )
