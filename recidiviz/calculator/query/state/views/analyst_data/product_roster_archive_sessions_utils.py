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
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
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


def get_primary_user_registration_sessions_view_builder(
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

    view_name = f"{product_name.lower()}_primary_user_registration_sessions"

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
    WHERE has_{product_name_str}_access
)
,
backfilled_product_roster_sessions AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        DATE("{MAGIC_START_DATE}") AS start_date,
        # Use first validated product roster date as the end date of backfill session
        # If product roster archive session only starts after 
        # PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE, use the start date of the archive
        GREATEST(start_date, "{first_validated_roster_date_str}") AS end_date_exclusive,
        roles_as_string,
        location_id,
    FROM
        product_roster_sessions
    WHERE
        "{first_validated_roster_date_str}" < {nonnull_end_date_clause("end_date_exclusive")}
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
# Filter to specified role types and pull out a system_type flag based on role type
primary_user_roster_sessions AS (
    SELECT
        state_code,
        {product_name_str}_user_email_address,
        start_date,
        end_date_exclusive,
        {get_query_fragment_for_role_types_by_system_type(role_types_by_system_type_dict)} AS system_type,
        location_id,
    FROM
        unioned_roster_sessions,
    UNNEST(SPLIT(roles_as_string, ",")) AS role
    # Filter to only primary user types
    WHERE
        role IN ({list_to_query_string(role_types, quoted = True)})
)
,
aggregated_roster_sessions AS (
    {aggregate_adjacent_spans(
        table_name='primary_user_roster_sessions',
        index_columns=["state_code", f"{product_name_str}_user_email_address"],
        attribute=['system_type', 'location_id'],
        session_id_output_name='registration_session_id',
        end_date_field_name='end_date_exclusive'
    )}
)
,
registration_sessions AS (
    -- Truncate all registration sessions to start after the first signup/login event
    SELECT
        a.state_code,
        a.{product_name_str}_user_email_address,
        GREATEST(a.start_date, b.{product_name_str}_registration_date) AS start_date,
        a.end_date_exclusive,
        a.registration_session_id,
        a.system_type,
        a.location_id,
    FROM
        aggregated_roster_sessions a
    # Only include roster sessions that overlap with the period following auth0 registration
    INNER JOIN
        `{{project_id}}.analyst_data.{product_name_str}_user_auth0_registrations_materialized` b
    ON
        a.state_code = b.state_code
        AND a.{product_name_str}_user_email_address = b.{product_name_str}_user_email_address
        AND b.{product_name_str}_registration_date < {nonnull_end_date_clause("a.end_date_exclusive")}
)
SELECT
    registration_sessions.*,
    ANY_VALUE(
        CASE system_type 
        WHEN "SUPERVISION" THEN supervision_district_name 
        WHEN "INCARCERATION" THEN facility_name END
    ) AS location_name,
FROM
    registration_sessions
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
            AND location_id = facility
        )
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7
    """
    return SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=view_name,
        description=view_description,
        view_query_template=query_template,
        should_materialize=True,
    )