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
"""View representing spans of time over which a Workflows user could
theoretically access information about a set of clients in the Workflows tool,
based on state-level permissioning."""
import os
from typing import Literal, Optional

import attr

import recidiviz
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.yaml_dict import YAMLDict

_VIEW_NAME = "workflows_user_person_assignment_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time over which a Workflows user could
theoretically access information about a set of clients in the Workflows tool,
based on state-level permissioning.

Users can access clients via three different kinds of access levels:
1) User can only view their own caseload
2) User can view any client in their district
3) User can view any client in their state

The view below takes as input a yaml file specifying which access level is
relevant to each state + compartment (supervision vs. facilities workflows).

## Field Definitions
|   Field   |   Description |
|   --------------------    |   --------------------    |
|   email_address   |   Unique workflows user identifier    |
|   state_code  |   State   |
|   start_date  |   Start day of session    |
|   end_date_exclusive  |   Day that session ends   |
|   person_id   |   Client associated with this workflows user  |
"""


@attr.define(frozen=True)
class AccessConfig:
    state_code: str
    compartment: str
    access_filter: Literal["state", "district", "caseload"]
    is_supervision_officer: Optional[bool] = None
    has_null_district: Optional[bool] = None


WORKFLOWS_USER_ACCESS_CONFIG_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "calculator/query/state/views/workflows/workflows_user_access_config.yaml",
)
WORKFLOWS_USER_ACCESS_CONFIG = [
    AccessConfig(**config)
    for config in YAMLDict.from_path(WORKFLOWS_USER_ACCESS_CONFIG_PATH).pop_list(
        "config", dict
    )
]

# The primary keys associated with each type of access filter
ACCESS_FILTER_PRIMARY_KEYS = {
    "state": ["state_code"],
    "district": ["state_code", "district"],
    "caseload": ["state_code", "email_address"],
}


def get_single_access_config_query_fragment(config: AccessConfig) -> str:
    """Returns a query fragment for a single line item in a configuration of access levels."""

    # get the rows from provisioned user registration sessions and join to appropriate table
    state_filter = f'state_code = "{config.state_code}"'
    compartment_filter = f"AND system_type = '{config.compartment}'"
    role_type_filter = ""
    if (
        hasattr(config, "is_supervision_officer")
        and config.is_supervision_officer is not None
    ):
        role_type_filter = (
            "AND is_primary_user"
            if config.is_supervision_officer
            else "AND NOT is_primary_user"
        )
    district_filter = ""
    if hasattr(config, "has_null_district") and config.has_null_district is not None:
        district_filter = (
            "AND district IS NULL"
            if config.has_null_district
            else "AND district IS NOT NULL"
        )
    mapping_view_cte = f"{config.access_filter}_mapping"

    # Get the primary keys associated with the level of access for this config
    access_filter_keys = ACCESS_FILTER_PRIMARY_KEYS[config.access_filter]

    table_1_columns = [
        x
        for x in ["email_address", "system_type", "is_primary_user", "district"]
        if x not in access_filter_keys
    ]

    # The query template joins to a person-level table based on the configured access level
    # For example, for the district-wide access level, this will join each user to all
    # clients in the district. For the state-wide access level, this will join each user
    # to every client in the state.
    return f"""
# {config.state_code} {config.compartment} access level: {config.access_filter}
SELECT
    person_id,
    state_code,
    start_date,
    end_date_exclusive,
    email_address,
    system_type,
    is_primary_user,
FROM (
{create_intersection_spans(
    table_1_name="provisioned_user_sessions",
    table_2_name=mapping_view_cte,
    index_columns=access_filter_keys,
    table_1_columns=table_1_columns,
    table_2_columns=["person_id"]
)}
)
WHERE
    -- TODO(#36827): Incorporate feature variant override
    {state_filter}
    {compartment_filter}
    {role_type_filter}
    {district_filter}
"""


def get_workflows_user_caseload_access_sessions_view_builder(
    configs: list[AccessConfig],
) -> SimpleBigQueryViewBuilder:
    """Returns view builder that generates spans of time over which a workflows
    user had access to a given person_id according to the configured access levels
    in their state."""

    all_config_query_fragments = "\nUNION ALL\n".join(
        [get_single_access_config_query_fragment(config) for config in configs]
    )

    query_template = f"""
WITH provisioned_user_sessions AS (
    SELECT
        state_code,
        workflows_user_email_address AS email_address,
        location_id AS district,
        system_type,
        is_primary_user,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
)
,
state_mapping AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.sessions.system_sessions_materialized`
)
,
district_mapping AS (
    SELECT
        state_code,
        person_id,
        supervision_district AS district,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.sessions.location_sessions_materialized`
)
,
caseload_mapping AS (
    SELECT
        a.state_code,
        a.person_id,
        a.supervising_officer_external_id,
        a.start_date,
        a.end_date_exclusive,
        c.email AS email_address,
    FROM
        `{{project_id}}.sessions.supervision_officer_sessions_materialized` a
    LEFT JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
    ON
        a.supervising_officer_external_id = b.external_id
    LEFT JOIN
        `{{project_id}}.normalized_state.state_staff` c
    ON
        b.staff_id = c.staff_id
)
{all_config_query_fragments}
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=query_template,
        clustering_fields=["state_code"],
        should_materialize=True,
    )


WORKFLOWS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER = (
    get_workflows_user_caseload_access_sessions_view_builder(
        WORKFLOWS_USER_ACCESS_CONFIG
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER.build_and_print()
