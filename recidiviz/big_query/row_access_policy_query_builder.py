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
"""Query builder for BigQuery row access policies."""
import os
from typing import Any, Dict, List

import attr
from google.cloud import bigquery

import recidiviz.big_query.config as _bq_config_pkg
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import table_has_field
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.yaml_dict import YAMLDict

_RESTRICTED_ACCESS_YAML_PATH = os.path.join(
    os.path.dirname(_bq_config_pkg.__file__),
    "restricted_access_state_groups.yaml",
)

_GROUP_SETTINGS_YAML_PATH = os.path.join(
    os.path.dirname(_bq_config_pkg.__file__),
    "state_data_access_group_settings.yaml",
)


def _load_restricted_access_state_code_to_access_group() -> Dict[StateCode, str]:
    """Returns the restricted-state to access-group-email map from YAML config.

    The config file is excluded from the public pulse-data mirror, so this
    module must import cleanly without it. When the file is absent, this
    returns an empty map: build_row_access_policies then builds no policies,
    and the apply entrypoint fails loudly instead (fail closed).
    """
    if not os.path.exists(_RESTRICTED_ACCESS_YAML_PATH):
        return {}
    raw = YAMLDict.from_path(_RESTRICTED_ACCESS_YAML_PATH).raw_yaml
    return {StateCode(state_code): str(email) for state_code, email in raw.items()}


def _load_group_settings() -> Dict[str, str]:
    """Returns the non-per-state group settings from YAML config, or an empty
    map when the file is absent (it is excluded from the public pulse-data
    mirror, like the restricted-access config above)."""
    if not os.path.exists(_GROUP_SETTINGS_YAML_PATH):
        return {}
    raw = YAMLDict.from_path(_GROUP_SETTINGS_YAML_PATH).raw_yaml
    return {key: str(value) for key, value in raw.items()}


RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP: Dict[
    StateCode, str
] = _load_restricted_access_state_code_to_access_group()

_GROUP_SETTINGS: Dict[str, str] = _load_group_settings()

# These are empty strings only in the public pulse-data mirror, where the
# settings file is excluded; build_row_access_policies builds no policies
# there. The .get() calls (instead of []) exist for exactly that case.
BIG_QUERY_ADMINS_GROUP_EMAIL: str = _GROUP_SETTINGS.get(
    "big_query_admins_group_email", ""
)
DEFAULT_STATE_DATA_GROUP_EMAIL: str = _GROUP_SETTINGS.get(
    "default_state_data_group_email", ""
)


def access_group_configs_are_loaded() -> bool:
    """Returns whether every BQ access group config value is loaded. This is
    only False in the public pulse-data mirror, which excludes the config
    files."""
    return bool(
        RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP
        and BIG_QUERY_ADMINS_GROUP_EMAIL
        and DEFAULT_STATE_DATA_GROUP_EMAIL
    )


RESTRICTED_ACCESS_FIELDS = ["state_code", "region_code"]


@attr.define
class RowAccessPolicy:
    """Class representing a row access policy for a BigQuery table."""

    policy_id: str
    filter_predicate: str
    access_group_email: str | None
    table: bigquery.Table

    def to_create_query(self) -> str:
        return f"""CREATE OR REPLACE ROW ACCESS POLICY
                {self.policy_id}
                ON `{self.table.project}.{self.table.dataset_id}.{self.table.table_id}`
                GRANT TO ("group:{self.access_group_email}")
                FILTER USING ({self.filter_predicate});"""

    @classmethod
    def from_api_response(cls, policy: Dict[str, Any]) -> "RowAccessPolicy":
        """Creates a RowAccessPolicy object from the API response."""
        policy_reference = policy["rowAccessPolicyReference"]
        return cls(
            policy_id=policy_reference["policyId"],
            filter_predicate=policy["filterPredicate"],
            # TODO(#41565) Access group email is not returned in the API response
            access_group_email=None,
            table=bigquery.Table(
                table_ref=f'{policy_reference["projectId"]}.{policy_reference["datasetId"]}.{policy_reference["tableId"]}'
            ),
        )


def row_access_policy_lists_are_equivalent(
    existing_policies: List[RowAccessPolicy], new_policies: List[RowAccessPolicy]
) -> bool:
    """Compare two lists of row access policies for a table to see if they are the same.

    Args:
        existing_policies: List of existing row access policies to compare against.
        new_policies: List of new row access policies to compare against.
    """

    existing_policies_by_id = {p.policy_id: p for p in existing_policies}
    new_policies_by_id = {p.policy_id: p for p in new_policies}

    for policy_id in set(new_policies_by_id.keys()) | set(
        existing_policies_by_id.keys()
    ):
        if policy_id not in existing_policies_by_id:
            return False
        if policy_id not in new_policies_by_id:
            return False
        existing_policy = existing_policies_by_id[policy_id]
        new_policy = new_policies_by_id[policy_id]

        # TODO(#41470) Compare access group emails as well
        if new_policy.filter_predicate != existing_policy.filter_predicate:
            return False

    return True


def _get_state_code_column(table: bigquery.Table) -> str | None:
    for field in RESTRICTED_ACCESS_FIELDS:
        if table_has_field(table, field):
            return field

    return None


@attr.define
class RowAccessPolicyQueryBuilder:
    """Class used to build queries to create or drop row-level policies for a BigQuery table.
    Row-level permissions are only necessary for tables in state-specific datasets
    for states with restricted access requirements, or for tables with multiple states'
    data, indicated by the presence of a state_code or region_code field."""

    @classmethod
    def _build_row_access_policy_queries_for_state_agnostic_table(
        cls, table: bigquery.Table, state_code_column: str
    ) -> List[RowAccessPolicy]:
        """Builds row-level permissions policy for the provided table based on the state code
        in the provided |state_code_field| column.
        """
        policies: List[RowAccessPolicy] = []
        # Create policy to grant admins access to view, edit and copy ALL state data. We
        # do this first so that if applying permissions fails in the middle, admins
        # (including service accounts) still will have access.
        policies.append(
            RowAccessPolicy(
                policy_id=f"ADMIN_ACCESS_TO_ALL_STATE_DATA_{state_code_column.upper()}",
                table=table,
                access_group_email=BIG_QUERY_ADMINS_GROUP_EMAIL,
                filter_predicate="TRUE",
            )
        )

        for state_code in sorted(
            RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP, key=lambda s: s.value
        ):
            access_group_email = RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP[
                state_code
            ]
            # Create explicit row-level policies for states with rigid access control requirements.
            policies.append(
                RowAccessPolicy(
                    policy_id=f"EXPLICIT_ACCESS_TO_{state_code.value}_{state_code_column.upper()}",
                    table=table,
                    access_group_email=access_group_email,
                    filter_predicate=f'UPPER({state_code_column}) = "{state_code.value}"',
                )
            )

        # Create policy for all states with lax access control. This should
        # handle row access to most state data
        filtered_states_str = ", ".join(
            [
                f'"{state_code.value}"'
                for state_code in sorted(
                    RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP, key=lambda s: s.value
                )
            ]
        )
        policies.append(
            RowAccessPolicy(
                policy_id=f"NON_RESTRICTIVE_STATE_DATA_ACCESS_{state_code_column.upper()}",
                table=table,
                access_group_email=DEFAULT_STATE_DATA_GROUP_EMAIL,
                filter_predicate=f"UPPER({state_code_column}) NOT IN ({filtered_states_str})",
            )
        )

        return policies

    @classmethod
    def _build_row_access_policy_queries_for_state_specific_table(
        cls, state_code: StateCode, table: bigquery.Table
    ) -> List[RowAccessPolicy]:
        """Builds row-level permissions policy query applying to all rows in the provided table"""
        policies: List[RowAccessPolicy] = []

        # Create policy to grant admins access to view, edit and copy ALL state data. We
        # do this first so that if applying permissions fails in the middle, admins
        # (including service accounts) still will have access.
        policies.append(
            RowAccessPolicy(
                policy_id="ADMIN_ACCESS_TO_ALL_ROWS",
                table=table,
                access_group_email=BIG_QUERY_ADMINS_GROUP_EMAIL,
                filter_predicate="TRUE",
            )
        )

        # Create policy for that allows member of the state data security group
        # to access all records
        policies.append(
            RowAccessPolicy(
                policy_id="RESTRICT_DATASET_TO_MEMBERS_OF_STATE_SECURITY_GROUP",
                table=table,
                access_group_email=RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP[
                    state_code
                ],
                filter_predicate="TRUE",
            )
        )
        return policies

    @classmethod
    def build_row_access_policies(cls, table: bigquery.Table) -> List[RowAccessPolicy]:
        """Builds row-level permissions policy queries for the provided table.
        Row-level permissions are only necessary for tables in state-specific datasets
        for states with restricted access requirements, or for tables with multiple states'
        data, indicated by the presence of a state_code or region_code field.

        Returns a list of semicolon-terminated queries to create row access policies for the provided table.
        """
        if not access_group_configs_are_loaded():
            # The group config files are excluded from the public pulse-data
            # mirror, so there is no group data to build policies from there.
            # In the private repo the files always exist, and the apply
            # entrypoint fails loudly when they are missing.
            return []

        address = BigQueryAddress.from_table(table)
        state_code = address.state_code_for_address()
        state_code_column = _get_state_code_column(table)

        is_non_restricted_state_specific_table = (
            state_code
            and state_code not in RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP
        )
        if is_non_restricted_state_specific_table or (
            _table_doesnt_contain_multiple_states_data := state_code_column is None
        ):
            return []

        if (
            _is_restricted_state_specific_table := state_code
            and state_code in RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP
        ):
            return cls._build_row_access_policy_queries_for_state_specific_table(
                state_code, table
            )

        # If we are here then the table must contain multiple states' data
        return cls._build_row_access_policy_queries_for_state_agnostic_table(
            table, state_code_column
        )

    @staticmethod
    def build_query_to_drop_row_access_policy(table: bigquery.Table) -> str:
        return f"""DROP ALL ROW ACCESS POLICIES
            ON `{table.project}.{table.dataset_id}.{table.table_id}`;"""
