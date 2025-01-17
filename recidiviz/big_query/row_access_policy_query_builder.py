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
from typing import Dict, List

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import table_has_field
from recidiviz.common.constants.states import StateCode

RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP: Dict[StateCode, str] = {
    StateCode.US_OZ: "s-oz-data@recidiviz.org",
    # TODO(#20822) "US_MI": "s-mi-data@recidiviz.org",
    # TODO(#20823) "US_PA": "s-pa-data@recidiviz.org",
}

RESTRICTED_ACCESS_FIELDS = ["state_code", "region_code"]


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

    @staticmethod
    def _build_create_row_access_policy_query(
        policy_name: str,
        table: bigquery.Table,
        access_group_email: str,
        filter_clause: str,
    ) -> str:
        return f"""CREATE OR REPLACE ROW ACCESS POLICY
                {policy_name}
                ON `{table.project}.{table.dataset_id}.{table.table_id}`
                GRANT TO ("group:{access_group_email}")
                FILTER USING ({filter_clause});"""

    @classmethod
    def _build_row_access_policy_queries_for_state_agnostic_table(
        cls, table: bigquery.Table, state_code_column: str
    ) -> List[str]:
        """Builds row-level permissions policy for the provided table based on the state code
        in the provided |state_code_field| column.
        """
        queries = []
        for (
            state_code,
            access_group_email,
        ) in RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP.items():
            # Create explicit row-level policies for states with rigid access control requirements.
            queries.append(
                cls._build_create_row_access_policy_query(
                    policy_name=f"EXPLICIT_ACCESS_TO_{state_code.value}_{state_code_column.upper()}",
                    table=table,
                    access_group_email=access_group_email,
                    filter_clause=f'UPPER({state_code_column}) = "{state_code.value}"',
                )
            )

        # Create policy for all states with lax access control. This should
        # handle row access to most state data
        filtered_states_str = ", ".join(
            [
                f'"{state_code.value}"'
                for state_code in RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP
            ]
        )
        queries.append(
            cls._build_create_row_access_policy_query(
                policy_name=f"NON_RESTRICTIVE_STATE_DATA_ACCESS_{state_code_column.upper()}",
                table=table,
                access_group_email="s-default-state-data@recidiviz.org",
                filter_clause=f"UPPER({state_code_column}) NOT IN ({filtered_states_str})",
            )
        )

        # Create policy to grant admins access to view, edit and copy ALL state data
        queries.append(
            cls._build_create_row_access_policy_query(
                policy_name=f"ADMIN_ACCESS_TO_ALL_STATE_DATA_{state_code_column.upper()}",
                table=table,
                access_group_email="s-big-query-admins@recidiviz.org",
                filter_clause="TRUE",
            )
        )

        return queries

    @classmethod
    def _build_row_access_policy_queries_for_state_specific_table(
        cls, state_code: StateCode, table: bigquery.Table
    ) -> List[str]:
        """Builds row-level permissions policy query applying to all rows in the provided table"""
        queries = []
        # Create policy for that allows member of the state data security group
        # to access all records
        queries.append(
            cls._build_create_row_access_policy_query(
                policy_name="RESTRICT_DATASET_TO_MEMBERS_OF_STATE_SECURITY_GROUP",
                table=table,
                access_group_email=RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP[
                    state_code
                ],
                filter_clause="TRUE",
            )
        )
        # Create policy to grant admins access to view, edit and copy all rows
        queries.append(
            cls._build_create_row_access_policy_query(
                policy_name="ADMIN_ACCESS_TO_ALL_ROWS",
                table=table,
                access_group_email="s-big-query-admins@recidiviz.org",
                filter_clause="TRUE",
            )
        )
        return queries

    @classmethod
    def build_queries_to_create_row_access_policy(
        cls, table: bigquery.Table
    ) -> List[str]:
        """Builds row-level permissions policy queries for the provided table.
        Row-level permissions are only necessary for tables in state-specific datasets
        for states with restricted access requirements, or for tables with multiple states'
        data, indicated by the presence of a state_code or region_code field.

        Returns a list of semicolon-terminated queries to create row access policies for the provided table.
        """

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
