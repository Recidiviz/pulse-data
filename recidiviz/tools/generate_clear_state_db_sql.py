# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Script for generating a full list of commands for dropping all state data for a particular state from our Postgres
DB.

Usage:
    python -m recidiviz.tools.generate_clear_state_db_sql --state-code US_MO
"""

import argparse
import logging
from typing import List

import sqlalchemy

from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema_utils import get_foreign_key_constraints

ASSOCIATION_TABLE_NAME_SUFFIX = "_association"

ASSOCIATION_TABLE_DELETION_FILTER_CLAUSE_TEMPLATE = (
    "{foreign_key_col} IN "
    "(SELECT {foreign_key_col} FROM {foreign_key_table} "
    "WHERE state_code = '{state_code}')"
)

SQLALCHEMY_STATE_CODE_VAR = ":state_code_1"


def _format_deletion_command(state_code: str, command: str) -> str:
    return str(command).replace(SQLALCHEMY_STATE_CODE_VAR, f"'{state_code}'") + ";"


def _commands_for_table(state_code: str, table: sqlalchemy.Table) -> List[str]:
    if hasattr(table.c, "state_code"):
        return [
            _format_deletion_command(
                state_code, table.delete().where(table.c.state_code == state_code)
            )
        ]

    if not table.name.endswith(ASSOCIATION_TABLE_NAME_SUFFIX):
        raise ValueError(
            f"Unexpected non-association table is missing a state_code field: [{table.name}]"
        )

    foreign_key_constraints = get_foreign_key_constraints(table)

    table_commands = []
    for c in foreign_key_constraints:
        constraint: sqlalchemy.ForeignKeyConstraint = c
        filter_statement = ASSOCIATION_TABLE_DELETION_FILTER_CLAUSE_TEMPLATE.format(
            foreign_key_table=constraint.referred_table,
            foreign_key_col=constraint.column_keys[0],
            state_code=state_code,
        )

        table_commands.append(
            _format_deletion_command(
                state_code, table.delete().where(sqlalchemy.text(filter_statement))
            )
        )
    return table_commands


def generate_region_deletion_commands(state_code: str) -> List[str]:
    commands = []

    for table in reversed(StateBase.metadata.sorted_tables):
        commands.extend(_commands_for_table(state_code, table))

    return commands


def main(state_code: str) -> None:
    """Executes the main flow of the script."""
    print(
        f"RUN THE FOLLOWING COMMANDS IN ORDER TO DELETE ALL DATA FOR REGION [{state_code}]"
    )
    print(
        "********************************************************************************"
    )

    for cmd in generate_region_deletion_commands(state_code):
        print(cmd)

    print(
        "********************************************************************************"
    )
    print("HOW TO PERFORM DELETION:")
    print(
        "1) Log into prod data client (`gcloud compute ssh prod-data-client --project=recidiviz-123`)"
    )
    print("\n> For production deletion:")
    print(
        "2) Go to secret manager to get login credentials stored in `state_db_user` and `state_db_password` secrets:"
        "\n\thttps://console.cloud.google.com/security/secret-manager?organizationId=448885369991&"
        "project=recidiviz-123"
    )
    print("3) Log into postgres database (`prod-state-psql`)")
    print("\n> For staging deletion:")
    print(
        "2) Go to secret manager to get login credentials stored in `state_db_user` and `state_db_password` secrets:"
        "\n\thttps://console.cloud.google.com/security/secret-manager?organizationId=448885369991&"
        "project=recidiviz-staging"
    )
    print("3) Log into postgres database (`dev-state-psql`)")
    print("\n> For all:")
    print(
        "4) Paste full list of commands listed above in postgres command line and run. Some commands may take a "
        "while to run."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--state-code",
        required=True,
        help="The state whose data we want to delete for a rerun.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main(args.state_code.upper())
