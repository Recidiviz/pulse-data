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
"""
Helper functions for confirming user input when running migrations.
"""
import logging
import sys

from pygit2.repository import Repository

from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType


def prompt_for_confirmation(input_text: str, accepted_response: str) -> None:
    check = input(
        f'{input_text}\nPlease type "{accepted_response}" to confirm. (Anything else exits):\n'
    )
    if check != accepted_response:
        logging.warning("\nConfirmation aborted.")
        sys.exit(1)


def confirm_correct_db_instance(database: SchemaType) -> None:
    dbname = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(database)
    if dbname is None:
        logging.error("Could not find database instance.")
        logging.error("Exiting...")
        sys.exit(1)

    prompt_for_confirmation(f"Running migrations on {dbname}.", dbname)


def confirm_correct_git_branch(repo_root: str, is_prod: bool = False) -> None:
    try:
        repo = Repository(repo_root)
    except Exception as e:
        logging.error("improper project root provided: %s", e)
        sys.exit(1)

    current_branch = repo.head.shorthand

    if is_prod and not current_branch.startswith("releases/"):
        logging.error(
            "Migrations run against production must be from a release branch. The current branch is %s.",
            current_branch,
        )
        sys.exit(1)

    prompt_for_confirmation(
        f"This script will execute migrations based on the contents of the current branch ({current_branch}).",
        current_branch,
    )
