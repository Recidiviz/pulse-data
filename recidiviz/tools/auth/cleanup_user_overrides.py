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
Tool for removing entries from the UserOverride table where the data is equivalent to a row in the
Roster table. If a single column is equivalent, it sets it to null in the override. If all relevant
columns are equivalent, it removes the entry.

The script can be run against a local database or against one running in Cloud SQL.

Usage against default development database (docker-compose v1) after `docker-compose up` has been
run: docker exec pulse-data_admin_panel_backend_1 uv run python -m
recidiviz.tools.auth.cleanup_user_overrides --state_code US_XX

Usage against default development database (docker-compose v2) after `docker-compose up` has been
run: docker exec pulse-data-admin_panel_backend-1 uv run python -m
recidiviz.tools.auth.cleanup_user_overrides --state_code US_XX

To run against Cloud SQL, specify the project id: python -m
recidiviz.tools.auth.cleanup_user_overrides --project_id recidiviz-staging --state_code US_XX

The tool can also be run in dry-run mode to see what would happen before making actual changes to
the database:
python -m recidiviz.tools.auth.cleanup_user_overrides --project_id recidiviz-staging --state_code US_XX --dry_run
"""

import argparse
import logging
import sys

from recidiviz.auth.cleanup_user_overrides import cleanup_user_overrides
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_development,
)
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses the arguments needed to call the cleanup_user_overrides function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=False,
    )

    parser.add_argument(
        "--state_code",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument("--dry_run", dest="dry_run", action="store_true")

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    if known_args.project_id:
        with local_project_id_override(
            known_args.project_id
        ), cloudsql_proxy_control.connection(
            schema_type=SchemaType.CASE_TRIAGE,
        ), SessionFactory.for_proxy(
            db_key
        ) as global_session:
            cleanup_user_overrides(
                global_session, known_args.dry_run, known_args.state_code.value
            )
    else:
        if not in_development():
            raise RuntimeError(
                "Expected to be called inside a docker container or with the --project_id argument. See usage in docstring"
            )
        SQLAlchemyEngineManager.init_engine(db_key)
        with SessionFactory.using_database(db_key) as global_session:
            cleanup_user_overrides(
                global_session, known_args.dry_run, known_args.state_code.value
            )
