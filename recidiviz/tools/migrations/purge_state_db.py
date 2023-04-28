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
Important note: This script should be run on your local machine. It will not work
when run anywhere else.

This script runs all downgrade migrations for a given state database, so that when
new data is subsequently imported, it will not conflict due to duplicate definitions
of enums for instance.
Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.purge_state_db \
    --state-code US_MI \
    --ingest-instance SECONDARY \
    --project-id recidiviz-staging \
    [--purge-schema]
"""
import argparse
import logging

import alembic.config

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.instance_database_key import database_key_for_state
from recidiviz.persistence.database.schema.state.schema import (
    StateAgent,
    StatePerson,
    StateStaff,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Purges all data from a remote PostgresQL database."
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        choices=list(StateCode),
        help="Specifies the state where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Specifies the database version where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--purge-schema",
        action="store_true",
        help="When set, runs the downgrade migrations.",
        default=False,
    )
    return parser


def main(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    purge_schema: bool,
) -> None:
    """
    Invokes the main code path for running a downgrade.

    This checks for user validations that the database and branches are correct and then
    runs the downgrade migration.
    """
    is_prod = metadata.running_against(GCP_PROJECT_PRODUCTION)
    if is_prod:
        logging.info("RUNNING AGAINST PRODUCTION\n")

    purge_str = (
        f"PURGE {state_code.value} DATABASE STATE IN "
        f"{'PROD' if is_prod else 'STAGING'} {ingest_instance.value}"
    )
    db_key = database_key_for_state(ingest_instance, state_code)

    prompt_for_confirmation(
        f"This script will PURGE all data for for [{state_code.value}] in DB [{db_key.db_name}].",
        purge_str,
    )

    if purge_schema:
        purge_schema_str = (
            f"RUN {state_code.value} DOWNGRADE MIGRATIONS IN "
            f"{'PROD' if is_prod else 'STAGING'} {ingest_instance.value}"
        )
        prompt_for_confirmation(
            f"This script will run all DOWNGRADE migrations for "
            f"[{state_code.value}] in DB [{db_key.db_name}].",
            purge_schema_str,
        )

    with cloudsql_proxy_control.connection(schema_type=SchemaType.STATE):
        db_key = database_key_for_state(ingest_instance, state_code)

        with SessionFactory.for_proxy(db_key) as session:
            tables_to_truncate = [
                StatePerson.__tablename__,
                StateStaff.__tablename__,
                StateAgent.__tablename__,
            ]
            for table_name in tables_to_truncate:
                command = f"TRUNCATE TABLE {table_name} CASCADE;"
                logging.info('Running query ["%s"]. This may take a while...', command)
                session.execute(command)

            logging.info("Done running truncate commands.")

        if purge_schema:
            with SessionFactory.for_proxy(db_key) as purge_session:
                overridden_env_vars = None

                try:
                    logging.info("Purging schema...")

                    overridden_env_vars = (
                        SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
                            database_key=db_key,
                            using_proxy=True,
                        )
                    )

                    # Alembic normally hijacks logging with its own logging, which means
                    # we would stop sending output to stdout. We set the 'configure_logger'
                    # flag here to turn off all logger configuration in the alembic env.py files.
                    config = alembic.config.Config(
                        db_key.alembic_file, attributes={"configure_logger": False}
                    )
                    alembic.command.downgrade(config, "base")

                    # We need to manually delete alembic_version because it's leftover after
                    # the downgrade migrations
                    purge_session.execute("DROP TABLE alembic_version;")
                finally:
                    if overridden_env_vars:
                        local_postgres_helpers.restore_local_env_vars(
                            overridden_env_vars
                        )

            logging.info("Purge complete.")

    logging.info("Script complete.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(
            args.state_code,
            args.ingest_instance,
            args.purge_schema,
        )
