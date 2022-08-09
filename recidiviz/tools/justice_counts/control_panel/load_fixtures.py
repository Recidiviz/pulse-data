#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
Tool for loading fixture data into the Justice Counts development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables and then re-add them from the
fixture files.
Usage against default development database (docker-compose v1):
docker exec pulse-data_control_panel_backend_1 pipenv run python -m recidiviz.tools.justice_counts.control_panel.load_fixtures

Usage against default development database (docker-compose v2):
docker exec pulse-data-control_panel_backend-1 pipenv run python -m recidiviz.tools.justice_counts.control_panel.load_fixtures
"""
import logging
from typing import List

from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.justice_counts.control_panel.generate_fixtures import (
    generate_fixtures,
)
from recidiviz.tools.utils.fixture_helpers import _get_table_name
from recidiviz.utils.environment import in_development

logger = logging.getLogger(__name__)


def reset_justice_counts_fixtures(engine: Engine) -> None:
    """Deletes all data and then re-imports data from our fixture files."""
    logger.info("Resetting fixtures for database %s", engine.url)

    tables: List[schema.JusticeCountsBase] = [
        schema.UserAccount,
        schema.Source,
        schema.Report,
    ]
    session = Session(bind=engine)
    for table in reversed(tables):
        table_name = _get_table_name(module=table)
        logger.info("Removing data from `%s` table", table_name)
        session.query(table).delete()

    # object_groups is a list of lists, each of which will get added,
    # in order, via session.add_all() + session.commit(). Objects that
    # depend on another object already being present in the db should
    # be added in a subsequent object group.
    object_groups = generate_fixtures()
    for object_group in object_groups:
        logger.info(
            "Adding a group of %d objects to the db",
            len(object_group),
        )
        session.add_all(object_group)
        session.execute(
            "SELECT pg_catalog.setval(pg_get_serial_sequence('user_account', 'id'), MAX(id)) FROM user_account;"
        )
        session.commit()


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(database_key)

    reset_justice_counts_fixtures(justice_counts_engine)
