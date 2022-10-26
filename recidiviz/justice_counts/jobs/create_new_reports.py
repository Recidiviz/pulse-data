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
"""Script for generating Reports for the latest month and/or year for each Justice Counts Agency.

Local Usage: docker exec pulse-data_control_panel_backend_1 pipenv run python -m recidiviz.justice_counts.jobs.create_new_reports
Remote Usage: Execute the `justice-counts-recurring-reports` Cloud Run Job
"""

import logging

from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

logger = logging.getLogger(__name__)


def main(engine: Engine) -> None:
    session = Session(bind=engine)

    logger.info("Generating new Reports in database %s", engine.url)
    for agency in session.query(schema.Agency).all():
        logger.info("Generating Reports for Agency %s", agency.name)
        # TODO(#15416) Implement the following:
        # 1. Determine the current month
        # 2. Determine if a monthly Report already exists for this agency and this month
        # 3. If not, create one
        # 4. If the month is January, determine the current year
        # 5. Determine if an annual Report already exists for the agency and this year
        # 6. If not, create one


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(database_key)

    main(justice_counts_engine)
