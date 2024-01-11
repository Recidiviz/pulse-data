#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Script for sending reminder emails to agencies to upload missing metrics.

Local Usage: docker exec pulse-data-control_panel_backend-1 pipenv run python -m recidiviz.justice_counts.jobs.email_reminder_job
Remote Usage: Execute the `email_reminder_job` Cloud Run Job
"""

import argparse
import logging

import sentry_sdk
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.justice_counts.utils.email import send_reminder_emails
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=JUSTICE_COUNTS_SENTRY_DSN,
    # Enable performance monitoring
    enable_tracing=True,
)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def send_reminder_emails_to_all_agencies(engine: Engine, dry_run: bool) -> None:
    session = Session(bind=engine)
    agencies = AgencyInterface.get_agencies(session=session)
    for agency in agencies:
        msg = "DRY_RUN " if dry_run is True else ""
        if agency.is_superagency is True or agency.super_agency_id is not None:
            # TODO(#26632) For P0 of rollout, no superagencies or child agencies will
            # receive reminder emails
            continue
        msg += "Sending reminder emails to " + agency.name
        logger.info(msg)
        send_reminder_emails(
            session=session, agency_id=agency.id, dry_run=dry_run, logger=logger
        )
        logger.info("-----------------------------------\n\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(
        database_key=database_key,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    args = create_parser().parse_args()
    send_reminder_emails_to_all_agencies(
        engine=justice_counts_engine, dry_run=args.dry_run
    )
