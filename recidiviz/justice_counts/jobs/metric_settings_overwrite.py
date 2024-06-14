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
"""Script for populating the MetricSettings table based on existing agency datapoints.

Steps:
    * Delete all existing rows in the MetricSettings table.
    * Copy all metric interfaces from the agency datapoint table to MetricSettings.

Usage: 

docker exec pulse-data-control_panel_backend-1 pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_overwrite --project-id="justice-counts-local"

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_overwrite --project-id="justice-counts-staging"

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_overwrite --project-id="justice-counts-production"

"""
import argparse
import logging
import sys
from collections import defaultdict
from typing import Any

from sqlalchemy import false

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.metric_setting import DatapointInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
    PROJECT_JUSTICE_COUNTS_LOCAL,
)
from recidiviz.utils.metadata import local_project_id_override

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            PROJECT_JUSTICE_COUNTS_LOCAL,
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    return parser


def overwrite_metric_settings(
    session: Any,
) -> None:
    """Read all metric interfaces from agency datapoints and write them to the
    MetricSettings table.
    """
    # Delete all rows from MetricSetting.
    all_settings = session.query(schema.MetricSetting)
    logger.info(
        "# of interfaces deleted from MetricSettings table: %s", all_settings.count()
    )
    all_settings.delete()
    session.commit()

    # Fetch all agency datapoints.
    agency_datapoints = (
        session.query(schema.Datapoint).filter(
            schema.Datapoint.is_report_datapoint == false(),
        )
    ).all()

    agency_id_to_datapoints = defaultdict(list)
    for agency_datapoint in agency_datapoints:
        agency_id_to_datapoints[agency_datapoint.source_id].append(agency_datapoint)

    # Fetch all agencies.
    agencies = AgencyInterface.get_agencies(session=session)
    logger.info("# of agencies in session: %s", len(agencies))
    count_agencies = 0
    count_interfaces = 0
    for agency in agencies:
        if count_agencies % 10 == 0:
            logger.info("%s%% complete", round(count_agencies / len(agencies) * 100, 2))
        count_agencies += 1
        metric_interfaces = DatapointInterface.get_metric_settings_by_agency(
            session=session,
            agency=agency,
            agency_datapoints=agency_id_to_datapoints[agency.id],
        )
        for metric_interface in metric_interfaces:
            # Use try and except here to aid in debugging.
            try:
                session.add(
                    schema.MetricSetting(
                        agency_id=agency.id,
                        metric_definition_key=metric_interface.key,
                        metric_interface=metric_interface.to_storage_json(),
                    )
                )
                count_interfaces += 1
            except Exception:
                logger.info(
                    "Error for agency %s, agency_id %s.", agency.name, agency.id
                )
                logger.info(
                    "Error with storage metric interface: %s",
                    metric_interface.to_storage_json(),
                )
                sys.exit(1)
    logger.info("# Metric settings to add: %s", count_interfaces)
    logger.info("----- Writing all metric settings [In Progress]  -----")
    session.commit()
    logger.info("----- Writing all metric settings [Complete]  -----")
    logger.info("Confirming all settings are written to MetricSetting table...  -----")
    logger.info(
        "# of entries now in the MetricSetting table: %s",
        session.query(schema.MetricSetting).count(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=schema_type)
    # Local usage.
    if args.project_id == PROJECT_JUSTICE_COUNTS_LOCAL:
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        overwrite_metric_settings(session=Session(bind=justice_counts_engine))
        sys.exit(0)
    # Staging/Production usage.
    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(
            schema_type=schema_type,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ):
            with SessionFactory.for_proxy(
                database_key=database_key,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                autocommit=False,
            ) as global_session:
                overwrite_metric_settings(
                    session=global_session,
                )
