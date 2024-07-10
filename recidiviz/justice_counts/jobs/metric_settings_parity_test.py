# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Successful invocation of this script means that the metric interfaces present as agency
datapoints exactly match the metric interfaces present in the metric settings table.
We will use this script to routinely check for parity between the two tables, during the
period that we are double-writing to both tables.

This job
- Reads all agency metric interfaces from the datapoints and the metric settings tables.
- Compares the metric interface by converting the using to_json (what the frontend sees) and then using DeepDiff to compare the json objects.
- Raises any errors if the metric interfaces do not match.

Local Usage:

docker exec pulse-data-control_panel_backend-1 pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_parity_test \
  --project-id="justice-counts-local"

Staging or Production Usage:

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_parity_test \
  --project-id="justice-counts-staging"

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_parity_test \
  --project-id="justice-counts-production"

Note: This script is a READ-ONLY script and is therefore safe to run in production.
"""

import argparse
import logging
import sys
from collections import defaultdict
from typing import Any, Dict, List

from deepdiff import DeepDiff
from sqlalchemy import false

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.metric_setting import (
    DatapointInterface,
    MetricSettingInterface,
)
from recidiviz.justice_counts.utils.constants import DatapointGetRequestEntryPoint
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


def parity_test(
    session: Any,
) -> None:
    """Test that the datapoint table and the metric settings table are storing identical
    information about metric interfaces.
    """
    agencies = AgencyInterface.get_agencies(session=session)
    logger.info("# of agencies in session: %s", len(agencies))
    count_agencies = 0
    count_interfaces = 0

    # Fetch all agency datapoints.
    agency_datapoints = (
        session.query(schema.Datapoint).filter(
            schema.Datapoint.is_report_datapoint == false(),
        )
    ).all()
    # Group agency datapoints by agency id.
    agency_id_to_datapoints = defaultdict(list)
    for agency_datapoint in agency_datapoints:
        agency_id_to_datapoints[agency_datapoint.source_id].append(agency_datapoint)

    # Fetch all agency metric settings.
    agency_metric_settings = session.query(schema.MetricSetting).all()
    # Group agency metric settings by agency id.
    agency_id_to_metric_settings: Dict[int, List[schema.MetricSetting]] = defaultdict(
        list
    )
    for setting in agency_metric_settings:
        agency_id_to_metric_settings[setting.agency_id].append(setting)

    for agency in agencies:
        count_agencies += 1
        logger.info(
            "%s%% complete.",
            round(count_agencies / len(agencies) * 100, 2),
        )
        # Get metric interfaces stored in the datapoint table.
        datapoint_metric_interfaces = DatapointInterface.get_metric_settings_by_agency(
            session=session,
            agency=agency,
            agency_datapoints=agency_id_to_datapoints[agency.id],
        )
        key_to_datapoint_metric_interface = {
            interface.key: interface for interface in datapoint_metric_interfaces
        }
        # Get metric interfaces stored in the metric settings table.
        metric_setting_metric_interfaces = (
            MetricSettingInterface.get_agency_metric_interfaces(
                session=session,
                agency=agency,
                agency_metric_settings=agency_id_to_metric_settings[agency.id],
            )
        )
        key_to_metric_setting_metric_interface = {
            metric_interface.key: metric_interface
            for metric_interface in metric_setting_metric_interfaces
        }

        # Ensure all metric keys are the same.
        if set(key_to_datapoint_metric_interface.keys()) != set(
            key_to_metric_setting_metric_interface.keys()
        ):
            logger.info("Mismatch in metric interface keys for agency %s", agency.id)
            logger.info(
                "Datapoint Metric Interface Keys: %s",
                key_to_datapoint_metric_interface.keys(),
            )
            logger.info(
                "Metric Setting Metric Interface Keys: %s",
                key_to_metric_setting_metric_interface.keys(),
            )
            sys.exit(1)

        # Compare metric interfaces from the datapoint and metric settings tables.
        for (
            key,
            metric_setting_metric_interface,
        ) in key_to_metric_setting_metric_interface.items():
            count_interfaces += 1
            # Use DeepDiff to compare complex structures.
            diff = DeepDiff(
                key_to_datapoint_metric_interface[key].to_json(
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                ),
                metric_setting_metric_interface.to_json(
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                ),
                ignore_order=True,
            )
            # Metric interfaces are the same.
            if not diff:
                continue
            # Otherwise, log the diff and exit.
            logger.info("Diff found for metric interface.\n\n")
            logger.info("Agency: %s \n\n", str(agency.to_json()))
            logger.info(
                "Datapoint Metric Interface %s\n\n",
                str(
                    key_to_datapoint_metric_interface[key].to_json(
                        entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                    )
                ),
            )
            logger.info(
                "Metric Settings Metric Interface %s\n\n",
                str(
                    metric_setting_metric_interface.to_json(
                        entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                    )
                ),
            )
            logger.info("Diff: %s\n\n", str(diff))
            sys.exit(1)

    logger.info("# Agencies compared: %s", count_agencies)
    logger.info("# Interfaces compared: %s", count_interfaces)
    logger.info("----- No mismatches detected -----")


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
        parity_test(session=Session(bind=justice_counts_engine))
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
                parity_test(
                    session=global_session,
                )
