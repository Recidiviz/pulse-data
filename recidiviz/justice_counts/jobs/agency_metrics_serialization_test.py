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
Successful invocation of this script means that all metric interfaces present in the
datapoint table are successfully serialized/deserialized without any data loss.

This job
- Reads all agency metric interfaces from the datapoints table.
- Converts all metric interfaces to JSON objects using to_storage_json.
- Converts all JSON objects back to metric interfaces using from_storage_json.
- Compares the original metric interface with the deserialized metric interface using DeepDiff.

DeepDiff is used to compare complex structures. It ignores the order of elements in
lists. We needed to add clean up functions to remove empty fields from the metric
interfaces, which remove all null value entries from the MetricInterface.

How to run:

pipenv run python -m recidiviz.justice_counts.jobs.agency_metrics_serialization_test \
  --project-id="justice-counts-staging"

pipenv run python -m recidiviz.justice_counts.jobs.agency_metrics_serialization_test \
  --project-id="justice-counts-production"

Note: This script is a READ-ONLY script and is therefore safe to run in production.
"""

import argparse
import logging
import sys
from typing import Any

from deepdiff import DeepDiff

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_disaggregation_data import (
    MetricAggregatedDimensionData,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.utils.constants import (
    AGENCIES_TO_EXCLUDE,
    DatapointGetRequestEntryPoint,
)
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=False,
    )
    return parser


def clean_aggregated_dimension(
    aggregated_dimension: MetricAggregatedDimensionData,
) -> MetricAggregatedDimensionData:
    """Cleans the MetricAggregatedDimensionData object by removing null or empty fields."""
    aggregated_dimension.dimension_to_value = None

    # Clean up dimension_to_enabled_status dicts.
    if aggregated_dimension.dimension_to_enabled_status is not None:
        aggregated_dimension.dimension_to_enabled_status = {
            dimension: enabled
            for dimension, enabled in aggregated_dimension.dimension_to_enabled_status.items()
            if enabled is not None
        }
        # If dictionary is empty, set it to None.
        if len(aggregated_dimension.dimension_to_enabled_status) == 0:
            aggregated_dimension.dimension_to_enabled_status = None

    # Clean up dimension_to_includes_excludes_member_to_setting dicts.
    aggregated_dimension.dimension_to_includes_excludes_member_to_setting = {
        dim: includes_excludes_member_to_setting
        for dim, includes_excludes_member_to_setting in aggregated_dimension.dimension_to_includes_excludes_member_to_setting.items()
        if includes_excludes_member_to_setting is not None
    }
    for (
        dim,
        includes_excludes_member_to_setting,
    ) in aggregated_dimension.dimension_to_includes_excludes_member_to_setting.items():
        aggregated_dimension.dimension_to_includes_excludes_member_to_setting[dim] = {
            member: setting
            for member, setting in includes_excludes_member_to_setting.items()
            if setting is not None
        }
    # Filter out empty dictionaries.
    aggregated_dimension.dimension_to_includes_excludes_member_to_setting = {
        dim: includes_excludes_member_to_setting
        for dim, includes_excludes_member_to_setting in aggregated_dimension.dimension_to_includes_excludes_member_to_setting.items()
        if len(includes_excludes_member_to_setting) > 0
    }

    # Clean up dimension_to_contexts dicts.
    for dim, contexts in aggregated_dimension.dimension_to_contexts.items():
        contexts = [context for context in contexts if context.value is not None]
    # Filter out empty dictionaries.
    aggregated_dimension.dimension_to_contexts = {
        dim: contexts
        for dim, contexts in aggregated_dimension.dimension_to_contexts.items()
        if len(contexts) > 0
    }
    return aggregated_dimension


def clean(metric_interface: MetricInterface) -> MetricInterface:
    """Cleans the MetricInterface object by removing empty fields."""
    # Clean up contexts.
    metric_interface.contexts = [
        context for context in metric_interface.contexts if context.value is not None
    ]

    # Clean up aggregated_dimensions.
    metric_interface.aggregated_dimensions = [
        clean_aggregated_dimension(aggregated_dimension)
        for aggregated_dimension in metric_interface.aggregated_dimensions
    ]

    # Clean up includes_excludes_member_to_setting.
    metric_interface.includes_excludes_member_to_setting = {
        member: setting
        for member, setting in metric_interface.includes_excludes_member_to_setting.items()
        if setting is not None
    }
    return metric_interface


def fetch_data_for_session(
    session: Any,
) -> None:
    """Retrieve agency, user, agency_user_account_association, and datapoint data from
    both Auth0 as well as our database.
    """
    original_agencies = session.execute(
        "select * from source where type = 'agency' and LOWER(name) not like '%test%'"
    ).all()
    datapoints = session.execute("select * from datapoint").all()
    logger.info("# of agencies read: %s", len(original_agencies))

    agencies_to_exclude = AGENCIES_TO_EXCLUDE.keys()
    agencies = [
        dict(a) for a in original_agencies if a["id"] not in agencies_to_exclude
    ]
    logger.info("# of agencies after exclusion: %s", len(agencies))

    logger.info("# of datapoints read: %s", len(datapoints))


def serialization_test(
    session: Any,
) -> None:
    """Test that all metric interfaces in the datapoint table are successfully
    serialized/deserialized without data loss.
    """
    # First, pull agency and agency datapoint data from our database
    fetch_data_for_session(session=session)
    agencies = AgencyInterface.get_agencies(session=session)
    logger.info("# of agencies in session: %s", len(agencies))
    count_agencies = 0
    count_interfaces = 0
    for agency in agencies:
        count_agencies += 1
        if count_agencies % 10 == 0:
            logger.info(
                "Percent complete: %s", round(count_agencies / len(agencies) * 100, 2)
            )
        metric_interfaces = MetricSettingInterface.get_agency_metric_interfaces(
            session=session, agency=agency
        )
        for metric_interface in metric_interfaces:
            count_interfaces += 1
            # Converting to-and-from a JSON using the existing to/from_json methods (as
            # opposed to the to/from_storage_json methods) is necessary to mimic the
            # existing behavior, since MetricInterfaces are fetched from Publisher's
            # Metrics Tab. This is also necessary so that we drop invalid fields.
            metric_interface = MetricInterface.from_json(
                json=metric_interface.to_json(
                    entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                ),
                entry_point=DatapointGetRequestEntryPoint.METRICS_TAB,
            )

            from_storage_json = metric_interface.from_storage_json(
                metric_interface.to_storage_json()
            )

            # Use DeepDiff to compare complex structures
            diff = DeepDiff(
                clean(metric_interface), clean(from_storage_json), ignore_order=True
            )
            if diff:
                logger.info("Diff found for metric interface.")
                logger.info("Agency: %s \n\n", str(agency.to_json()))
                logger.info(
                    "Metric Interface %s",
                    str(
                        metric_interface.to_json(
                            entry_point=DatapointGetRequestEntryPoint.METRICS_TAB
                        )
                    ),
                )
                logger.info("\n\n diff: %s", str(diff))
                sys.exit(1)

    logger.info("# Agencies compared: %s", count_agencies)
    logger.info("# Interfaces compared: %s", count_interfaces)
    logger.info("\n\n\n ----- No mismatches detected -----")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    if args.project_id is not None:
        with local_project_id_override(args.project_id):
            schema_type = SchemaType.JUSTICE_COUNTS
            database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
            with cloudsql_proxy_control.connection(
                schema_type=schema_type,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            ):
                with SessionFactory.for_proxy(
                    database_key=database_key,
                    secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                    autocommit=False,
                ) as global_session:
                    serialization_test(
                        session=global_session,
                    )
