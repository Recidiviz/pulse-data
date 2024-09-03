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
"""Script for copying created_at and last_updated timestamps from Agency Datapoints to
the MetricSettings table.

Usage: 

docker exec pulse-data-control_panel_backend-1 pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_timestamp_migration --project-id="justice-counts-local" --dry-run=true

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_timestamp_migration --project-id="justice-counts-staging" --dry-run=true

pipenv run python -m recidiviz.justice_counts.jobs.metric_settings_timestamp_migration --project-id="justice-counts-production" --dry-run=true

"""
import argparse
import datetime
import logging
import sys
from collections import defaultdict
from typing import Any, DefaultDict, List

from recidiviz.justice_counts.metric_setting import MetricSettingInterface
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
from recidiviz.utils.params import str_to_bool

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
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def min_ignore_nones(vals: List[int | None]) -> int:
    """Returns the minimum value in the list. Requires that at least one value is non-null."""
    non_null_vals = [val for val in vals if val is not None]
    if len(non_null_vals) == 0:
        raise ValueError("vals must contain at least one non-null value.")
    return min(non_null_vals)


def max_ignore_nones(vals: List[int | None]) -> int:
    """Returns the maximum value in the list. Requires that at least one value is non-null."""
    non_null_vals = [val for val in vals if val is not None]
    if len(non_null_vals) == 0:
        raise ValueError("vals must contain at least one non-null value.")
    return max(non_null_vals)


def overwrite_metric_settings_timestamp(
    session: Any,
    dry_run: bool = True,
) -> None:
    """Read all metric interfaces from agency datapoints and write them to the
    MetricSettings table.
    """

    tmrw: datetime.datetime = datetime.datetime.today() + datetime.timedelta(days=1)

    the_past: datetime.datetime = datetime.datetime(1970, 1, 1)

    agency_id_to_metric_key_to_created_at: DefaultDict[
        int, DefaultDict[str, Any]
    ] = defaultdict(lambda: defaultdict(lambda: tmrw))
    agency_id_to_metric_key_to_last_updated: DefaultDict[
        int, DefaultDict[str, Any]
    ] = defaultdict(lambda: defaultdict(lambda: the_past))

    # Read all agency datapoints.
    datapoints: List[schema.Datapoint] = session.execute(
        "select * from datapoint where is_report_datapoint is False"
    ).all()

    # Get first_updated dict for datapoints. This is necessary since created_at was
    # added a few months after last_updated. This mimics the CSG datapull logic for
    # computing the 'INITIAL_METRIC_CONFIG_DATE' metric.
    datapoint_id_to_first_update = dict(
        session.execute(
            "select datapoint_history.datapoint_id, min(datapoint_history.timestamp) from datapoint_history where datapoint_history.old_value is null group by datapoint_history.datapoint_id"
        ).all()
    )

    for datapoint in datapoints:
        first_update = datapoint_id_to_first_update.get(datapoint.id, None)
        agency_id_to_metric_key_to_created_at[datapoint.source_id][
            datapoint.metric_definition_key
        ] = min_ignore_nones(
            [
                datapoint.created_at,
                agency_id_to_metric_key_to_created_at[datapoint.source_id][
                    datapoint.metric_definition_key
                ],
                first_update,
            ]
        )
        agency_id_to_metric_key_to_last_updated[datapoint.source_id][
            datapoint.metric_definition_key
        ] = max_ignore_nones(
            [
                datapoint.last_updated,
                agency_id_to_metric_key_to_last_updated[datapoint.source_id][
                    datapoint.metric_definition_key
                ],
                first_update,
            ]
        )

    for agency_id, metric_key_to_created_at in sorted(
        agency_id_to_metric_key_to_created_at.items()
    ):
        for metric_definition_key, created_at in metric_key_to_created_at.items():
            # If created_at is still tmrw, this means that no created_at values were
            # found in the table.
            if created_at == tmrw:
                created_at = None
            print(
                f"Created at for agency:key {agency_id}:{metric_definition_key} {created_at}"
            )
            if dry_run:
                continue
            existing_setting = (
                MetricSettingInterface.get_metric_setting_by_agency_id_and_metric_key(
                    session=session,
                    agency_id=agency_id,
                    metric_definition_key=metric_definition_key,
                )
            )
            if existing_setting is None:
                raise ValueError("Metric setting not found. This shouldn't happen.")

    for agency_id, metric_key_to_last_updated in sorted(
        agency_id_to_metric_key_to_last_updated.items()
    ):
        for metric_definition_key, last_updated in metric_key_to_last_updated.items():
            # If last_updated is still the_past(), this means that no last_updated values were
            # found in the table.
            if last_updated == the_past:
                last_updated = None
            print(
                f"Last updated for agency:key {agency_id}:{metric_definition_key} {last_updated}"
            )
            if dry_run:
                continue
            existing_setting = (
                MetricSettingInterface.get_metric_setting_by_agency_id_and_metric_key(
                    session=session,
                    agency_id=agency_id,
                    metric_definition_key=metric_definition_key,
                )
            )
            if existing_setting is None:
                raise ValueError("Metric setting not found. This shouldn't happen.")

            existing_setting.last_updated = last_updated

    if dry_run:
        print("---------- This was just a dry run. No values were written. ----------")
    else:
        session.commit()


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
        overwrite_metric_settings_timestamp(
            session=Session(bind=justice_counts_engine), dry_run=args.dry_run
        )
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
                overwrite_metric_settings_timestamp(
                    session=global_session, dry_run=args.dry_run
                )
