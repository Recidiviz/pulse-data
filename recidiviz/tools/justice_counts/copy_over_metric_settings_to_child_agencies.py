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

"""
Copies all metric settings from a super agency to its child agencies.
If child agencies have existing settings that conflict with the super agency,
they will be overwritten. If the child agency has existing settings that do not
conflict with the super agency, then they will not be affected.

python -m recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies \
  --project_id=<justice-counts-staging OR justice-counts-production> \
  --super_agency_id=<agency_id> \
  --dry-run=<true Or false>
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--super_agency_id",
        type=int,
        help="The id of the super_agency you want to copy agency settings from.",
        required=True,
    )

    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def copy_metric_settings(super_agency_id: int, dry_run: bool) -> None:
    """Copies metric settings from a super agency to a child agency."""
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            super_agency_list = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=[super_agency_id]
            )

            if len(super_agency_list) == 0:
                logger.info(
                    "No agency was found with the super_agency_id provided. Please check that you are running the script in the right environment"
                )
                return

            super_agency = super_agency_list.pop()

            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=[super_agency_id]
            )
            super_agency_metric_settings = (
                DatapointInterface.get_metric_settings_by_agency(
                    session=session,
                    agency=super_agency,
                )
            )

            super_agency_metric_keys = [
                metric.key for metric in METRICS_BY_SYSTEM["SUPERAGENCY"]
            ]

            for child_agency in child_agencies:
                logger.info("Child Agency: %s", child_agency.name)
                for metric_setting in super_agency_metric_settings:
                    # We do not want to copy over Superagency specific metric
                    # configurations. Skip these.
                    if metric_setting.key in super_agency_metric_keys:
                        logger.info("Skipping %s", metric_setting.key)
                        continue
                    logger.info("Metric %s, is being updated", metric_setting.key)
                    if dry_run is False:
                        DatapointInterface.add_or_update_agency_datapoints(
                            session=session,
                            agency=child_agency,
                            agency_metric=metric_setting,
                        )

            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        copy_metric_settings(dry_run=args.dry_run, super_agency_id=args.super_agency_id)
