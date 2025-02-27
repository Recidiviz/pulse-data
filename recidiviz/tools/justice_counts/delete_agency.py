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
Script to delete an agency and all of its data.

python -m recidiviz.tools.justice_counts.delete_agency \
    --project-id=PROJECT_ID \
    --agency-id=AGENCY_ID \
    --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
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
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--agency-id",
        type=int,
        help="The id of the agency you want to delete.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def delete_agency(agency_id: int, dry_run: bool) -> None:
    """Delete the given agency and its data."""
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            autocommit=False,
        ) as session:
            agency = AgencyInterface.get_agency_by_id(
                session=session, agency_id=agency_id
            )

            logger.info("DRY RUN: %s", dry_run)
            logger.info("Will delete %s", agency.name)

            objects_to_delete = []
            associations = (
                session.query(schema.AgencyUserAccountAssociation)
                .filter_by(agency_id=agency_id)
                .all()
            )
            objects_to_delete.extend(associations)
            logger.info("Will delete %d UserAccountAssociations", len(associations))

            reports = session.query(schema.Report).filter_by(source_id=agency_id).all()
            objects_to_delete.extend(reports)
            logger.info("Will delete %d reports", len(reports))

            datapoints = (
                session.query(schema.Datapoint).filter_by(source_id=agency_id).all()
            )
            objects_to_delete.extend(datapoints)
            logger.info("Will delete %d datapoints", len(datapoints))

            settings = (
                session.query(schema.AgencySetting).filter_by(source_id=agency_id).all()
            )
            objects_to_delete.extend(settings)
            logger.info("Will delete %d agency settings", len(settings))

            jurisdictions = (
                session.query(schema.AgencyJurisdiction)
                .filter_by(source_id=agency_id)
                .all()
            )
            objects_to_delete.extend(jurisdictions)
            logger.info("Will delete %d jurisdictions", len(jurisdictions))

            spreadsheets = (
                session.query(schema.Spreadsheet).filter_by(agency_id=agency_id).all()
            )
            objects_to_delete.extend(spreadsheets)
            logger.info("Will delete %d spreadsheets", len(spreadsheets))

            agencies = session.query(schema.Agency).filter_by(id=agency_id).all()
            objects_to_delete.extend(agencies)
            logger.info("Will delete %d agencies", len(agencies))

            if dry_run is False:
                for obj in objects_to_delete:
                    session.delete(obj)
                session.commit()
                logger.info("%s deleted", agency.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        delete_agency(agency_id=args.agency_id, dry_run=args.dry_run)
