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
Script that sends emails to all subscribed users in an agency. This will be turned
into a Cloud Run Job in the future to automate email reminders. For the P0 of the 
email reminder project, emails will be sent on the 15th of every month if the agency 
is missing metrics in their most recent annual or monthly report.
"""
import argparse
import datetime
import logging

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.utils.email import (
    send_reminder_emails,
    send_reminder_emails_for_superagency,
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
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

today = datetime.date.today()


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=False,
    )
    parser.add_argument(
        "--agency_id",
        help="ID of the agency you want to send emails to",
        required=True,
    )
    parser.add_argument("--day", type=int, default=today.day)
    parser.add_argument("--month", type=int, default=today.month)
    parser.add_argument("--year", type=int, default=today.year)
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(
            schema_type=schema_type,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ):
            with SessionFactory.for_proxy(
                database_key=database_key,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                autocommit=False,
            ) as session:
                agency = AgencyInterface.get_agency_by_id(
                    session=session, agency_id=args.agency_id
                )
                if agency.is_superagency is not True:
                    send_reminder_emails(
                        session=session,
                        agency_id=args.agency_id,
                        dry_run=args.dry_run,
                        logger=logger,
                        today=datetime.date(
                            day=args.day, month=args.month, year=args.year
                        ),
                    )
                else:
                    send_reminder_emails_for_superagency(
                        session=session,
                        agency_id=args.agency_id,
                        dry_run=args.dry_run,
                        logger=logger,
                        today=datetime.date(
                            day=args.day, month=args.month, year=args.year
                        ),
                    )
