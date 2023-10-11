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
Updates start and end dates for non-calendar year annual reports 
to start one year earlier.

python -m recidiviz.tools.justice_counts.update_fiscal_year \
  --environment=justice-counts-production \
  --agency_id=532 \
  --dry_run=true
"""

import argparse
import logging

from dateutil.relativedelta import relativedelta
from sqlalchemy import func

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.schema.justice_counts import schema
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
        "--agency_id",
        type=int,
        help="The id of the agency who's reports you want to update.",
        required=True,
    )

    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def update_start_end_date_of_fiscal_report(agency_id: int, dry_run: bool) -> None:
    """
    Changes existing fiscal reports for a particular agency such that they represent the previous year.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(
            database_key=database_key,
            autocommit=False,
        ) as session:
            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=[agency_id]
            )
            relevant_agency_ids = [a.id for a in child_agencies] + [agency_id]
            fiscal_reports = (
                session.query(schema.Report)
                .filter(
                    func.extract("month", schema.Report.date_range_start) != 1,
                    schema.Report.type == "ANNUAL",
                    schema.Report.source_id.in_(relevant_agency_ids),
                )
                .all()
            )
            print("AMOUNT OF REPORTS:", len(fiscal_reports))
            for report in fiscal_reports:
                msg = "DRY RUN:  " if dry_run is True else ""
                msg += f"Updating Report ID: {report.id} from {report.source.name}\n"
                msg += f" ----- Before update ------ \nStarting date: {report.date_range_start.month}-{report.date_range_start.day}-{report.date_range_start.year} \n"
                msg += f"End date: {report.date_range_end.month}-{report.date_range_end.day}-{report.date_range_end.year} \n"
                report.date_range_start -= relativedelta(years=1)
                report.date_range_end -= relativedelta(years=1)
                for datapoint in report.datapoints:
                    datapoint.start_date -= relativedelta(years=1)
                    datapoint.end_date -= relativedelta(years=1)
                msg += f" ---- After update ------ \nStarting date: {report.date_range_start.month}-{report.date_range_start.day}-{report.date_range_start.year} \n"
                msg += f"End date: {report.date_range_end.month}-{report.date_range_end.day}-{report.date_range_end.year} \n"
                print(msg)

            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        update_start_end_date_of_fiscal_report(
            dry_run=args.dry_run,
            agency_id=args.agency_id,
        )
