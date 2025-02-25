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
Updates calendar-year reports to be fiscal-year reports

python -m recidiviz.tools.justice_counts.change_calendar_year_to_fiscal_year \
  --project-id=justice-counts-production \
  --agency_id=532 \
  --dry_run=true
"""

import argparse
import datetime
import logging

from dateutil.relativedelta import relativedelta

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.report import ReportInterface
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
        dest="project_id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        type=str,
        required=True,
    )

    parser.add_argument(
        "--agency_id",
        type=int,
        help="The id of the agency who's reports you want to update.",
        required=True,
    )

    parser.add_argument("--dry_run", type=str_to_bool, default=True)
    return parser


def update_existing_fiscal_year_report(
    existing_fiscal_year_report: schema.Report,
    existing_calendar_year_report: schema.Report,
    msg: str,
) -> None:
    """Updates existing fiscal year reports and datapoints. In this case,
    we don't want to create any duplicate datapoints or reports, we
    just want to update any datapoints in the existing fiscal report
    that have values of None or that have differing values than the
    calendar-year report.
    """

    msg += "A fiscal-year report already exists for this time frame\n"
    existing_fiscal_report_datapoints_dict = (
        ReportInterface.get_existing_datapoints_dict(
            reports=[existing_fiscal_year_report]
        )
    )

    existing_datapoints_dict_calendar_year = (
        ReportInterface.get_existing_datapoints_dict(
            reports=[existing_calendar_year_report]
        )
    )

    if len(existing_datapoints_dict_calendar_year) == 0:
        msg += "Nothing to migrate, the calendar-year report is empty."
        return

    msg += "----------UPDATING DATAPOINTS------ \n\n"

    for (
        key,
        existing_calendar_year_datapoint,
    ) in existing_datapoints_dict_calendar_year.items():
        fiscal_year_key = (
            key[0] - relativedelta(months=6),
            key[1] - relativedelta(months=6),
            key[2],
            key[3],
            key[4],
            key[5],
        )
        # Case 1: Fiscal-year report does not have a datapoint for that time-range.
        if existing_fiscal_report_datapoints_dict.get(fiscal_year_key) is None:
            # If the fiscal report DOES NOT have a datapoint for that metric,
            # then change the start / end date and report_id of the datapoint
            # in the calendar year report.
            if existing_calendar_year_datapoint.value is not None:
                msg += f"UPDATING - METRIC_KEY: {existing_calendar_year_datapoint.metric_definition_key}, \
DIMENSION_IDENTIFIER_TO_MEMBER: {existing_calendar_year_datapoint.metric_definition_key} from None -> {existing_calendar_year_datapoint.value}\n"
                existing_calendar_year_datapoint.start_date -= relativedelta(months=6)
                existing_calendar_year_datapoint.end_date -= relativedelta(months=6)
                existing_calendar_year_datapoint.report_id = (
                    existing_fiscal_year_report.id
                )
            else:
                msg += f"NOT UPDATING - METRIC_KEY: {existing_calendar_year_datapoint.metric_definition_key}, \
DIMENSION_IDENTIFIER_TO_MEMBER: {existing_calendar_year_datapoint.metric_definition_key} because the calendar-year report value is None!\n\n"

        # Case 2: Fiscal-year has a datapoint with not-None value, calendar year has a
        # datapoint with a None value
        elif (
            existing_fiscal_report_datapoints_dict.get(fiscal_year_key) is not None
            and existing_calendar_year_datapoint.value is None
        ):
            if (
                existing_fiscal_report_datapoints_dict.get(fiscal_year_key).value  # type: ignore[union-attr]
                is None
                and existing_calendar_year_datapoint.value is not None
            ):
                # Do nothing, since we don't want to overwrite an existing value with None
                msg += f"NOT UPDATING - METRIC_KEY: {existing_calendar_year_datapoint.metric_definition_key}, \
DIMENSION_IDENTIFIER_TO_MEMBER {existing_calendar_year_datapoint.metric_definition_key} because the calendar-year \
report value is None and fiscal-year report value is not None!\n\n"
        # Case 3: All other cases. Replace the fiscal year datapoint value with the calendar year
        # datapoint value. This should be safe to do no matter what. Either they are both None, they
        # both have values (in which case we want to prefer calendar year), or fiscal is None and
        # calendar is not None.
        else:
            fiscal_year_datapoint = existing_fiscal_report_datapoints_dict[
                fiscal_year_key
            ]
            msg += f"UPDATING - The {fiscal_year_datapoint.metric_definition_key} datapoint in the fiscal year \
report from {fiscal_year_datapoint.value} -> {existing_calendar_year_datapoint.value}\n\n"
            fiscal_year_datapoint.value = existing_calendar_year_datapoint.value

        print(msg)


def transform_calendar_year_report_to_fiscal_year_report(
    msg: str,
    calendar_year_report: schema.Report,
    new_date_range_start: datetime.datetime,
    new_date_range_end: datetime.datetime,
) -> None:
    # If there is no existing fiscal-year report, change the start
    # and end date of the current calendar-year report and its datapoints.
    msg += "No fiscal-year report exists for this time frame, changing the start/end date + all of the datapoints for this calendar-year report.\n"
    msg += f" ----- Before update ------ \nStarting date: {calendar_year_report.date_range_start.month}-{calendar_year_report.date_range_start.day}-{calendar_year_report.date_range_start.year} \n"
    msg += f"End date: {calendar_year_report.date_range_end.month}-{calendar_year_report.date_range_end.day}-{calendar_year_report.date_range_end.year} \n"
    calendar_year_report.date_range_start = new_date_range_start
    calendar_year_report.date_range_end = new_date_range_end
    for datapoint in calendar_year_report.datapoints:
        datapoint.start_date = new_date_range_start
        datapoint.end_date = new_date_range_end
    msg += f" ---- After update ------ \nStarting date: {calendar_year_report.date_range_start.month}-{calendar_year_report.date_range_start.day}-{calendar_year_report.date_range_start.year} \n"
    msg += f"End date: {calendar_year_report.date_range_end.month}-{calendar_year_report.date_range_end.day}-{calendar_year_report.date_range_end.year} \n"
    print(msg)
    msg += "--------------------------------------------------------\n"


def change_calendar_year_to_fiscal_year(agency_id: int, dry_run: bool) -> None:
    """
    Changes existing calendar year reports for a particular agency such that they represent the fiscal-year.
    """
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
            child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
                session=session, agency_ids=[agency_id]
            )
            relevant_agency_ids = [a.id for a in child_agencies] + [agency_id]
            reports = (
                session.query(schema.Report)
                .filter(
                    schema.Report.type == "ANNUAL",
                    schema.Report.source_id.in_(relevant_agency_ids),
                )
                .all()
            )
            date_range_and_agency_id_to_fiscal_report = {
                (
                    report.date_range_start,
                    report.date_range_end,
                    report.source.id,
                ): report
                for report in reports
                if report.date_range_start.month == 7
            }

            calendar_year_reports = [
                report for report in reports if report.date_range_start.month == 1
            ]

            print("AMOUNT OF REPORTS TO UPDATE:", len(calendar_year_reports), "\n")
            for calendar_year_report in calendar_year_reports:
                msg = "--------------------------------------------------------\n\n"
                msg += "DRY RUN:  " if dry_run is True else ""
                msg += f"Updating {calendar_year_report.date_range_start.month}/\
{calendar_year_report.date_range_start.day}\
/{calendar_year_report.date_range_start.year} \
- {calendar_year_report.date_range_end.month}\
/{calendar_year_report.date_range_end.day}\
/{calendar_year_report.date_range_end.year} \
Report from {calendar_year_report.source.name}\n"
                # The new date range should be from June the prior year, to July of the current
                # year of the calendar year report. For example, if the report is CY 2023, the
                # the new fiscal year report should be June of 2022 - July of 2023.
                new_date_range_start = (
                    calendar_year_report.date_range_start - relativedelta(months=6)
                )
                new_date_range_end = (
                    calendar_year_report.date_range_end - relativedelta(months=6)
                )
                existing_fiscal_year_report = (
                    date_range_and_agency_id_to_fiscal_report.get(
                        (
                            new_date_range_start,
                            new_date_range_end,
                            calendar_year_report.source.id,
                        )
                    )
                )

                if existing_fiscal_year_report is not None:
                    update_existing_fiscal_year_report(
                        existing_fiscal_year_report=existing_fiscal_year_report,
                        existing_calendar_year_report=calendar_year_report,
                        msg=msg,
                    )
                    if dry_run is False:
                        session.delete(calendar_year_report)

                else:
                    transform_calendar_year_report_to_fiscal_year_report(
                        msg=msg,
                        calendar_year_report=calendar_year_report,
                        new_date_range_start=new_date_range_start,
                        new_date_range_end=new_date_range_end,
                    )

            if dry_run is False:
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        change_calendar_year_to_fiscal_year(
            dry_run=args.dry_run,
            agency_id=args.agency_id,
        )
