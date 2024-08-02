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
Pulls information of agencies who have published capacity and cost metrics.
This helps us determine what agencies would have non-empty Agency Dashboards.

python -m recidiviz.justice_counts.jobs.pull_agencies_with_published_data \
    --project_id=justice-counts-production \
    --run_as_script=false \
    --credentials-path=<path>

"""
import argparse
import datetime
import logging
import urllib.parse
from collections import defaultdict
from typing import Any, Dict, Set, Tuple

import sentry_sdk
from google.oauth2.service_account import Credentials

from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.justice_counts.metrics.metric_definition import MetricCategory
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
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


# Sheet Title: Agency Dashboard Pull
# Link: https://docs.google.com/spreadsheets/d/1TcskbQat7a3OA7X-KRra74qBcXZMZVg5nOfdRFa9-64/edit#gid=251741843
SPREADSHEET_ID = "1TcskbQat7a3OA7X-KRra74qBcXZMZVg5nOfdRFa9-64"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        dest="project_id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        type=str,
        required=True,
    )
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    parser.add_argument("--run_as_script", type=str_to_bool, default=False)

    return parser


def get_reported_metrics_and_dashboard_helper(
    session: Session,
) -> Tuple[Dict, Dict, Dict]:
    """
    Helper function that gets all published reports and builds 3 dictionaries:
    - agency_id_to_reported_metrics_with_data
    - agency_id_to_time_periods
    - agency_id_to_agency_with_dashboard_data
    These help in determining which agencies have published capacity and cost metrics
    and determine what agencies would have non-empty Agency Dashboards.
    """
    published_reports = session.query(schema.Report).filter(
        schema.Report.status == schema.ReportStatus.PUBLISHED
    )
    agency_id_to_reported_metrics_with_data: Dict[int, Set[str]] = defaultdict(set)
    agency_id_to_time_periods: Dict[
        int, Set[Tuple[datetime.datetime, datetime.datetime]]
    ] = defaultdict(set)
    agency_id_to_agency_with_dashboard_data = {}
    for report in published_reports:
        report_capacity_and_cost_metrics = {
            datapoint.metric_definition_key
            for datapoint in report.datapoints
            if METRIC_KEY_TO_METRIC.get(datapoint.metric_definition_key) is not None
            and datapoint.value is not None
            and METRIC_KEY_TO_METRIC.get(datapoint.metric_definition_key).category == MetricCategory.CAPACITY_AND_COST  # type: ignore[union-attr]
        }

        if len(report_capacity_and_cost_metrics) > 0:
            if report.source_id not in agency_id_to_agency_with_dashboard_data:
                agency_id_to_agency_with_dashboard_data[
                    report.source_id
                ] = report.source
            agency_id_to_time_periods[report.source_id].add(
                (report.date_range_start, report.date_range_end)
            )
            agency_id_to_reported_metrics_with_data[report.source_id].update(
                report_capacity_and_cost_metrics
            )
    logger.info(
        "Number of agencies with published Capacity and Cost data: %s \n",
        len(agency_id_to_agency_with_dashboard_data),
    )
    return (
        agency_id_to_reported_metrics_with_data,
        agency_id_to_time_periods,
        agency_id_to_agency_with_dashboard_data,
    )


def calculate_columns_helper(
    agency_id_to_time_periods: Dict[
        int, Set[Tuple[datetime.datetime, datetime.datetime]]
    ],
    agency_name: str,
    agency_id: int,
    environment: str,
) -> Tuple[str, str, str]:
    dashboard_site = (
        f"https://dashboard-staging.justice-counts.org/agency/{str(agency_id)}/"
        if environment == "STAGING"
        else f"https://dashboard-demo.justice-counts.org/agency/{str(agency_id)}/"
    )
    api_link = (
        f"https://publisher-staging.justice-counts.org/api/v2/agencies/{str(agency_id)}/published_data"
        if environment == "STAGING"
        else f"https://publisher.justice-counts.org/api/v2/agencies/{str(agency_id)}/published_data"
    )
    # Encode agency name as URI
    dashboard_link = dashboard_site + urllib.parse.quote(agency_name).lower()
    num_time_periods_reported = str(len(agency_id_to_time_periods[agency_id]))
    return dashboard_link, api_link, num_time_periods_reported


def pull_agencies_with_published_capacity_and_cost_data(
    session: Session, environment: str, google_credentials: Any
) -> None:
    """
    Pulls agencies that have published data and writes their information to a google sheet.
    """

    data_to_write = [
        [
            "Name",
            "Agency Dashboard Link",
            "Number of Metrics With Data",
            "Number of Time Periods Reported",
            "Is V0 Dashboard Enabled?",
            "Agency Sectors",
            "API Link",
        ]
    ]
    (
        agency_id_to_reported_metrics_with_data,
        agency_id_to_time_periods,
        agency_id_to_agency_with_dashboard_data,
    ) = get_reported_metrics_and_dashboard_helper(session=session)

    for (
        agency_id,
        agency,
    ) in agency_id_to_agency_with_dashboard_data.items():
        dashboard_link, api_link, num_time_periods_reported = calculate_columns_helper(
            agency_id_to_time_periods=agency_id_to_time_periods,
            agency_name=agency.name,
            agency_id=agency.id,
            environment=environment,
        )
        data_to_write.append(
            [
                agency.name,
                dashboard_link,
                str(len(agency_id_to_reported_metrics_with_data[agency_id])),
                num_time_periods_reported,
                "Yes" if agency.is_dashboard_enabled is True else "No",
                ", ".join(agency.systems),
                api_link,
            ]
        )

    # Sort data so that the agencies with enabled dashboards are listed first
    data_to_write.sort(key=lambda row: row[-1] == "No")
    now = datetime.datetime.now()
    new_sheet_title = f"{now.month}-{now.day}-{now.year}"
    write_data_to_spreadsheet(
        google_credentials=google_credentials,
        data_to_write=data_to_write,
        new_sheet_title=new_sheet_title,
        spreadsheet_id=SPREADSHEET_ID,
        logger=logger,
        index=0,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    args = create_parser().parse_args()
    if args.run_as_script is True:
        environment_str = (
            "PRODUCTION"
            if args.project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
            else "STAGING"
        )
        # When running locally, point to JSON file with service account credentials.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = Credentials.from_service_account_file(args.credentials_path)
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
                    pull_agencies_with_published_capacity_and_cost_data(
                        session=global_session,
                        environment=environment_str,
                        google_credentials=credentials,
                    )
