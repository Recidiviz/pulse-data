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
"""Job that writes to the Justice Counts Data Pull google spreadsheet with data about
all agencies with Publisher accounts. Includes fields like "num_record_with_data",
"num_metrics_configured", etc.

pipenv run python -m recidiviz.justice_counts.jobs.csg_data_pull \
  --project-id=PROJECT_ID \
  --dry-run=true
"""
import argparse
import datetime
import logging
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List

import pandas as pd
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.utils.constants import AGENCIES_TO_EXCLUDE
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
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

# Spreadsheet Name: Justice Counts Data Pull Spreadsheet
# https://docs.google.com/spreadsheets/d/1Vcz110SWJoTE345w3buPd8oYnwqu-Q_mIJ4C-9z_CC0/edit#gid=870547342
SPREADSHEET_ID = "1Vcz110SWJoTE345w3buPd8oYnwqu-Q_mIJ4C-9z_CC0"

CREATED_AT = "created_at"
LAST_LOGIN = "last_login"
LAST_UPDATE = "last_update"
NUM_RECORDS_WITH_DATA = "num_records_with_data"
NUM_METRICS_WITH_DATA = "num_metrics_with_data"
NUM_METRICS_CONFIGURED = "num_metrics_configured"
NUM_METRICS_AVAILABLE = "num_metrics_available"
NUM_METRICS_UNAVAILABLE = "num_metrics_unavailable"
IS_SUPERAGENCY = "is_superagency"
IS_CHILD_AGENCY = "is_child_agency"

# To add:
# NUM_METRICS_UNCONFIGURED = "num_metrics_unconfigured"
# NUM_METRICS_DEFINED = "num_metrics_defined"

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
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def summarize(datapoints: List[schema.Datapoint]) -> Dict[str, Any]:
    """Given a list of Datapoints belonging to a particular agency,
    return a dictionary containing summary statistics.
    """
    report_id_to_datapoints = defaultdict(list)
    metric_key_to_datapoints = defaultdict(list)
    metrics_configured = set()
    metrics_available = set()
    metrics_unavailable = set()

    last_update = None
    for datapoint in datapoints:
        datapoint_last_update = datapoint["last_updated"]
        # If the agency's true last_update was prior to the new last_update field was
        # added to our schema, use the most recent created_at field
        if datapoint_last_update is not None:
            if last_update is None or (datapoint_last_update.date() > last_update):
                last_update = datapoint_last_update.date()
        elif datapoint["created_at"] is not None:
            datapoint_created_at = datapoint["created_at"].date()
            if last_update is None or (datapoint_created_at > last_update):
                last_update = datapoint_created_at

        # Process report datapoints (i.e. those that contain data for a time period)
        if datapoint["is_report_datapoint"] and datapoint["value"] is not None:
            # Group datapoints by report (i.e. time period)
            report_id_to_datapoints[datapoint["report_id"]].append(datapoint)
            # Group datapoints by metric
            metric_key_to_datapoints[datapoint["metric_definition_key"]].append(
                datapoint
            )
        # Process non-report datapoints (i.e. tthose that contain info about metric configuration)
        elif (
            not datapoint["is_report_datapoint"]
            # The following filters to non-report datapoints that contain information about whether
            # the top-level metric is turned on or off
            and not datapoint["dimension_identifier_to_member"]
            and not datapoint["context_key"]
            and not datapoint["includes_excludes_key"]
            and datapoint["enabled"] is not None
        ):
            # We consider a metric configured if it is either turned on or off
            metrics_configured.add(datapoint["metric_definition_key"])
            if datapoint["enabled"] is True:
                metrics_available.add(datapoint["metric_definition_key"])
            elif datapoint["enabled"] is False:
                metrics_unavailable.add(datapoint["metric_definition_key"])

    return {
        LAST_UPDATE: last_update or "",
        NUM_RECORDS_WITH_DATA: len(report_id_to_datapoints),
        NUM_METRICS_WITH_DATA: len(metric_key_to_datapoints),
        NUM_METRICS_CONFIGURED: len(metrics_configured),
        NUM_METRICS_AVAILABLE: len(metrics_available),
        NUM_METRICS_UNAVAILABLE: len(metrics_unavailable),
    }


def generate_agency_summary_csv(session: Session, dry_run: bool) -> None:
    """Generates a CSV with data about all agencies with Publisher accounts."""

    # Authenticate with the Google Sheets API
    credentials = GoogleCredentials.get_application_default()
    # GoogleCredentials.get_application_default() will use the service account
    # assigned to the Cloud Run Job. The service account has access to the spreadsheet
    # with editor permissions.
    spreadsheet_service = build("sheets", "v4", credentials=credentials)
    auth0_client = Auth0Client(  # nosec
        domain_secret_name="justice_counts_auth0_api_domain",
        client_id_secret_name="justice_counts_auth0_api_client_id",
        client_secret_secret_name="justice_counts_auth0_api_client_secret",
    )
    auth0_users = auth0_client.get_all_users()
    auth0_user_id_to_user = {user["user_id"]: user for user in auth0_users}

    original_agencies = session.execute(
        "select * from source where type = 'agency' and LOWER(name) not like '%test%'"
    ).all()
    agency_user_account_associations = session.execute(
        "select * from agency_user_account_association"
    ).all()
    users = session.execute("select * from user_account").all()
    datapoints = session.execute("select * from datapoint").all()

    user_id_to_auth0_user = {
        user["id"]: auth0_user_id_to_user.get(user["auth0_user_id"]) for user in users
    }

    agency_id_to_users = defaultdict(list)
    for assoc in agency_user_account_associations:
        auth0_user = user_id_to_auth0_user[assoc["user_account_id"]]
        if (
            # Skip over CSG and Recidiviz users -- we shouldn't
            # count as a true login!
            auth0_user
            and "csg" not in auth0_user["email"]
            and "recidiviz" not in auth0_user["email"]
        ):
            agency_id_to_users[assoc["agency_id"]].append(auth0_user)
    agencies_to_exclude = AGENCIES_TO_EXCLUDE.keys()
    agencies = [
        dict(a) for a in original_agencies if a["id"] not in agencies_to_exclude
    ]
    agency_id_to_agency = {a["id"]: a for a in agencies}

    logger.info("Number of agencies: %s", len(agencies))

    for agency in agencies:
        users = agency_id_to_users[agency["id"]]
        agency_created_at = agency.get("created_at")
        last_login = None
        first_user_created_at = None
        for user in users:
            # If the agency was created before the agency.created_at field was
            # added to our schema, we use the date of the first created user
            # that belongs to that agency
            if agency_created_at is None:
                user_created_at = datetime.datetime.strptime(
                    user["created_at"].split("T")[0], "%Y-%m-%d"
                ).date()
                if first_user_created_at is None or (
                    user_created_at < first_user_created_at
                ):
                    first_user_created_at = user_created_at
            # The agency's last_login is the most recent login date of
            # any of its users.
            if "last_login" not in user:
                continue
            user_last_login = datetime.datetime.strptime(
                user["last_login"].split("T")[0], "%Y-%m-%d"
            ).date()
            if not last_login or (user_last_login > last_login):
                last_login = user_last_login

        agency[LAST_LOGIN] = last_login or ""
        agency[CREATED_AT] = agency_created_at or first_user_created_at or ""
        agency[LAST_UPDATE] = ""
        agency[NUM_RECORDS_WITH_DATA] = 0
        agency[NUM_METRICS_WITH_DATA] = 0
        agency[NUM_METRICS_CONFIGURED] = 0
        agency[NUM_METRICS_AVAILABLE] = 0
        agency[NUM_METRICS_UNAVAILABLE] = 0
        agency[IS_SUPERAGENCY] = bool(agency["is_superagency"])
        agency[IS_CHILD_AGENCY] = bool(agency["super_agency_id"])

    agency_id_to_datapoints_groupby = groupby(
        sorted(datapoints, key=lambda x: x["source_id"]),
        key=lambda x: x["source_id"],
    )
    agency_id_to_datapoints = {k: list(v) for k, v in agency_id_to_datapoints_groupby}

    for agency_id, datapoints in agency_id_to_datapoints.items():
        if agency_id not in agency_id_to_agency:
            continue

        data = summarize(datapoints=datapoints)
        agency_id_to_agency[agency_id] = dict(agency_id_to_agency[agency_id], **data)

    agencies = list(agency_id_to_agency.values())

    columns = [
        "name",
        "state",
        CREATED_AT,
        LAST_LOGIN,
        LAST_UPDATE,
        NUM_RECORDS_WITH_DATA,
        NUM_METRICS_WITH_DATA,
        NUM_METRICS_CONFIGURED,
        NUM_METRICS_AVAILABLE,
        NUM_METRICS_UNAVAILABLE,
        IS_SUPERAGENCY,
        IS_CHILD_AGENCY,
        "systems",
    ]
    df = (
        pd.DataFrame.from_records(agencies)
        # Sort agencies alphabetically by name
        .sort_index().rename(columns={"state_code": "state"})
        # Put columns in desired order
        .reindex(columns=columns)
    )

    now = datetime.datetime.now()
    new_sheet_title = f"{now.month}-{now.day}-{now.year}"

    if dry_run is False:
        # Create a new worksheet in the spreadsheet
        request = {"addSheet": {"properties": {"title": new_sheet_title, "index": 1}}}

        # Create new sheet
        spreadsheet_service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body={"requests": [request]}
        ).execute()

        logger.info("New Sheet Created")

        # Format data to fit Google API specifications.
        # Google API spec requires a list of lists,
        # each list representing a row.
        data_to_write = [columns]
        data_to_write.extend(df.astype(str).values.tolist())
        body = {"values": data_to_write}
        range_name = f"{new_sheet_title}!A1"  #
        spreadsheet_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body=body,
        ).execute()

        logger.info("Sheet '%s' added and data written.", new_sheet_title)


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
                    generate_agency_summary_csv(
                        session=global_session, dry_run=args.dry_run
                    )
    else:
        database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.JUSTICE_COUNTS,
        )
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        global_session = Session(bind=justice_counts_engine)
        generate_agency_summary_csv(session=global_session, dry_run=args.dry_run)
