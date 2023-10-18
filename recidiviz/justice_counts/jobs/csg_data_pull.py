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
  --dry-run=true \
  --credentials-path=<path>
"""
import argparse
import datetime
import logging
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Tuple

import pandas as pd
from google.oauth2.service_account import Credentials
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
LOGIN_THIS_WEEK = "login_this_week"
LAST_UPDATE = "last_update"
NUM_RECORDS_WITH_DATA = "num_records_with_data"
NUM_METRICS_WITH_DATA = "num_metrics_with_data"
NUM_METRICS_CONFIGURED = "num_metrics_configured"
NUM_METRICS_AVAILABLE = "num_metrics_available"
NUM_METRICS_UNAVAILABLE = "num_metrics_unavailable"
METRIC_CONFIG_THIS_WEEK = "metric_configured_this_week"
IS_SUPERAGENCY = "is_superagency"
IS_CHILD_AGENCY = "is_child_agency"
NEW_STATE_THIS_WEEK = "new_state_this_week"
NEW_AGENCY_THIS_WEEK = "new_agency_this_week"
DATA_SHARED_THIS_WEEK = "data_shared_this_week"

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
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    return parser


def summarize(
    datapoints: List[schema.Datapoint], today: datetime.datetime
) -> Dict[str, Any]:
    """Given a list of Datapoints belonging to a particular agency, return a dictionary
    containing summary statistics.
    """
    report_id_to_datapoints = defaultdict(list)
    metric_key_to_datapoints = defaultdict(list)
    metrics_configured = set()
    metrics_available = set()
    metrics_unavailable = set()

    # last_update is the most recent date in which an agency or report datapoint
    # has been changed. For report datapoints, we do not consider null values
    last_update = datetime.datetime(1970, 1, 1)
    # data_shared_this week is a boolean that indicates if an agency has updated at least
    # 1 report datapoint (does not consider agency datapoints). We do not consider null
    # values
    data_shared_this_week = False
    # last_metric_config_update is the most recent date in which a metric was either
    # enabled or disabled
    last_metric_config_update = datetime.datetime(1970, 1, 1)
    metric_configured_this_week = False
    for datapoint in datapoints:
        if datapoint["is_report_datapoint"] is True and datapoint["value"] is None:
            # Ignore report datapoints with no values
            continue
        datapoint_last_update = datapoint["last_updated"]

        # Part 1: Things that involve a timestamp
        if datapoint_last_update is not None:
            # last_update involves a timestamp and report and agency datapoints
            last_update = max(last_update, datapoint_last_update)
            if datapoint["is_report_datapoint"] is True:
                # data_shared_this_week involves a timestamp and report datapoints
                data_shared_this_week = last_update.replace(
                    tzinfo=datetime.timezone.utc
                ) > (today + datetime.timedelta(days=-7))
            elif (
                # The following filters to agency datapoints that contain information about whether
                # the top-level metric is turned on or off
                not datapoint["dimension_identifier_to_member"]
                and not datapoint["context_key"]
                and not datapoint["includes_excludes_key"]
                and datapoint["enabled"] is not None
            ):
                # metric_configured_this_week involves a timestamp and agency datapoints
                last_metric_config_update = max(
                    last_metric_config_update, datapoint_last_update
                )
                metric_configured_this_week = last_metric_config_update.replace(
                    tzinfo=datetime.timezone.utc
                ) > (today + datetime.timedelta(days=-7))
        elif datapoint["created_at"] is not None:
            # If the agency's true last_update was prior to the new last_update field was
            # added to our schema, use the most recent created_at field
            last_update = max(last_update, datapoint["created_at"])

        # Part 2: Things that do not involve a timestamp
        # Process report datapoints (i.e. those that contain data for a time period)
        if datapoint["is_report_datapoint"] is True:
            # Group datapoints by report (i.e. time period)
            report_id_to_datapoints[datapoint["report_id"]].append(datapoint)
            # Group datapoints by metric
            metric_key_to_datapoints[datapoint["metric_definition_key"]].append(
                datapoint
            )
        # Process agency datapoints (i.e. those that contain info about metric configuration)
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
        LAST_UPDATE: last_update.date()
        if last_update != datetime.datetime(1970, 1, 1)
        else "",
        NUM_RECORDS_WITH_DATA: len(report_id_to_datapoints),
        NUM_METRICS_WITH_DATA: len(metric_key_to_datapoints),
        NUM_METRICS_CONFIGURED: len(metrics_configured),
        NUM_METRICS_AVAILABLE: len(metrics_available),
        NUM_METRICS_UNAVAILABLE: len(metrics_unavailable),
        METRIC_CONFIG_THIS_WEEK: metric_configured_this_week,
        DATA_SHARED_THIS_WEEK: data_shared_this_week,
    }


def get_all_data(
    session: Session,
) -> Tuple[Dict[str, schema.UserAccount], List[Any], List[Any], List[Any], List[Any]]:
    """Retrieve agency, user, agency_user_account_association, and datapoint data from both
    Auth0 as well as our database.
    """
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

    agencies_to_exclude = AGENCIES_TO_EXCLUDE.keys()
    agencies = [
        dict(a) for a in original_agencies if a["id"] not in agencies_to_exclude
    ]
    return (
        auth0_user_id_to_user,
        agencies,
        agency_user_account_associations,
        users,
        datapoints,
    )


def create_new_agency_columns(
    agencies: List[schema.Agency],
    agency_id_to_users: Dict[str, List[schema.UserAccount]],
    today: datetime.datetime,
) -> Tuple[List[schema.Agency], Dict[str, int]]:
    """Given a list of agencies and their users, create and populate the following columns:
    - last_login
    - created_at
    - is_superagency
    - is_child_agency
    - new_agency_this_week
    """

    # A dictionary that maps state codes to their count of new agencies: {"us_state": 2}
    state_code_by_new_agency_count: Dict[str, int] = defaultdict(int)
    for agency in agencies:
        users = agency_id_to_users[agency["id"]]

        agency_created_at = agency.get("created_at")
        agency_created_at_str = None
        if agency_created_at:
            agency_created_at_str = agency_created_at.strftime("%Y-%m-%d")

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

        new_agency_this_week = False
        # this should not be None as of 09/27/2023, so we don't need another condition
        if agency_created_at is not None:
            if agency_created_at > (today + datetime.timedelta(days=-7)):
                new_agency_this_week = True
                agency_state_code = agency["state_code"]
                state_code_by_new_agency_count[agency_state_code] += 1

        agency[LAST_LOGIN] = last_login or ""
        agency[LOGIN_THIS_WEEK] = (
            last_login > (today + datetime.timedelta(days=-7)).date()
            if last_login
            else False
        )
        agency[CREATED_AT] = agency_created_at_str or first_user_created_at or ""
        agency[LAST_UPDATE] = ""
        agency[NUM_RECORDS_WITH_DATA] = 0
        agency[NUM_METRICS_WITH_DATA] = 0
        agency[DATA_SHARED_THIS_WEEK] = False
        agency[NUM_METRICS_CONFIGURED] = 0
        agency[NUM_METRICS_AVAILABLE] = 0
        agency[NUM_METRICS_UNAVAILABLE] = 0
        agency[METRIC_CONFIG_THIS_WEEK] = False
        agency[IS_SUPERAGENCY] = bool(agency["is_superagency"])
        agency[IS_CHILD_AGENCY] = bool(agency["super_agency_id"])
        agency[NEW_AGENCY_THIS_WEEK] = new_agency_this_week

    return agencies, state_code_by_new_agency_count


def populate_new_state_this_week(
    agencies: List[schema.Agency],
    state_code_by_all_agency_count: Dict[str, int],
    state_code_by_new_agency_count: Dict[str, int],
) -> List[schema.Agency]:
    """Given a list of agencies, create and populate the new_state_this_week column."""
    for agency in agencies:
        new_state_this_week = False
        if agency[NEW_AGENCY_THIS_WEEK] is True:
            potential_new_state_code = agency["state_code"]
            num_total_agencies_for_state = state_code_by_all_agency_count.get(
                potential_new_state_code
            )
            if num_total_agencies_for_state == 1:
                # A new agency is the only agency associated with this state
                # So this must be a new state
                new_state_this_week = True
            elif (
                num_total_agencies_for_state
                == state_code_by_new_agency_count[potential_new_state_code]
            ):
                # Multiple agencies are associated with the state, but they are all new
                # agencies. So this is also a new state.
                new_state_this_week = True
        agency[NEW_STATE_THIS_WEEK] = new_state_this_week
    return agencies


def generate_agency_summary_csv(
    session: Session, dry_run: bool, google_credentials: Any
) -> None:
    """Generates a CSV with data about all agencies with Publisher accounts."""

    # First, pull user, agency, and datapoint data from auth0 and our database
    (
        auth0_user_id_to_user,
        agencies,
        agency_user_account_associations,
        users,
        datapoints,
    ) = get_all_data(session=session)
    logger.info("Number of agencies: %s", len(agencies))

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

    # A dictionary that maps agency ids to agency objects
    agency_id_to_agency = {}
    # A dictionary that maps all state codes to their count of total agencies: {"us_il": 4, "us_oh": 1}
    state_code_by_all_agency_count: Dict[str, int] = defaultdict(int)

    # Next, populate agency_id_to_agency and all_state_code_by_count
    for a in agencies:
        agency_id_to_agency[a["id"]] = a
        state_code = a["state_code"]
        state_code_by_all_agency_count[state_code] += 1

    today = datetime.datetime.now(tz=datetime.timezone.utc)
    # Create new agency columns and populate state_code_by_new_agency_count
    agencies, state_code_by_new_agency_count = create_new_agency_columns(
        agencies=agencies, agency_id_to_users=agency_id_to_users, today=today
    )

    # Now that state_code_by_new_agency_count is populated, we can determine if
    # NEW_STATE_THIS_WEEK is True
    agencies = populate_new_state_this_week(
        agencies=agencies,
        state_code_by_all_agency_count=state_code_by_all_agency_count,
        state_code_by_new_agency_count=state_code_by_new_agency_count,
    )

    agency_id_to_datapoints_groupby = groupby(
        sorted(datapoints, key=lambda x: x["source_id"]),
        key=lambda x: x["source_id"],
    )
    agency_id_to_datapoints = {k: list(v) for k, v in agency_id_to_datapoints_groupby}

    for agency_id, datapoints in agency_id_to_datapoints.items():
        if agency_id not in agency_id_to_agency:
            continue

        data = summarize(datapoints=datapoints, today=today)
        agency_id_to_agency[agency_id] = dict(agency_id_to_agency[agency_id], **data)

    agencies = list(agency_id_to_agency.values())

    columns = [
        "name",
        "state",
        CREATED_AT,
        LAST_LOGIN,
        LOGIN_THIS_WEEK,
        LAST_UPDATE,
        NUM_RECORDS_WITH_DATA,
        NUM_METRICS_WITH_DATA,
        DATA_SHARED_THIS_WEEK,
        NUM_METRICS_CONFIGURED,
        NUM_METRICS_AVAILABLE,
        NUM_METRICS_UNAVAILABLE,
        METRIC_CONFIG_THIS_WEEK,
        IS_SUPERAGENCY,
        IS_CHILD_AGENCY,
        NEW_AGENCY_THIS_WEEK,
        NEW_STATE_THIS_WEEK,
        "systems",
    ]
    df = (
        pd.DataFrame.from_records(agencies)
        # Sort agencies alphabetically by name
        .sort_values("name").rename(columns={"state_code": "state"})
        # Put columns in desired order
        .reindex(columns=columns)
    )

    if dry_run is False:
        write_data_to_spreadsheet(
            google_credentials=google_credentials, df=df, columns=columns
        )


def write_data_to_spreadsheet(
    google_credentials: Credentials, df: pd.DataFrame, columns: List[str]
) -> None:
    """Now that we have retrieved and cleaned all agency data, write this data to a new
    sheet in the CSG Data Pull spreadsheet.
    """
    spreadsheet_service = build("sheets", "v4", credentials=google_credentials)

    now = datetime.datetime.now()
    new_sheet_title = f"{now.month}-{now.day}-{now.year}"
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
                    generate_agency_summary_csv(
                        session=global_session,
                        dry_run=args.dry_run,
                        google_credentials=credentials,
                    )
    else:
        # When running via Cloud Run Job, GoogleCredentials.get_application_default()
        # will use the service account assigned to the Cloud Run Job.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = GoogleCredentials.get_application_default()
        database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.JUSTICE_COUNTS,
        )
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        global_session = Session(bind=justice_counts_engine)
        generate_agency_summary_csv(
            session=global_session, dry_run=args.dry_run, google_credentials=credentials
        )
