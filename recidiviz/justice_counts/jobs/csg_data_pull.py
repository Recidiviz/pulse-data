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

uv run python -m recidiviz.justice_counts.jobs.csg_data_pull \
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
import sentry_sdk
from google.oauth2.service_account import Credentials

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.control_panel.utils import (
    append_row_to_spreadsheet,
    is_email_excluded,
    write_data_to_spreadsheet,
)
from recidiviz.justice_counts.jobs.pull_agencies_with_published_data import (
    calculate_columns_helper,
    get_reported_metrics_and_dashboard_helper,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import (
    METRIC_KEY_TO_METRIC,
    METRICS_BY_SYSTEM,
)
from recidiviz.justice_counts.utils.constants import (
    AGENCIES_TO_EXCLUDE,
    JUSTICE_COUNTS_SENTRY_DSN,
)
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

EXCLUDED_DOMAINS = ["@insomniacdesign.com", "@recidiviz.org", "@csg.org"]
# Spreadsheet Name: Justice Counts Data Pull
# https://docs.google.com/spreadsheets/d/1Vcz110SWJoTE345w3buPd8oYnwqu-Q_mIJ4C-9z_CC0/edit#gid=870547342
DATA_PULL_SPREADSHEET_ID = "1Vcz110SWJoTE345w3buPd8oYnwqu-Q_mIJ4C-9z_CC0"
SCOREBOARD_ALL_SHEET_ID = 659567415
SCOREBOARD_NON_CHILD_SHEET_ID = 855433685

CREATED_AT = "created_at"
LAST_VISIT = "last_visit"
VISIT_THIS_WEEK = "visit_this_week"
LAST_LOGIN = "last_login"
LOGIN_THIS_WEEK = "login_this_week"
LAST_UPDATE = "last_update"
INITIAL_METRIC_CONFIG_DATE = "initial_metric_config_date"
INITIAL_DATA_ENTRY_DATE = "initial_data_entry_date"
NUM_RECORDS_WITH_DATA = "num_records_with_data"
NUM_TOTAL_POSSIBLE_METRICS = "num_total_possible_metrics"
NUM_METRICS_WITH_DATA = "num_metrics_with_data"
NUM_TIME_PERIODS_REPORTED = "num_time_periods_reported"
NUM_METRICS_CONFIGURED = "num_metrics_configured"
METRIC_CONFIG_THIS_WEEK = "metric_configured_this_week"
NUM_METRICS_AVAILABLE = "num_metrics_available"
NUM_METRICS_UNAVAILABLE = "num_metrics_unavailable"
AGENCY_DASHBOARD_LINK = "agency_dashboard_link"
PUBLISHED_CAPACITY_AND_COSTS_METRICS = "published_capacity_and_costs_metrics"
IS_DASHBOARD_ENDABLED = "is_dashboard_enabled"
IS_SUPERAGENCY = "is_superagency"
IS_CHILD_AGENCY = "is_child_agency"
NEW_STATE_THIS_WEEK = "new_state_this_week"
NEW_AGENCY_THIS_WEEK = "new_agency_this_week"
DATA_SHARED_THIS_WEEK = "data_shared_this_week"
USED_BULK_UPLOAD = "used_bulk_upload"

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
    datapoints: List[schema.Datapoint],
    metric_settings: List[schema.MetricSetting],
    metric_setting_histories_last_week: List[schema.MetricSettingHistory],
    today: datetime.datetime,
    datapoint_id_to_first_update: Dict[int, datetime.datetime],
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
    a_long_time_ago = datetime.datetime(1970, 1, 1)
    metric_configured_this_week = False

    # We default both initial_metric_config_date and initial_data_entry_date to tomorrow
    # That way when we take the minimum of datapoint_last_update and initial_metric_config_date
    # and the minimum of datapoint_last_update and initial_data_entry_date later on,
    # we can account for when datapoint_last_update = datetime.datetime.today()
    # initial_metric_config_date is the date when configuration first occurs for any metric for a given agency
    tomorrow = datetime.datetime.today() + datetime.timedelta(days=1)
    initial_metric_config_date = tomorrow
    # initial_data_entry_date is the date when the agency first entered data for any metric
    initial_data_entry_date = tomorrow

    for datapoint in datapoints:
        if datapoint["is_report_datapoint"] is True and datapoint["value"] is None:
            # Ignore report datapoints with no values
            continue
        datapoint_last_update = datapoint["last_updated"]
        datapoint_first_update = datapoint_id_to_first_update.get(datapoint.id)

        # Part 1: Things that involve a timestamp
        if datapoint["is_report_datapoint"] is True:
            # initial_data_entry_date involves a timestamp and report datapoints
            if datapoint_first_update is not None:
                initial_data_entry_date = min(
                    datapoint_first_update, initial_data_entry_date
                )
            elif datapoint["created_at"] is not None:
                initial_data_entry_date = min(
                    initial_data_entry_date, datapoint["created_at"]
                )
            elif datapoint_last_update is not None:
                initial_data_entry_date = min(
                    initial_data_entry_date, datapoint_last_update
                )
        if datapoint_last_update is not None:
            # last_update involves a timestamp and report and agency datapoints
            last_update = max(last_update, datapoint_last_update)
            if datapoint["is_report_datapoint"] is True:
                # data_shared_this_week involves a timestamp and report datapoints
                data_shared_this_week = last_update.replace(
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

    for metric_setting in metric_settings:
        # Update last_update.

        if metric_setting.metric_definition_key not in METRIC_KEY_TO_METRIC:
            # Don't surface data about deprecated metrics
            continue

        last_update = max(
            last_update,
            (
                metric_setting.created_at
                if metric_setting.created_at is not None
                else a_long_time_ago
            ),
            (
                metric_setting.last_updated
                if metric_setting.last_updated is not None
                else a_long_time_ago
            ),
        )

        # Update initial_metric_config_date. We use both created_at and last_updated
        # since the last_updated column was introduced before created_at existed.
        initial_metric_config_date = min(
            initial_metric_config_date,
            (
                metric_setting.created_at
                if metric_setting.created_at is not None
                else tomorrow
            ),
            (
                metric_setting.last_updated
                if metric_setting.last_updated is not None
                else tomorrow
            ),
        )

        # Update metrics_configured, metrics_available, and metrics_unavailable.
        metric_interface = MetricInterface.from_storage_json(
            metric_setting.metric_interface
        )

        def is_supervision_subsystems_metric(metric_key: str) -> bool:
            """Returns true if the metric belongs to a supervision subsystem."""
            for subsystem in schema.System.supervision_subsystems():
                if metric_key.startswith(subsystem.value):
                    return True
            return False

        def is_supervision_metric(metric_key: str) -> bool:
            """Returns true if the metric is a supervision metric."""
            return metric_key.startswith("SUPERVISION")

        # Skip metrics if they are subsystems and the metric is not disaggregated by
        # subsystems. We don't want to count these metrics as
        # enabled/disabled/configured.
        if (
            is_supervision_subsystems_metric(metric_interface.key)
            and metric_interface.disaggregated_by_supervision_subsystems is not True
        ):
            continue

        # Skip metrics if they are supervision metrics and the metric is disaggregated
        # by subsystems. We don't want to count these metrics as
        # enabled/disabled/configured.
        if (
            metric_interface.disaggregated_by_supervision_subsystems is True
            and is_supervision_metric(metric_interface.key)
        ):
            continue

        # We consider a metric configured if it is either turned on or off. An
        # 'is_metric_enabled' value of None means that the metric is not yet configured.
        if metric_interface.is_metric_enabled is not None:
            metrics_configured.add(metric_interface.key)
        if metric_interface.is_metric_enabled is True:
            metrics_available.add(metric_interface.key)
        if metric_interface.is_metric_enabled is False:
            metrics_unavailable.add(metric_interface.key)

    for metric_setting_history_last_week in metric_setting_histories_last_week:
        # Filter out metric setting histories where enabled/disabled was not changed.
        metric_interface_updates = MetricInterface.from_storage_json(
            metric_setting_history_last_week.updates
        )
        if metric_interface_updates.is_metric_enabled is not None:
            metric_configured_this_week = True
            break

    return {
        LAST_UPDATE: (
            last_update.date() if last_update != datetime.datetime(1970, 1, 1) else ""
        ),
        NUM_RECORDS_WITH_DATA: len(report_id_to_datapoints),
        NUM_METRICS_WITH_DATA: len(metric_key_to_datapoints),
        NUM_METRICS_CONFIGURED: len(metrics_configured),
        METRIC_CONFIG_THIS_WEEK: metric_configured_this_week,
        NUM_METRICS_AVAILABLE: len(metrics_available),
        NUM_METRICS_UNAVAILABLE: len(metrics_unavailable),
        DATA_SHARED_THIS_WEEK: data_shared_this_week,
        INITIAL_METRIC_CONFIG_DATE: (
            initial_metric_config_date.date()
            if initial_metric_config_date != tomorrow
            else ""
        ),
        INITIAL_DATA_ENTRY_DATE: (
            initial_data_entry_date.date()
            if initial_data_entry_date != tomorrow
            else ""
        ),
    }


def get_all_data(
    session: Session,
) -> Tuple[
    Dict[str, schema.UserAccount],
    List[Any],
    List[Any],
    List[Any],
    List[Any],
    List[Any],
    List[Any],
    List[Any],
    Dict[int, datetime.datetime],
]:
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
    metric_settings = session.execute("select * from metric_setting").all()
    metric_setting_histories_last_week = session.execute(
        "select * from metric_setting_history where timestamp >= NOW() - INTERVAL '7 days'"
    ).all()
    spreadsheets = session.execute("select * from spreadsheet").all()
    datapoint_id_to_first_update = dict(
        session.execute(
            "select datapoint_history.datapoint_id, min(datapoint_history.timestamp) from datapoint_history where datapoint_history.old_value is null group by datapoint_history.datapoint_id"
        ).all()
    )

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
        metric_settings,
        metric_setting_histories_last_week,
        spreadsheets,
        datapoint_id_to_first_update,
    )


def build_system_to_num_metrics_dict() -> Dict[str, int]:
    """Helper function that builds a dictionary that maps a system to the number of
    possible metrics an agency with that system could have. The values for each of these
    systems were calculated based on the number of MetricFiles (that do not have
    disaggregations) for the given system.
    """
    system_to_num_metrics_dict: Dict[str, int] = defaultdict(int)

    for system, metrics in METRICS_BY_SYSTEM.items():
        system_to_num_metrics_dict[system] = len(metrics)
    return system_to_num_metrics_dict


def create_new_agency_columns(
    session: Session,
    agencies: List[schema.Agency],
    agency_id_to_users: Dict[str, List[schema.UserAccount]],
    today: datetime.datetime,
    spreadsheets: List[schema.Spreadsheet],
    environment: str,
    agency_id_to_last_visit: Dict[str, datetime.date],
) -> Tuple[List[schema.Agency], Dict[str, int]]:
    """Given a list of agencies and their users, create and populate the following columns:
    - last_visit
    - last_login
    - created_at
    - is_superagency
    - is_child_agency
    - new_agency_this_week
    - num_total_possible_metrics
    - used_bulk_upload
    - num_time_periods_reported
    - agency_dashboard_link
    - published_capacity_and_costs_metrics
    - is_dashboard_enabled
    """

    (
        agency_id_to_reported_metrics_with_data,
        agency_id_to_time_periods,
        agency_id_to_agency_with_dashboard_data,
    ) = get_reported_metrics_and_dashboard_helper(session=session)

    # A dictionary that maps agency_ids to a list of uploaded spreadsheets
    agency_id_to_spreadsheets: Dict[str, List[schema.Spreadsheet]] = defaultdict(list)
    for spreadsheet in spreadsheets:
        agency_id_to_spreadsheets[spreadsheet["agency_id"]] += spreadsheet

    system_to_num_metrics_dict = build_system_to_num_metrics_dict()

    # A dictionary that maps state codes to their count of new agencies: {"us_state": 2}
    state_code_by_new_agency_count: Dict[str, int] = defaultdict(int)
    for agency in agencies:
        agency_id = agency["id"]
        users = agency_id_to_users[agency_id]

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
            # Skip last_login for child agencies
            if agency["super_agency_id"] is not None or "last_login" not in user:
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

        num_total_possible_metrics = 0
        for system in agency["systems"]:
            num_total_possible_metrics += system_to_num_metrics_dict[system]

        used_bulk_upload = False
        if len(agency_id_to_spreadsheets.get(agency_id, [])) > 0:
            used_bulk_upload = True

        num_time_periods_reported = None
        agency_dashboard_link = None
        published_capacity_and_costs_metrics = None
        is_dashboard_enabled = None
        if agency_id_to_agency_with_dashboard_data.get(agency_id) is not None:
            (
                agency_dashboard_link,
                _,
                num_time_periods_reported,
            ) = calculate_columns_helper(
                agency_id_to_time_periods=agency_id_to_time_periods,
                agency_name=agency["name"],
                agency_id=agency_id,
                environment=environment,
            )
            published_capacity_and_costs_metrics = (
                len(agency_id_to_reported_metrics_with_data[agency_id]) > 0
            )
            is_dashboard_enabled = (
                agency["is_dashboard_enabled"]
                if agency["is_dashboard_enabled"] is True
                else False
            )

        last_visit = agency_id_to_last_visit.get(agency_id)

        agency[LAST_VISIT] = last_visit or ""
        agency[VISIT_THIS_WEEK] = (
            last_visit > (today + datetime.timedelta(days=-7)).date()
            if last_visit is not None
            else False
        )
        agency[LAST_LOGIN] = last_login or ""
        agency[LOGIN_THIS_WEEK] = (
            last_login > (today + datetime.timedelta(days=-7)).date()
            if last_login is not None
            else False
        )
        agency[CREATED_AT] = agency_created_at_str or first_user_created_at or ""
        agency[LAST_UPDATE] = ""
        agency[INITIAL_METRIC_CONFIG_DATE] = ""
        agency[INITIAL_DATA_ENTRY_DATE] = ""
        agency[NUM_RECORDS_WITH_DATA] = 0
        agency[NUM_TOTAL_POSSIBLE_METRICS] = num_total_possible_metrics
        agency[NUM_METRICS_WITH_DATA] = 0
        agency[DATA_SHARED_THIS_WEEK] = False
        agency[NUM_METRICS_CONFIGURED] = 0
        agency[NUM_METRICS_AVAILABLE] = 0
        agency[NUM_METRICS_UNAVAILABLE] = 0
        agency[METRIC_CONFIG_THIS_WEEK] = False
        agency[IS_SUPERAGENCY] = bool(agency["is_superagency"])
        agency[IS_CHILD_AGENCY] = bool(agency["super_agency_id"])
        agency[NEW_AGENCY_THIS_WEEK] = new_agency_this_week
        agency[USED_BULK_UPLOAD] = used_bulk_upload
        agency[NUM_TIME_PERIODS_REPORTED] = num_time_periods_reported or ""
        agency[AGENCY_DASHBOARD_LINK] = agency_dashboard_link or ""
        agency[PUBLISHED_CAPACITY_AND_COSTS_METRICS] = (
            published_capacity_and_costs_metrics
            if published_capacity_and_costs_metrics is not None
            else ""
        )
        agency[IS_DASHBOARD_ENDABLED] = (
            is_dashboard_enabled if is_dashboard_enabled is not None else ""
        )

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
    session: Session,
    dry_run: bool,
    google_credentials: Any,
    environment: str,
) -> None:
    """Generates a CSV with data about all agencies with Publisher accounts."""

    # First, pull user, agency, and datapoint data from auth0 and our database
    (
        auth0_user_id_to_user,
        agencies,
        agency_user_account_associations,
        users,
        datapoints,
        metric_settings,
        metric_setting_histories_last_week,
        spreadsheets,
        datapoint_id_to_first_update,
    ) = get_all_data(session=session)
    logger.info("Number of agencies: %s", len(agencies))

    users = [
        user
        for user in users
        if not is_email_excluded(
            user_email=user.email, excluded_domains=EXCLUDED_DOMAINS
        )
    ]
    user_id_to_auth0_user = {
        user["id"]: auth0_user_id_to_user.get(user["auth0_user_id"]) for user in users
    }

    agency_id_to_users = defaultdict(list)
    # A dictionary that maps agency ids to the date of the last user visit to that agency
    agency_id_to_last_visit: Dict[str, datetime.date] = defaultdict()
    for assoc in agency_user_account_associations:
        auth0_user = user_id_to_auth0_user.get(assoc["user_account_id"])
        if auth0_user is None:
            # Skip over CSG and Recidiviz users -- we shouldn't
            # count as a true login!
            continue
        agency_id_to_users[assoc["agency_id"]].append(auth0_user)

        if assoc["last_visit"] is not None:
            assoc_last_visit = assoc["last_visit"].date()
            agency_last_visit = agency_id_to_last_visit.get(assoc["agency_id"])
            if not agency_last_visit or (assoc_last_visit > agency_last_visit):
                agency_id_to_last_visit[assoc["agency_id"]] = assoc_last_visit

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
        session=session,
        agencies=agencies,
        agency_id_to_users=agency_id_to_users,
        today=today,
        spreadsheets=spreadsheets,
        environment=environment,
        agency_id_to_last_visit=agency_id_to_last_visit,
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

    agency_id_to_metric_settings_groupby = groupby(
        sorted(metric_settings, key=lambda x: x["agency_id"]),
        key=lambda x: x["agency_id"],
    )
    agency_id_to_metric_settings = {
        k: list(v) for k, v in agency_id_to_metric_settings_groupby
    }

    agency_id_to_metric_setting_histories_last_week_groupby = groupby(
        sorted(metric_setting_histories_last_week, key=lambda x: x["agency_id"]),
        key=lambda x: x["agency_id"],
    )
    agency_id_to_metric_setting_histories_last_week = {
        k: list(v) for k, v in agency_id_to_metric_setting_histories_last_week_groupby
    }

    for agency_id, datapoints in agency_id_to_datapoints.items():
        if agency_id not in agency_id_to_agency:
            continue

        data = summarize(
            datapoints=datapoints,
            metric_settings=agency_id_to_metric_settings.get(agency_id, []),
            metric_setting_histories_last_week=agency_id_to_metric_setting_histories_last_week.get(
                agency_id, []
            ),
            today=today,
            datapoint_id_to_first_update=datapoint_id_to_first_update,
        )
        agency_id_to_agency[agency_id] = dict(agency_id_to_agency[agency_id], **data)

    agencies = list(agency_id_to_agency.values())

    # Split out systems column into multiple columns
    agencies, system_columns = process_systems(agencies=agencies)

    columns = [
        "id",
        "name",
        "state",
        CREATED_AT,
        LAST_VISIT,
        VISIT_THIS_WEEK,
        LAST_LOGIN,
        LOGIN_THIS_WEEK,
        LAST_UPDATE,
        INITIAL_METRIC_CONFIG_DATE,
        INITIAL_DATA_ENTRY_DATE,
        NUM_RECORDS_WITH_DATA,
        NUM_TOTAL_POSSIBLE_METRICS,
        NUM_METRICS_WITH_DATA,
        NUM_TIME_PERIODS_REPORTED,
        DATA_SHARED_THIS_WEEK,
        AGENCY_DASHBOARD_LINK,
        PUBLISHED_CAPACITY_AND_COSTS_METRICS,
        IS_DASHBOARD_ENDABLED,
        NUM_METRICS_CONFIGURED,
        METRIC_CONFIG_THIS_WEEK,
        NUM_METRICS_AVAILABLE,
        NUM_METRICS_UNAVAILABLE,
        IS_SUPERAGENCY,
        IS_CHILD_AGENCY,
        NEW_AGENCY_THIS_WEEK,
        NEW_STATE_THIS_WEEK,
        USED_BULK_UPLOAD,
    ] + system_columns
    df = (
        pd.DataFrame.from_records(agencies)
        # Sort agencies alphabetically by name
        .sort_values("name").rename(columns={"state_code": "state"})
        # Put columns in desired order
        .reindex(columns=columns)
    )

    if dry_run is False:
        now = datetime.datetime.now()
        today_str = f"{now.month}-{now.day}-{now.year}"
        generate_excluded_agencies_sheet(
            google_credentials=google_credentials,
        )
        generate_and_write_scoreboard(
            df=df,
            updated=today_str,
            google_credentials=google_credentials,
        )
        # Index is 4 since we have Key, Excluded Agencies, and Scoreboard sheets
        write_data_to_spreadsheet(
            google_credentials=google_credentials,
            df=df,
            columns=columns,
            spreadsheet_id=DATA_PULL_SPREADSHEET_ID,
            new_sheet_title=today_str,
            logger=logger,
            index=4,
        )


def generate_excluded_agencies_sheet(google_credentials: Credentials) -> None:
    """
    Generates and writes a spreadsheet containing agencies excluded from the CSG Data Pull.
    """
    df = pd.DataFrame(AGENCIES_TO_EXCLUDE.items(), columns=["agency_id", "agency_name"])
    write_data_to_spreadsheet(
        df=df,
        columns=["Agency ID", "Agency Name"],
        google_credentials=google_credentials,
        spreadsheet_id=DATA_PULL_SPREADSHEET_ID,
        index=1,
        logger=logger,
        new_sheet_title="Excluded Agencies",
        overwrite_sheets=True,
    )


def generate_and_write_scoreboard(
    df: pd.DataFrame,
    updated: str,
    google_credentials: Credentials,
) -> None:
    """Generate some summary statistics related to Publisher usage and write these
    statistics (appended as a new row) to the following sheets in the Justice Counts
    Data Pull: Scoreboard (ALL) and Scoreboard (NON-CHILD).

    Columns generated include the following:
        - date_last_updated
        - num_total_agencies
        - num_agencies_created_in_last_week
        - num_agencies_visited_last_week
        - num_agencies_logged_in_last_week
        - num_super_agencies
        - num_agencies_shared_data_at_least_one_metric
        - num_agencies_at_least_one_metric_configured
        - num_agencies_data_shared_this_week
        - num_agencies_used_bulk_upload
    """
    scoreboard_sheets = ["Scoreboard (ALL)", "Scoreboard (NON-CHILD)"]

    for sheet_title in scoreboard_sheets:
        if sheet_title == "Scoreboard (ALL)":
            sheet_id = SCOREBOARD_ALL_SHEET_ID
            sheet_df = df
        else:
            sheet_id = SCOREBOARD_NON_CHILD_SHEET_ID
            sheet_df = df[~df["is_child_agency"]]

        num_total_agencies = str(len(sheet_df))
        num_agencies_created_in_last_week = str(
            len(sheet_df[sheet_df["new_agency_this_week"]])
        )
        num_agencies_visited_last_week = str(len(sheet_df[sheet_df["visit_this_week"]]))
        num_agencies_logged_in_last_week = str(
            len(sheet_df[sheet_df["login_this_week"]])
        )
        num_super_agencies = str(len(sheet_df[sheet_df["is_superagency"]]))
        num_agencies_shared_data_at_least_one_metric = str(
            len(sheet_df[sheet_df["num_metrics_with_data"] > 0])
        )
        num_agencies_at_least_one_metric_configured = str(
            len(sheet_df[sheet_df["num_metrics_configured"] > 0])
        )
        num_agencies_data_shared_this_week = str(
            len(sheet_df[sheet_df["data_shared_this_week"]])
        )
        num_agencies_used_bulk_upload = str(len(sheet_df[sheet_df["used_bulk_upload"]]))

        # data_to_write is a list containing lists that represent new rows to append to the
        # existing Justice Counts Scoreboard spreadsheet
        # The order of these rows matters and should coincide with the columns listed above!
        data_to_write = [
            [
                updated,
                num_total_agencies,
                num_agencies_created_in_last_week,
                num_agencies_visited_last_week,
                num_agencies_logged_in_last_week,
                num_super_agencies,
                num_agencies_shared_data_at_least_one_metric,
                num_agencies_at_least_one_metric_configured,
                num_agencies_data_shared_this_week,
                num_agencies_used_bulk_upload,
            ]
        ]
        append_row_to_spreadsheet(
            google_credentials=google_credentials,
            data_to_write=data_to_write,
            spreadsheet_id=DATA_PULL_SPREADSHEET_ID,
            sheet_title=sheet_title,
            logger=logger,
            sort_by="date_last_updated",
            sheet_id=sheet_id,
        )


def process_systems(
    agencies: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Given a list of agencies (represented as dictionaries), split the 'systems'
    column into multiple columns. This will result in max_num_systems new columns,
    which is the number of systems that the agency with the most systems has. The new
    columns will be labeled 'system_1', 'system_2', ...., 'system_n', where n is
    max_num_systems"""
    agencies_df = pd.DataFrame(agencies)
    max_num_systems = pd.DataFrame(agencies_df["systems"].to_list()).shape[1]
    system_cols = []
    for n in range(1, max_num_systems + 1):
        system_cols.append(f"system_{n}")

    # Split 'systems' column into multiple columns
    agencies_df[system_cols] = pd.DataFrame(
        pd.DataFrame(agencies_df["systems"].to_list())
    ).fillna("")
    agencies_df = agencies_df.drop(labels="systems", axis=1)
    agencies = agencies_df.to_dict("records")
    return agencies, system_cols


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )
    args = create_parser().parse_args()
    if args.project_id is not None:
        # When running locally, point to JSON file with service account credentials.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = Credentials.from_service_account_file(args.credentials_path)
        environment_str = (
            "PRODUCTION"
            if args.project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
            else "STAGING"
        )
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
                        environment=environment_str,
                    )
