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

"""Generates a CSV with data about all agencies with Publisher accounts.
Includes fields like "num_record_with_data", "num_metrics_configured", etc.

python -m recidiviz.tools.justice_counts.generate_agency_summary_csv \
  --project-id=PROJECT_ID \
"""

import argparse
import datetime
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List

import pandas as pd

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CREATED_AT = "created_at"
LAST_LOGIN = "last_login"
LAST_UPDATE = "last_update"
NUM_RECORDS_WITH_DATA = "num_records_with_data"
NUM_METRICS_WITH_DATA = "num_metrics_with_data"
NUM_METRICS_CONFIGURED = "num_metrics_configured"
NUM_METRICS_AVAILABLE = "num_metrics_available"
NUM_METRICS_UNAVAILABLE = "num_metrics_unavailable"
IS_ALPHA_PARTNER = "is_alpha_partner"
IS_SUPERAGENCY = "is_superagency"

# To add:
# NUM_METRICS_UNCONFIGURED = "num_metrics_unconfigured"
# NUM_METRICS_DEFINED = "num_metrics_defined"

# These are the IDs of our alpha partner agencies in production
ALPHA_PARTNER_IDS = {74, 75, 73, 77, 79, 80, 81, 83, 84, 85}


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )

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
        # For now, an agency's last_update is the max(dp["created_at"])
        # over all of its datapoints. That means if a user edits an existing datapoint,
        # it won't count towards last_update.
        # TODO(#22370) Add updated_at to schema.Datapoint and use that instead
        if datapoint["created_at"]:
            datapoint_created_at = datapoint["created_at"].date()
            if not last_update or (last_update and datapoint_created_at > last_update):
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


def generate_agency_summary_csv() -> None:
    """Generates a CSV with data about all agencies with Publisher accounts."""
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)

    auth0_client = Auth0Client(  # nosec
        domain_secret_name="justice_counts_auth0_api_domain",
        client_id_secret_name="justice_counts_auth0_api_client_id",
        client_secret_secret_name="justice_counts_auth0_api_client_secret",
    )
    auth0_users = auth0_client.get_all_users()
    auth0_user_id_to_user = {user["user_id"]: user for user in auth0_users}

    with cloudsql_proxy_control.connection(
        schema_type=schema_type, secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX
    ):
        with SessionFactory.for_proxy(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        ) as session:
            agencies = session.execute(
                "select * from source where type = 'agency'"
            ).all()
            agency_user_account_associations = session.execute(
                "select * from agency_user_account_association"
            ).all()
            users = session.execute("select * from user_account").all()
            datapoints = session.execute("select * from datapoint").all()

            user_id_to_auth0_user = {
                user["id"]: auth0_user_id_to_user.get(user["auth0_user_id"])
                for user in users
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

            # We have some test agencies in production, currently distinguished only in name
            test_agency_ids = {
                a["id"] for a in agencies if "TEST" in a["name"] or "Test" in a["name"]
            }
            # Skip child agencies, since a superagency might have hundreds of them
            child_agency_ids = {a["id"] for a in agencies if a["super_agency_id"]}

            original_agencies = agencies
            agencies = [
                dict(a)
                for a in original_agencies
                if a["id"] not in (test_agency_ids | child_agency_ids)
            ]
            agency_id_to_agency = {a["id"]: a for a in agencies}

            print(f"Number of agencies: {len(agencies)}")

            for agency in agencies:
                users = agency_id_to_users[agency["id"]]
                agency_created_at = agency["created_at"]
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
                agency[IS_ALPHA_PARTNER] = agency["id"] in ALPHA_PARTNER_IDS
                agency[IS_SUPERAGENCY] = bool(agency["is_superagency"])

            agency_id_to_datapoints_groupby = groupby(
                sorted(datapoints, key=lambda x: x["source_id"]),
                key=lambda x: x["source_id"],
            )
            agency_id_to_datapoints = {
                k: list(v) for k, v in agency_id_to_datapoints_groupby
            }

            for agency_id, datapoints in agency_id_to_datapoints.items():
                if agency_id not in agency_id_to_agency:
                    continue

                data = summarize(datapoints=datapoints)
                agency_id_to_agency[agency_id] = dict(
                    agency_id_to_agency[agency_id], **data
                )

            agencies = list(agency_id_to_agency.values())

            df = (
                pd.DataFrame.from_records(agencies)
                # Sort agencies alphabetically by name
                .set_index("name")
                .sort_index()
                .rename(columns={"state_code": "state"})
                # Put columns in desired order
                .reindex(
                    columns=[
                        "state",
                        CREATED_AT,
                        LAST_LOGIN,
                        LAST_UPDATE,
                        NUM_RECORDS_WITH_DATA,
                        NUM_METRICS_WITH_DATA,
                        NUM_METRICS_CONFIGURED,
                        NUM_METRICS_AVAILABLE,
                        NUM_METRICS_UNAVAILABLE,
                        IS_ALPHA_PARTNER,
                        IS_SUPERAGENCY,
                        "systems",
                    ]
                )
            )

            df.to_csv("justice_counts.csv")


if __name__ == "__main__":
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        generate_agency_summary_csv()
