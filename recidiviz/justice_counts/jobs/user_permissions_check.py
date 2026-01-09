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
"""Job that posts to Slack with data about users with incorrect permissions.
Called from the production data_pull_job script that runs weekly.

uv run python -m recidiviz.justice_counts.jobs.user_permissions_check \
  --project-id=justice-counts-local
"""
import argparse
import dataclasses
import logging
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import requests
import sentry_sdk
from sqlalchemy.orm import Session

from recidiviz.justice_counts.control_panel.utils import (
    is_demo_agency,
    is_email_excluded,
)
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
    PROJECT_JUSTICE_COUNTS_LOCAL,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.secrets import get_secret

SCHEMA_TYPE = schema_type = SchemaType.JUSTICE_COUNTS
EXCLUDED_DOMAINS = ["@insomniacdesign.com"]
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UserInfo:
    """Holds information about a JC user"""

    name: str
    email: str
    states: Set[str]
    agency_names: List[str]
    superagencies: List[schema.Agency]
    child_agencies: List[schema.Agency]
    individual_agencies: List[schema.Agency]


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[
            PROJECT_JUSTICE_COUNTS_LOCAL,
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    return parser


def find_users_with_missing_child_agencies(
    users: List[UserInfo], child_agency_id_to_agency: Dict[int, schema.Agency]
) -> List[List[Any]]:
    """Return information about users who have access to a superagency
    but not all of its child agencies.

    Arguments:
    users -- List of UserInfo objects
    child_agency_id_to_agency -- Dictionary mapping child agency IDs to agencies

    Returns:
    List of lists. Each sublist has the format of
    [user_name: str, superagency_name: str, missing_child_agency_names: List[str]]
    """
    super_agency_id_to_child_agency_ids = defaultdict(list)
    for _, child_agency in child_agency_id_to_agency.items():
        super_agency_id_to_child_agency_ids[child_agency.super_agency_id].append(
            child_agency.id
        )

    user_infos = []
    for user in users:
        user_child_agency_ids = {
            child_agency.id for child_agency in user.child_agencies
        }
        missing_child_agency_names = []
        for super_agency in user.superagencies:
            child_agency_ids = set(super_agency_id_to_child_agency_ids[super_agency.id])
            missing_child_agency_ids = child_agency_ids - user_child_agency_ids
            missing_child_agency_names.extend(
                [
                    child_agency_id_to_agency[_id].name
                    for _id in missing_child_agency_ids
                ]
            )
            if missing_child_agency_names:
                user_infos.append(
                    [
                        user.name,
                        super_agency.name,
                        missing_child_agency_names,
                    ]
                )
    return user_infos


def find_users_with_multiple_agencies(
    users: List[UserInfo],
) -> Tuple[List[Any], List[Any], List[Any]]:
    """Return information about users who have:
    * Access to multiple agencies but no superagency
    * Access to > 10 agencies
    * Access to agencies in multiple states

    Arguments:
    users -- List of UserInfo objects

    Returns:
    Tuple of three lists of lists:
    -- users_with_multiple_agencies_no_super
       Each sublist has the format: [user_name: str, agency_names: List[str]]
    -- users_with_too_many_agencies
       Each sublist has the format: [user_name: str, agency_names: List[str]]
    -- users_with_multiple_states
       Each sublist has the format: [user_name: str, state_names: List[str], agency_names: List[str]]
    """
    users_with_multiple_agencies_no_super = []
    users_with_too_many_agencies = []
    users_with_multiple_states = []
    for user in users:
        user_info = [user.name, user.agency_names]
        if len(user.agency_names) > 1 and not user.superagencies:
            users_with_multiple_agencies_no_super.append(user_info)
        if (len(user.superagencies) + len(user.individual_agencies)) > 10:
            users_with_too_many_agencies.append(user_info)
        if len(user.states) > 1:
            users_with_multiple_states.append(
                [user.name, list(user.states), user.agency_names]
            )
    return (
        users_with_multiple_agencies_no_super,
        users_with_too_many_agencies,
        users_with_multiple_states,
    )


def find_csg_users_with_wrong_role(
    session: Session,
) -> List[List[Any]]:
    """Return information about CSG users who have a non-READ_ONLY role
    for non-demo agencies in Production.

    Returns:
    List of lists. Each sublist has the format of
    [user_name: str, agency_names: List[str], role: str]
    """
    user_infos = []
    users = UserAccountInterface.get_csg_users(session=session)
    for user in users:
        if "@csg.org" not in user.email:
            continue
        role_to_agency_names = defaultdict(list)
        for assoc in user.agency_assocs:
            if is_demo_agency(assoc.agency.name):
                continue
            role_to_agency_names[
                assoc.role.value if assoc.role is not None else "NO ROLE"
            ].append(assoc.agency.name)
        for role, agency_names in role_to_agency_names.items():
            if role == schema.UserAccountRole.READ_ONLY.value:
                continue
            user_infos.append(
                [
                    user.name,
                    agency_names,
                    role,
                ]
            )
    return user_infos


def construct_user_infos(
    session: Session,
) -> Tuple[List[UserInfo], Dict[int, schema.Agency]]:
    """Construct and returns list of UserInfo objects about all
    non-CSG and non-Recidiviz users. Also return information about
    all child agencies to which these users belong.

    Returns:
    -- user_infos: List of UserInfo objects
    -- child_agency_id_to_agency: Dictionary mapping child agency IDs to agencies
    """
    non_csg_users = UserAccountInterface.get_non_csg_and_recidiviz_users(
        session=session
    )
    user_infos = []
    child_agency_id_to_agency = {}
    for user in non_csg_users:
        if is_email_excluded(user_email=user.email, excluded_domains=EXCLUDED_DOMAINS):
            continue
        states = set()
        agency_names = []
        superagencies = []
        child_agencies = []
        individual_agencies = []
        for agency_assoc in user.agency_assocs:
            agency_names.append(agency_assoc.agency.name)
            if is_demo_agency(agency_assoc.agency.name):
                continue
            states.add(agency_assoc.agency.state_code)
            if agency_assoc.agency.is_superagency is True:
                superagencies.append(agency_assoc.agency)
            elif agency_assoc.agency.super_agency_id is not None:
                child_agencies.append(agency_assoc.agency)
                if agency_assoc.agency.id not in child_agency_id_to_agency:
                    child_agency_id_to_agency[
                        agency_assoc.agency.id
                    ] = agency_assoc.agency
            else:
                individual_agencies.append(agency_assoc.agency)
        user_infos.append(
            UserInfo(
                name=user.name,
                email=user.email,
                states=states,
                agency_names=agency_names,
                superagencies=superagencies,
                child_agencies=child_agencies,
                individual_agencies=individual_agencies,
            )
        )
    return user_infos, child_agency_id_to_agency


def check_user_permissions(
    session: Session,
    project_id: str,
) -> None:
    """Identify users with potentially incorrect access levels
    and send a message to Slack.
    """
    user_infos, child_agency_id_to_agency = construct_user_infos(session=session)
    csg_users_with_wrong_role = find_csg_users_with_wrong_role(session=session)
    users_with_missing_child_agencies = find_users_with_missing_child_agencies(
        users=user_infos,
        child_agency_id_to_agency=child_agency_id_to_agency,
    )
    (
        users_with_multiple_agencies_no_super,
        users_with_too_many_agencies,
        users_with_multiple_states,
    ) = find_users_with_multiple_agencies(users=user_infos)

    users_with_no_agencies = [
        (user.name, user.email) for user in user_infos if not user.agency_names
    ]

    def format_role(role: str) -> str:
        # Convert e.g. AGENCY_ADMIN to Agency Admin
        return role.replace("_", " ").title()

    def add_users(text: str, user_infos: List[List[Any]]) -> str:
        # Add information about these users to the text string
        for name, agencies in user_infos:
            text += f"* {name}: {len(agencies)} Agencies\n"
        text += "\n"
        return text

    text = ""
    if csg_users_with_wrong_role:
        text += "The following CSG users have non-read-only access to a production agency: \n"
        for name, agencies, role in csg_users_with_wrong_role:
            text += f"* {name}: [Role] {format_role(role)}, {len(agencies)} Agencies\n"
        text += "\n"

    if users_with_missing_child_agencies:
        text += "The following users have access to a superagency but not all of its child agencies: \n"
        for (
            name,
            super_agency,
            _,
        ) in users_with_missing_child_agencies:
            text += f"{name}: [Superagency] {super_agency}\n"

    if users_with_multiple_agencies_no_super:
        text += "The following users have access to multiple agencies but no superagency: \n"
        text = add_users(text=text, user_infos=users_with_multiple_agencies_no_super)

    if users_with_too_many_agencies:
        text += "The following users have access to > 10 agencies: \n"
        text = add_users(text=text, user_infos=users_with_too_many_agencies)

    if users_with_multiple_states:
        text += "The following users have access to agencies in multiple states: \n"
        for name, states, agencies in users_with_multiple_states:
            text += (
                f"* {name}: [States] {', '.join(states)}, {len(agencies)} Agencies\n"
            )
        text += "\n"

    if users_with_no_agencies:
        text += "The following users don't have access to any agencies: \n"
        for name, email in users_with_no_agencies:
            text += f"* {name}, {email}\n"

    if not text:
        logging.info(
            "No output of user permissions check; not sending a Slack message."
        )
        return

    if project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION:
        logging.info("Running in production; sending a Slack message to #jc-tech-chat")
        slack_channel_id = "C04JL8A2ADB"
    else:
        logging.info(
            "Running in local/staging; sending a Slack message to #jc-eng-only"
        )
        slack_channel_id = "C05C9CND8N7"

    slack_token = get_secret("deploy_slack_bot_authorization_token")
    if not slack_token:
        logging.error("Couldn't find `deploy_slack_bot_authorization_token`")
        return

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            json={"text": text, "channel": slack_channel_id},
            headers={"Authorization": "Bearer " + slack_token},
            timeout=60,
        )
        logging.info("Response from Slack API call: %s", response)
    except Exception as e:
        logging.exception("Error when calling Slack API: %s", str(e))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
    )

    args = create_parser().parse_args()
    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    if args.project_id == PROJECT_JUSTICE_COUNTS_LOCAL:
        # If running locally, we actually want to point to production DB
        with local_project_id_override(GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION):
            with cloudsql_proxy_control.connection(
                schema_type=schema_type,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            ):
                with SessionFactory.for_proxy(
                    database_key=database_key,
                    secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                    autocommit=False,
                ) as global_session:
                    check_user_permissions(
                        session=global_session,
                        project_id=args.project_id,
                    )
    else:
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        global_session = Session(bind=justice_counts_engine)
        check_user_permissions(
            session=global_session,
            project_id=args.project_id,
        )
