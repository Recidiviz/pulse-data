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
Copies all metric settings from a super agency to its child agencies.
If child agencies have existing settings that conflict with the super agency,
they will be overwritten. If the child agency has existing settings that do not
conflict with the super agency, then they will not be affected.

The following are a list of metric definition keys grouped by system. 
These can be passed into the metric_definition_key_subset flag to 
update a subset of metrics.

COURTS: 
- COURTS_AND_PRETRIAL_FUNDING
- COURTS_AND_PRETRIAL_EXPENSES
- COURTS_AND_PRETRIAL_TOTAL_STAFF
- COURTS_AND_PRETRIAL_PRETRIAL_RELEASES
- COURTS_AND_PRETRIAL_CASES_FILED
- COURTS_AND_PRETRIAL_CASES_DISPOSED
- COURTS_AND_PRETRIAL_SENTENCES
- COURTS_AND_PRETRIAL_ARRESTS_ON_PRETRIAL_RELEASE

DEFENSE:
- DEFENSE_FUNDING
- DEFENSE_EXPENSES
- DEFENSE_TOTAL_STAFF
- DEFENSE_CASELOADS_PEOPLE
- DEFENSE_CASELOADS_STAFF
- DEFENSE_CASES_APPOINTED_COUNSEL
- DEFENSE_CASES_DISPOSED
- DEFENSE_COMPLAINTS_SUSTAINED

JAILS:
- JAILS_FUNDING
- JAILS_EXPENSES
- JAILS_TOTAL_STAFF
- JAILS_PRE_ADJUDICATION_ADMISSIONS
- JAILS_POST_ADJUDICATION_ADMISSIONS
- JAILS_PRE_ADJUDICATION_POPULATION
- JAILS_POST_ADJUDICATION_POPULATION
- JAILS_PRE_ADJUDICATION_RELEASES
- JAILS_POST_ADJUDICATION_RELEASES
- JAILS_READMISSIONS
- JAILS_USE_OF_FORCE_INCIDENTS 
- JAILS_GRIEVANCES_UPHELD

LAW_ENFORCEMENT:
- LAW_ENFORCEMENT_FUNDING
- LAW_ENFORCEMENT_EXPENSES
- LAW_ENFORCEMENT_TOTAL_STAFF
- LAW_ENFORCEMENT_CALLS_FOR_SERVICE
- LAW_ENFORCEMENT_ARRESTS
- LAW_ENFORCEMENT_REPORTED_CRIME
- LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS
- LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED

PRISONS:
- PRISONS_FUNDING
- PRISONS_EXPENSES
- PRISONS_TOTAL_STAFF
- PRISONS_ADMISSIONS
- PRISONS_POPULATION
- PRISONS_RELEASES
- PRISONS_READMISSIONS
- PRISONS_USE_OF_FORCE_INCIDENTS
- PRISONS_GRIEVANCES_UPHELD

PROSECUTION: 
- PROSECUTION_FUNDING
- PROSECUTION_EXPENSES
- PROSECUTION_TOTAL_STAFF
- PROSECUTION_CASELOADS_PEOPLE
- PROSECUTION_CASELOADS_STAFF
- PROSECUTION_CASES REFERRED
- PROSECUTION_CASES_DECLINED
- PROSECUTION_CASES_DIVERTED
- PROSECUTION_CASES_PROSECUTED
- PROSECUTION_CASES_DISPOSED
- PROSECUTION_VIOLATIONS_WITH_DISCIPLINARY_ACTION

SUPERVISION: 
- SUPERVISION_FUNDING
- SUPERVISION_EXPENSES
- SUPERVISION_TOTAL_STAFF
- SUPERVISION_CASELOADS_PEOPLE
- SUPERVISION_CASELOADS_STAFF
- SUPERVISION_SUPERVISION_STARTS
- SUPERVISION_POPULATION
- SUPERVISION_SUPERVISION_TERMINATIONS
- SUPERVISION_SUPERVISION_VIOLATIONS
- SUPERVISION_REVOCATIONS
- SUPERVISION_RECONVICTIONS

Note: If you need to copy over metrics for a supervision subsystem, replace the SUPERVISION 
in the metric name with the name of the subsystem (i.e SUPERVISION_FUNDING -> PAROLE_FUNDING).

python -m recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies \
  --project_id=<justice-counts-staging OR justice-counts-production> \
  --super_agency_id=<agency_id> \
  --dry-run=<true Or false>
  --metric_definition_key_subset=ALL
"""

import argparse
import logging
from collections import defaultdict
from typing import List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.utils.constants import UNSUBSCRIBE_GROUP_ID
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
from recidiviz.utils.params import str_to_bool, str_to_list
from recidiviz.utils.sendgrid_client_wrapper import SendGridClientWrapper

logger = logging.getLogger(__name__)


def create_script_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--super_agency_id",
        type=int,
        help="The id of the super_agency you want to copy agency settings from.",
        required=True,
    )

    parser.add_argument(
        "--project_id",
        dest="project_id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        type=str,
    )
    parser.add_argument(
        "--agency_name",
        type=str,
        help="The name of the superagency.",
        required=True,
    )
    parser.add_argument(
        "--user_email",
        type=str,
        help="The email address of the user to send a job completion message to.",
        required=True,
    )
    parser.add_argument(
        "--metric_definition_key_subset",
        type=str_to_list,
        help="List of metrics definition keys that should be copied over.",
        required=True,
    )
    parser.add_argument(
        "--child_agency_id_subset",
        type=str_to_list,
        help="List of child agency IDs that should get metrics copied to.",
        required=False,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def copy_metric_settings(
    super_agency_id: int,
    dry_run: bool,
    agency_name: str,
    metric_definition_key_subset: List[str],
    current_session: Session,
    child_agency_id_subset: Optional[List[int]] = None,
) -> str:
    """Copies all (or a subset of) metric settings from a super agency to
    all (or a subset of) its child agencies.
    """

    if dry_run is True:
        logger.info("DRY RUN! THE FOLLOWING CHANGES WILL NOT BE COMMITTED.")

    metric_definition_key_set = set(metric_definition_key_subset)
    child_agency_id_set = (
        set(child_agency_id_subset) if child_agency_id_subset else None
    )

    super_agency = AgencyInterface.get_agency_by_id(
        session=current_session, agency_id=super_agency_id
    )

    child_agencies = AgencyInterface.get_child_agencies_by_agency_ids(
        session=current_session, agency_ids=[super_agency_id]
    )
    super_agency_metric_settings = MetricSettingInterface.get_agency_metric_interfaces(
        session=current_session,
        agency=super_agency,
    )

    copied_metric_system_and_display_name = set()
    for child_agency in child_agencies:
        logger.info("Child Agency: %s", child_agency.name)

        if child_agency_id_set is not None:
            if child_agency.id not in child_agency_id_set:
                logger.info(
                    "Skipping Child Agency because it is not in subset: %s",
                    child_agency.name,
                )
                continue

        for metric_setting in super_agency_metric_settings:
            if metric_setting.metric_definition.system == schema.System.SUPERAGENCY:
                # Don't copy over superagency metrics to child agencies.
                continue

            if (
                "ALL" not in metric_definition_key_set
                and metric_setting.metric_definition.key
                not in metric_definition_key_set
            ):
                logger.info(
                    "Skipping metric because it is not in subset: %s",
                    metric_setting.key,
                )
                continue

            if metric_setting.metric_definition.key not in METRIC_KEY_TO_METRIC:
                logger.info(
                    "Metric deprecated: %s, skipping",
                    metric_setting.metric_definition.key,
                )
                continue

            logger.info("Metric %s is being updated", metric_setting.key)
            MetricSettingInterface.add_or_update_agency_metric_setting(
                session=current_session,
                agency=child_agency,
                agency_metric_updates=metric_setting,
            )
            copied_metric_system_and_display_name.add(
                (
                    metric_setting.metric_definition.system.value,
                    metric_setting.metric_definition.display_name,
                )
            )
    if dry_run is False:
        current_session.commit()

    return _get_email_content_from_metrics(
        is_copying_subset=len(metric_definition_key_set)
        < len(super_agency_metric_settings),
        child_agencies=child_agencies,
        child_agency_id_set=child_agency_id_set,
        copied_metric_system_and_display_name=copied_metric_system_and_display_name,
        super_agency_name=agency_name,
        super_agency_id=super_agency_id,
    )


def _get_email_content_from_metrics(
    is_copying_subset: bool,
    super_agency_name: str,
    super_agency_id: int,
    child_agencies: List[schema.Agency],
    copied_metric_system_and_display_name: Set[Tuple[str, str]],
    child_agency_id_set: Optional[Set[int]] = None,
) -> str:
    """
    Generate HTML email content based on metrics settings.

    Args:
        is_copying_subset (bool): Indicates whether we are copying all metric settings or a subset of metric settings.
        child_agencies (List[schema.Agency]): List of child agencies.
        metric_definition_key_set (Set[str]): Set of metric definition keys.
        child_agency_id_set (Optional[Set[int]], optional): Set of child agency IDs. Defaults to None.

    Returns:
        str: HTML content for the email.
    """

    if (
        is_copying_subset is False
        and child_agency_id_set is not None
        and len(child_agency_id_set) == len(child_agencies)
    ):
        return f"""<p>Success! All of the metric settings from {super_agency_name} (ID: {super_agency_id}) have been copied over to all of its child agencies.</p>"""

    # If a subset of metrics or child agencies are affected, the email will be in the following format.

    # Success! Your request to copy the following metrics from the ____ superagency to ____, ___, ___, was successful.

    # From the ____ sector:
    # A
    # B
    # C

    # From the ____ sector:
    # A
    # B
    # C

    html_content = f"<p> Success! Your request to copy the following metrics from the {super_agency_name} superagency to "
    if child_agency_id_set is not None and len(child_agency_id_set) < len(
        child_agencies
    ):
        combined_names = ", ".join(
            [
                child_agency.name
                for child_agency in child_agencies
                if child_agency.id in child_agency_id_set
            ]
        )
        html_content += combined_names + " was successful.</p>"
    else:
        html_content += "all of its child agencies was successful.</p>"

    system_to_metric_display_names = defaultdict(list)

    for system_str, metric_display_name in copied_metric_system_and_display_name:
        system_to_metric_display_names[system_str.replace("_", " ").title()].append(
            metric_display_name
        )

    for system, metric_display_names in system_to_metric_display_names.items():
        if len(system_to_metric_display_names) > 1:
            html_content += f"From the {system} sector: <br>"
        html_content += "<ul>"
        for metric_display_name in metric_display_names:
            html_content += f"<li>{metric_display_name}</li>"
        html_content += "</ul>"

    return html_content


def copy_metric_settings_and_alert_user(
    super_agency_id: int,
    dry_run: bool,
    agency_name: str,
    metric_definition_key_subset: List[str],
    current_session: Session,
    user_email: str,
    send_grid_client: SendGridClientWrapper,
    child_agency_id_subset: Optional[List[int]] = None,
) -> None:
    """
    Copy metric settings from a superagency to its child agencies and notify the user via email about the result.

    Args:
        super_agency_id (int): The ID of the super agency whose metric settings will be copied.
        dry_run (bool): If True, perform a dry run without actual copying. If False, copy the metric settings.
        agency_name (str): The name of the super agency.
        metric_definition_key_subset (List[str]): A subset of metric definitions to copy.
        current_session (Session): The current database session.
        send_grid_client (SendGridClientWrapper): A wrapper for SendGrid to send email notifications.
        child_agency_id_subset (Optional[List[int]]): A list of child agency IDs to copy to. If None, all child agencies are considered.

    Raises:
        Exception: If an error occurs during the copying process.

    Returns:
        None: This function does not return a value, but sends email notifications based on the outcome.

    Operation:
        - Attempts to copy metric settings from the specified super agency to its child agencies using the provided session.
        - Sends an email notification to the user upon successful copying, using the `success_html_content` received from the `copy_metric_settings` function.
        - If copying fails, sends an email to the user notifying them of the failure, with instructions to reach out for assistance.
    """
    try:
        success_html_content = copy_metric_settings(
            dry_run=dry_run,
            super_agency_id=super_agency_id,
            metric_definition_key_subset=metric_definition_key_subset,
            child_agency_id_subset=child_agency_id_subset,
            current_session=current_session,
            agency_name=agency_name,
        )
    except Exception as e:
        logging.exception("Failed to copy metric settings: %s", e)
        send_grid_client.send_message(
            to_email=user_email,
            from_email="no-reply@justice-counts.org",
            from_email_name="Justice Counts",
            subject=f"Unfortunately, your request to copy {agency_name}'s (ID: {super_agency_id}) metric settings to its child agencies could not be completed.",
            html_content=f"""<p>Sorry, something went wrong while attempting to copy the metric settings from {agency_name} (ID: {super_agency_id}). Please reach out to a member of the Recidiviz team to help troubleshoot.</p>""",
            disable_link_click=True,
            unsubscribe_group_id=UNSUBSCRIBE_GROUP_ID,
        )
    else:
        # Send confirmation email once metrics have been successfully copied over
        send_grid_client.send_message(
            to_email=user_email,
            from_email="no-reply@justice-counts.org",
            from_email_name="Justice Counts",
            subject=f"Your request to copy {agency_name}'s (ID: {super_agency_id}) metric settings to its child agencies has been completed.",
            html_content=success_html_content,
            disable_link_click=True,
            unsubscribe_group_id=UNSUBSCRIBE_GROUP_ID,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_script_parser().parse_args()
    _child_agency_id_subset = (
        list(map(int, args.child_agency_id_subset))
        if args.child_agency_id_subset
        else []
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
            ) as session:
                email_client = SendGridClientWrapper(key_type="justice_counts")
                copy_metric_settings_and_alert_user(
                    dry_run=args.dry_run,
                    super_agency_id=args.super_agency_id,
                    metric_definition_key_subset=args.metric_definition_key_subset,
                    child_agency_id_subset=_child_agency_id_subset,
                    current_session=session,
                    agency_name=args.agency_name,
                    send_grid_client=email_client,
                    user_email=args.user_email,
                )
