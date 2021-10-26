# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""The logic to retrieve and process data for generated reports.

This module contains all the functions and logic needed to retrieve data from the calculation pipelines and modify it
for use in the emails.
"""

import json
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import dateutil.parser

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    OfficerDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state
from recidiviz.case_triage.user_context import UserContext
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting import email_generation
from recidiviz.reporting.context.available_context import get_report_context
from recidiviz.reporting.context.po_monthly_report.constants import (
    OFFICER_GIVEN_NAME,
    ReportType,
)
from recidiviz.reporting.email_reporting_utils import gcsfs_path_for_batch_metadata
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException

MAX_SUPERVISION_MISMATCHES_TO_SHOW = 5
IDEAL_SUPERVISION_MISMATCH_AGE_IN_DAYS = 30


def filter_recipients(
    recipients: List[Recipient],
    region_code: Optional[str] = None,
    email_allowlist: Optional[List[str]] = None,
) -> List[Recipient]:
    if region_code is not None and region_code not in REGION_CODES:
        raise InvalidRegionCodeException()

    return [
        recipient
        for recipient in recipients
        if all(
            [
                recipient.district == REGION_CODES[region_code]
                if region_code is not None
                else True,
                recipient.email_address in email_allowlist
                if email_allowlist is not None
                else True,
            ]
        )
    ]


def start(
    state_code: StateCode,
    report_type: ReportType,
    batch_id: Optional[str] = None,
    test_address: Optional[str] = None,
    region_code: Optional[str] = None,
    email_allowlist: Optional[List[str]] = None,
    message_body_override: Optional[str] = None,
) -> MultiRequestResult[str, str]:
    """Begins data retrieval for a new batch of email reports.

    Start with collection of data from the calculation pipelines.

    If a test_address is provided, it overrides the recipient's email_address with a formatted test_address
    and uses the formatted test_address to save the generated emails.

    Args:
        state_code: The state for which to generate reports
        report_type: The type of report to send
        batch_id: The batch id to save the newly started batch to
        test_address: Optional email address for which to generate all emails
        region_code: Optional region code which specifies the sub-region of the state in which to
            generate reports. If empty, this generates reports for all regions.
        email_allowlist: Optional list of email_addresses to generate for; all other recipients are skipped
        recipient_emails: Optional list of email_addresses to generate for; all other recipients are skipped
        message_body_override: Optional override for the message body in the email.

    Returns: A MultiRequestResult containing the email addresses for which reports were successfully generated for
            and failed to generate for
    """
    if batch_id is None:
        batch_id = utils.generate_batch_id()

    logging.info(
        "New batch started for %s (region: %s) and %s. Batch id = %s",
        state_code,
        region_code,
        report_type,
        batch_id,
    )

    recipients: List[Recipient] = retrieve_data(state_code, report_type, batch_id)
    recipients = filter_recipients(recipients, region_code, email_allowlist)

    if test_address:
        logging.info("Overriding batch emails with test address: %s", test_address)
        recipients = [
            recipient.create_derived_recipient(
                {
                    "email_address": utils.format_test_address(
                        test_address, recipient.email_address
                    ),
                }
            )
            for recipient in recipients
        ]

    if message_body_override is not None:
        logging.info(
            "Overriding default message body in batch emails (batch id = %s)", batch_id
        )
        recipients = [
            recipient.create_derived_recipient(
                {"message_body_override": message_body_override}
            )
            for recipient in recipients
        ]

    failed_email_addresses: List[str] = []
    succeeded_email_addresses: List[str] = []

    # Currently this is only used to pass review month & year, but when we need to add
    # more, the way that we do this should likely be changed/refactored.
    metadata: Dict[str, str] = {}

    for recipient in recipients:
        try:
            report_context = get_report_context(state_code, report_type, recipient)
            email_generation.generate(report_context)
        except Exception:
            failed_email_addresses.append(recipient.email_address)
            logging.error(
                "Failed to generate report email for %s", recipient, exc_info=True
            )
        else:
            succeeded_email_addresses.append(recipient.email_address)
            if report_type == ReportType.POMonthlyReport and len(metadata) == 0:
                metadata["review_year"] = recipient.data["review_year"]
                metadata["review_month"] = recipient.data["review_month"]

    _write_batch_metadata(
        batch_id=batch_id,
        state_code=state_code,
        report_type=report_type,
        **metadata,
    )
    return MultiRequestResult(
        successes=succeeded_email_addresses, failures=failed_email_addresses
    )


def retrieve_data(
    state_code: StateCode, report_type: ReportType, batch_id: str
) -> List[Recipient]:
    """Retrieves the data for email generation of the given report type for the given state.

    Get the data from Cloud Storage and return it in a list of dictionaries. Saves the data file into an archive
    bucket on completion, so that we have the ability to troubleshoot or re-generate a previous batch of emails
    later on.

    Args:
        state_code: State identifier used to retrieve appropriate data
        report_type: The type of report, used to determine the data file name
        batch_id: The identifier for this batch

    Returns:
        A list of recipient data dictionaries

    Raises:
        Non-recoverable errors that should stop execution. Attempts to catch and handle errors that are recoverable.
        Provides logging for debug purposes whenever possible.
    """
    if report_type == ReportType.POMonthlyReport:
        return _retrieve_data_for_po_monthly_report(state_code, batch_id)
    if report_type == ReportType.TopOpportunities:
        return _retrieve_data_for_top_opportunities(state_code)

    raise ValueError("unexpected report type for retrieving data")


def _top_opps_email_recipient_addresses() -> List[str]:
    # TODO(#7790): We should find a way to automate this. Right now, this is being mocked
    # out for use in test fixtures.
    return []


def _get_mismatch_data_for_officer(
    officer_email: str,
) -> List[Dict[str, str]]:
    """Fetches the list of supervision mismatches on an officer's caseload for display
    in our email templates."""
    with SessionFactory.using_database(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE), autocommit=False
    ) as session:
        try:
            officer = CaseTriageQuerier.officer_for_email(session, officer_email)
        except OfficerDoesNotExistError:
            return []

        try:
            policy_requirements = policy_requirements_for_state(
                StateCode(officer.state_code)
            )
        except Exception:
            # If for some reason we can't fetch the policy requirements, we should not show mismatches.
            return []

        user_context = UserContext(
            email=officer_email,
            authorization_store=AuthorizationStore(),  # empty store won't actually be leveraged
            current_user=officer,
        )
        opportunities = [
            opp.opportunity
            for opp in CaseTriageQuerier.opportunities_for_officer(
                session, user_context
            )
            if not opp.is_deferred()
            and opp.opportunity.opportunity_type
            == OpportunityType.OVERDUE_DOWNGRADE.value
        ]
        mismatches: List[Dict[str, str]] = []
        for opp in opportunities:
            client = CaseTriageQuerier.etl_client_for_officer(
                session, user_context, opp.person_external_id
            )

            mismatches.append(
                {
                    "full_name": client.full_name,
                    "person_external_id": client.person_external_id,
                    "last_score": opp.opportunity_metadata["assessmentScore"],
                    "last_assessment_date": opp.opportunity_metadata[
                        "latestAssessmentDate"
                    ],
                    "current_supervision_level": policy_requirements.get_supervision_level_name(
                        StateSupervisionLevel(client.supervision_level)
                    ),
                    "recommended_level": policy_requirements.get_supervision_level_name(
                        StateSupervisionLevel(
                            opp.opportunity_metadata["recommendedSupervisionLevel"]
                        )
                    ),
                }
            )

        mismatches.sort(key=lambda x: x["last_assessment_date"], reverse=True)
        if len(mismatches) > MAX_SUPERVISION_MISMATCHES_TO_SHOW:
            cutoff_date = date.today() - timedelta(
                days=IDEAL_SUPERVISION_MISMATCH_AGE_IN_DAYS
            )

            cutoff_index = len(mismatches) - MAX_SUPERVISION_MISMATCHES_TO_SHOW
            for i in range(cutoff_index):
                if (
                    dateutil.parser.parse(mismatches[i]["last_assessment_date"]).date()
                    <= cutoff_date
                ):
                    cutoff_index = i
                    break

            return mismatches[
                cutoff_index : cutoff_index + MAX_SUPERVISION_MISMATCHES_TO_SHOW
            ]

        return mismatches


def _retrieve_data_for_top_opportunities(state_code: StateCode) -> List[Recipient]:
    """Fetches list of recipients from the Case Triage backend where we store information
    about which opportunities are active via the OpportunityPresenter."""
    recipients = []
    for officer_email in _top_opps_email_recipient_addresses():
        mismatches = _get_mismatch_data_for_officer(officer_email)
        if mismatches is not None:
            with SessionFactory.using_database(
                SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE),
                autocommit=False,
            ) as session:
                officer = CaseTriageQuerier.officer_for_email(session, officer_email)
                recipients.append(
                    Recipient.from_report_json(
                        {
                            utils.KEY_EMAIL_ADDRESS: officer_email,
                            utils.KEY_STATE_CODE: state_code.value,
                            utils.KEY_DISTRICT: None,
                            OFFICER_GIVEN_NAME: officer.given_names,
                            "mismatches": mismatches,
                        }
                    )
                )

    return recipients


def _retrieve_data_for_po_monthly_report(
    state_code: StateCode, batch_id: str
) -> List[Recipient]:
    """Retrieves the data if the report type is POMonthlyReport."""
    data_bucket = utils.get_data_storage_bucket_name()
    data_filename = ""
    gcs_file_system = GcsfsFactory.build()
    try:
        data_filename = utils.get_data_filename(state_code, ReportType.POMonthlyReport)
        path = GcsfsFilePath.from_absolute_path(f"gs://{data_bucket}/{data_filename}")
        file_contents = gcs_file_system.download_as_string(path)
    except BaseException:
        logging.info("Unable to load data file %s/%s", data_bucket, data_filename)
        raise

    archive_bucket = utils.get_data_archive_bucket_name()
    archive_filename = ""
    try:
        archive_filename = utils.get_data_archive_filename(batch_id, state_code)
        archive_path = GcsfsFilePath.from_absolute_path(
            f"gs://{archive_bucket}/{archive_filename}"
        )
        gcs_file_system.upload_from_string(
            path=archive_path, contents=file_contents, content_type="text/json"
        )
    except Exception:
        logging.error(
            "Unable to archive the data file to %s/%s", archive_bucket, archive_filename
        )
        raise

    json_list = file_contents.splitlines()

    recipient_data: List[dict] = []
    for json_str in json_list:
        try:
            item = json.loads(json_str)
        except Exception as err:
            logging.error(
                "Unable to parse JSON found in the file %s. Offending json string is: '%s'. <%s> %s",
                data_filename,
                json_str,
                type(err).__name__,
                err,
            )
        else:
            if email := item.get("email_address"):
                mismatches = _get_mismatch_data_for_officer(email)
                if mismatches is not None:
                    item["mismatches"] = mismatches
            recipient_data.append(item)

    logging.info(
        "Retrieved %s recipients from data file %s", len(recipient_data), data_filename
    )
    return [
        Recipient.from_report_json(
            {
                **recipient,
                utils.KEY_BATCH_ID: batch_id,
            }
        )
        for recipient in recipient_data
    ]


def _write_batch_metadata(
    *,
    batch_id: str,
    state_code: StateCode,
    report_type: ReportType,
    **metadata_fields: str,
) -> None:
    gcsfs = GcsfsFactory.build()
    gcsfs.upload_from_string(
        path=gcsfs_path_for_batch_metadata(batch_id, state_code),
        contents=json.dumps({**metadata_fields, "report_type": report_type.value}),
        content_type="text/json",
    )
