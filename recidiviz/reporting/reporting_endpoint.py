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
# pylint:disable=bare-except

"""A single module containing all Python code related to Email Reporting.
"""
import json
import logging
from http import HTTPStatus
from json import JSONDecodeError
from typing import List, Optional, Tuple

from flask import Blueprint, request

import recidiviz.reporting.data_retrieval as data_retrieval
import recidiviz.reporting.email_delivery as email_delivery
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.reporting import email_reporting_utils
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_utils import (
    EmailMetadataReportDateError,
    InvalidReportTypeError,
    generate_report_date,
    get_report_type,
    validate_email_address,
)
from recidiviz.reporting.region_codes import InvalidRegionCodeException
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import get_only_str_param_value, get_str_param_values

reporting_endpoint_blueprint = Blueprint("reporting_endpoint_blueprint", __name__)


@reporting_endpoint_blueprint.route("/start_new_batch", methods=["GET", "POST"])
@requires_gae_auth
def start_new_batch() -> Tuple[str, HTTPStatus]:
    """Start a new batch of email generation for the indicated state.

    Validates the test address provided in the params.

    Query parameters:
        state_code: (required) A valid state code for which reporting is enabled (ex: "US_ID")
        report_type: (required) A valid report type identifier (ex: "po_monthly_report)
        test_address: (optional) Should only be used for testing. When provided, the test_address is used to generate
            the email filenames, ensuring that all emails in the batch can only be delivered to the test_address and not
            to the usual recipients of the report. The email filenames will include the original recipient's email
            username, for example: tester+recipient_username@tester-domain.org.
        region_code: (optional) Indicates the sub-region of the state to generate emails for. If
            omitted, we generate emails for all sub-regions of the state.
        email_allowlist: (optional) A json list of emails we should generate emails for. Emails that do not exist in the
            report will be silently skipped.
        message_body_override: (optional) If included, overrides the default message body.

    Returns:
        Text indicating the results of the run and an HTTP status

    Raises:
        Nothing.  Catch everything so that we can always return a response to the request
    """
    try:
        raw_state_code = get_only_str_param_value("state_code", request.args)
        report_type = ReportType(get_only_str_param_value("report_type", request.args))
        test_address = get_only_str_param_value("test_address", request.args)
        region_code = get_only_str_param_value("region_code", request.args)
        raw_email_allowlist = get_only_str_param_value("email_allowlist", request.args)
        message_body_override = get_only_str_param_value(
            "message_body_override", request.args, preserve_case=True
        )

        validate_email_address(test_address)

        email_allowlist: Optional[List[str]] = (
            json.loads(raw_email_allowlist) if raw_email_allowlist else None
        )

        if email_allowlist is not None:
            for recipient_email in email_allowlist:
                validate_email_address(recipient_email)

    except (ValueError, JSONDecodeError) as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    if not raw_state_code or not report_type:
        msg = "Request does not include 'state_code' and/or 'report_type' parameters"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    # Normalize query param inputs
    if test_address == "":
        test_address = None
    region_code = None if not region_code else region_code.upper()

    try:
        state_code = StateCode(raw_state_code.upper())
    except ValueError:
        msg = f"Invalid parameter: state_code={raw_state_code}"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    try:
        batch_id = email_reporting_utils.generate_batch_id()
        if in_development():
            with local_project_id_override(GCP_PROJECT_STAGING):
                result: MultiRequestResult[str, str] = data_retrieval.start(
                    state_code=state_code,
                    report_type=report_type,
                    batch_id=batch_id,
                    test_address=test_address,
                    region_code=region_code,
                    email_allowlist=email_allowlist,
                    message_body_override=message_body_override,
                )
        else:
            result = data_retrieval.start(
                state_code=state_code,
                report_type=report_type,
                batch_id=batch_id,
                test_address=test_address,
                region_code=region_code,
                email_allowlist=email_allowlist,
                message_body_override=message_body_override,
            )
    except InvalidRegionCodeException:
        return "Invalid region code provided", HTTPStatus.BAD_REQUEST
    else:
        new_batch_text = f"New batch started for {state_code.value} and {report_type}. Batch id = {batch_id}."
        test_address_text = (
            f"Emails generated for test address: {test_address}" if test_address else ""
        )
        counts_text = f"Successfully generated {len(result.successes)} email(s)"
        success_text = f"{new_batch_text} {test_address_text} {counts_text}"
        if result.failures and not result.successes:
            return (
                f"{success_text}"
                f" Failed to generate all emails. Retry the request again."
            ), HTTPStatus.INTERNAL_SERVER_ERROR
        if result.failures:
            return (
                f"{success_text}"
                f" Failed to generate {len(result.failures)} email(s): {','.join(result.failures)}"
            ), HTTPStatus.MULTI_STATUS

        return (f"{success_text}"), HTTPStatus.OK


@reporting_endpoint_blueprint.route(
    "/deliver_emails_for_batch", methods=["GET", "POST"]
)
@requires_gae_auth
def deliver_emails_for_batch() -> Tuple[str, HTTPStatus]:
    """Deliver a batch of generated emails.

    Validates email addresses provided in the query params.

    Query parameters:
        batch_id: (required) Identifier for this batch
        state_code: (required) A valid state code for which reporting is enabled (ex. "US_ID")
        redirect_address: (optional) An email address to which all emails will be sent. This can be used for redirecting
        all of the reports to a supervisor.
        cc_address: (optional) An email address to which all emails will be CC'd. This can be used for sending
        a batch of reports to multiple recipients. Multiple cc_address params can be given.
            Example:
            ?batch_id=123&cc_address=cc-one%40test.org&cc_address=cc_two%40test.org&cc_address=cc_three%40test.org
        subject_override: (optional) Override for subject being sent.
        email_allowlist: (optional) A json list of emails we should deliver emails to. Emails in this list should be a subset
        of the recipients in the batch.

    Returns:
        Text indicating the results of the run and an HTTP status

    Raises:
        Nothing.  Catch everything so that we can always return a response to the request
    """

    try:
        batch_id = get_only_str_param_value("batch_id", request.args)
        raw_state_code = get_only_str_param_value("state_code", request.args)
        redirect_address = get_only_str_param_value("redirect_address", request.args)
        cc_addresses = get_str_param_values("cc_address", request.args)
        subject_override = get_only_str_param_value(
            "subject_override", request.args, preserve_case=True
        )
        raw_email_allowlist = get_only_str_param_value("email_allowlist", request.args)

        validate_email_address(redirect_address)
        for cc_address in cc_addresses:
            validate_email_address(cc_address)

        email_allowlist: Optional[List[str]] = (
            json.loads(raw_email_allowlist) if raw_email_allowlist else None
        )

        if email_allowlist is not None:
            for recipient_email in email_allowlist:
                validate_email_address(recipient_email)
    except ValueError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    if not batch_id:
        msg = "Query parameter 'batch_id' not received"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    if not raw_state_code:
        msg = "Query parameter 'state_code' not received"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST
    try:
        state_code = StateCode(raw_state_code.upper())
    except ValueError:
        msg = f"Invalid query parameter state_code={raw_state_code} provided"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    # TODO(#7790): Support more email types.
    try:
        report_type = get_report_type(batch_id, state_code)
        if report_type != ReportType.POMonthlyReport:
            raise InvalidReportTypeError(
                f"Invalid report type: Sending emails with {report_type} is not implemented yet."
            )
    except InvalidReportTypeError as error:
        logging.error(error)
        return str(error), HTTPStatus.NOT_IMPLEMENTED

    try:
        report_date = generate_report_date(batch_id, state_code)
    except EmailMetadataReportDateError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    result = email_delivery.deliver(
        batch_id=batch_id,
        state_code=state_code,
        redirect_address=redirect_address,
        cc_addresses=cc_addresses,
        subject_override=subject_override,
        email_allowlist=email_allowlist,
        report_date=report_date,
    )

    redirect_text = (
        f"to the redirect email address {redirect_address}" if redirect_address else ""
    )
    cc_addresses_text = (
        f"CC'd {','.join(email for email in cc_addresses)}." if cc_addresses else ""
    )
    success_text = (
        f"Sent {len(result.successes)} emails {redirect_text}. {cc_addresses_text} "
    )
    if result.failures and not result.successes:
        return (
            f"{success_text} " f"All emails failed to send",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    if result.failures:
        return (
            f"{success_text} "
            f"{len(result.failures)} emails failed to send: {','.join(result.failures)}",
            HTTPStatus.MULTI_STATUS,
        )

    return (
        f"{success_text}",
        HTTPStatus.OK,
    )
