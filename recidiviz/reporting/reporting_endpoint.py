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
from http import HTTPStatus
import logging
from typing import Tuple

from flask import Blueprint, request

import recidiviz.reporting.data_retrieval as data_retrieval
import recidiviz.reporting.email_delivery as email_delivery
from recidiviz.reporting.email_reporting_utils import validate_email_address
from recidiviz.reporting.region_codes import InvalidRegionCodeException
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_only_str_param_value, get_str_param_values

reporting_endpoint_blueprint = Blueprint('reporting_endpoint_blueprint', __name__)


@reporting_endpoint_blueprint.route('/start_new_batch', methods=['GET', 'POST'])
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

    Returns:
        Text indicating the results of the run and an HTTP status

    Raises:
        Nothing.  Catch everything so that we can always return a response to the request
    """
    try:
        state_code = get_only_str_param_value('state_code', request.args)
        report_type = get_only_str_param_value('report_type', request.args)
        test_address = get_only_str_param_value('test_address', request.args)
        region_code = get_only_str_param_value('region_code', request.args)

        validate_email_address(test_address)
    except ValueError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    if not state_code or not report_type:
        msg = "Request does not include 'state_code' and 'report_type' parameters"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    # Normalize query param inputs
    state_code = state_code.upper()
    if test_address == '':
        test_address = None
    region_code = None if not region_code else region_code.upper()

    try:
        batch_id = data_retrieval.start(state_code, report_type, test_address, region_code)
    except InvalidRegionCodeException:
        return 'Invalid region code provided', HTTPStatus.BAD_REQUEST
    else:
        test_address_text = f"Emails generated for test address: {test_address}" if test_address else ""
        return (f"New batch started for {state_code} and {report_type}.  Batch "
                f"id = {batch_id}. {test_address_text}"), HTTPStatus.OK


@reporting_endpoint_blueprint.route('/deliver_emails_for_batch', methods=['GET', 'POST'])
@requires_gae_auth
def deliver_emails_for_batch() -> Tuple[str, HTTPStatus]:
    """Deliver a batch of generated emails.

    Validates email addresses provided in the query params.

    Query parameters:
        batch_id: (required) Identifier for this batch
        redirect_address: (optional) An email address to which all emails will be sent. This can be used for redirecting
        all of the reports to a supervisor.
        cc_address: (optional) An email address to which all emails will be CC'd. This can be used for sending
        a batch of reports to multiple recipients. Multiple cc_address params can be given.
            Example:
            ?batch_id=123&cc_address=cc-one@test.org&cc_address=cc_two@test.org&cc_address=cc_three@test.org

    Returns:
        Text indicating the results of the run and an HTTP status

    Raises:
        Nothing.  Catch everything so that we can always return a response to the request
    """

    try:
        batch_id = get_only_str_param_value('batch_id', request.args)
        redirect_address = get_only_str_param_value('redirect_address', request.args)
        cc_addresses = get_str_param_values('cc_address', request.args)

        validate_email_address(redirect_address)
        for cc_address in cc_addresses:
            validate_email_address(cc_address)
    except ValueError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    if not batch_id:
        msg = "Query parameter 'batch_id' not received"
        logging.error(msg)
        return msg, HTTPStatus.BAD_REQUEST

    success_count, failure_count = email_delivery.deliver(batch_id,
                                                          redirect_address=redirect_address,
                                                          cc_addresses=cc_addresses)

    redirect_text = f"to the redirect email address {redirect_address}" if redirect_address else ""
    cc_addresses_text = f"CC'd {','.join(email for email in cc_addresses)}." if cc_addresses else ""

    return f"Sent {success_count} emails {redirect_text}. {cc_addresses_text} " \
           f"{failure_count} emails failed to send", HTTPStatus.OK
