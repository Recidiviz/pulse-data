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
from typing import List, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
import recidiviz.reporting.email_generation as email_generation
import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.reporting.context.available_context import get_report_context
from recidiviz.reporting.recipient import Recipient
from recidiviz.reporting.region_codes import InvalidRegionCodeException, REGION_CODES


def start(state_code: str,
          report_type: str,
          test_address: Optional[str] = None,
          region_code: Optional[str] = None,
          message_body: Optional[str] = None) -> str:
    """Begins data retrieval for a new batch of email reports.

    Start with collection of data from the calculation pipelines.

    If a test_address is provided, it overrides the recipient's email_address with a formatted test_address
    and uses the formatted test_address to save the generated emails.

    Args:
        state_code: The state for which to generate reports
        report_type: The type of report to send
        test_address: Optional email address for which to generate all emails
        region_code: Optional region code which specifies the sub-region of the state in which to
            generate reports. If empty, this generates reports for all regions.
        message_body: Optional override for the message body in the email.

    Returns: The batch id for the newly started batch
    """
    batch_id = utils.generate_batch_id()
    logging.info("New batch started for %s (region: %s) and %s. Batch id = %s",
                 state_code, region_code, report_type, batch_id)

    recipients: List[Recipient] = retrieve_data(state_code, report_type, batch_id)

    if region_code is not None and region_code not in REGION_CODES:
        raise InvalidRegionCodeException()

    if test_address:
        logging.info("Overriding batch emails with test address: %s", test_address)
        recipients = [
            recipient.create_derived_recipient({
                "email_address": utils.format_test_address(test_address, recipient.email_address),
            })
            for recipient in recipients
        ]

    if message_body is not None:
        logging.info("Overriding default message body in batch emails (batch id = %s)", batch_id)
        recipients = [
            recipient.create_derived_recipient({"message_body": message_body})
            for recipient in recipients
        ]

    for recipient in recipients:
        if region_code is not None and recipient.district != REGION_CODES[region_code]:
            continue

        report_context = get_report_context(state_code, report_type, recipient)
        email_generation.generate(report_context)

    return batch_id


def retrieve_data(state_code: str, report_type: str, batch_id: str) -> List[Recipient]:
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
    data_bucket = utils.get_data_storage_bucket_name()
    data_filename = ''
    gcs_file_system = GcsfsFactory.build()
    try:
        data_filename = utils.get_data_filename(state_code, report_type)
        path = GcsfsFilePath.from_absolute_path(f'gs://{data_bucket}/{data_filename}')
        file_contents = gcs_file_system.download_as_string(path)
    except BaseException:
        logging.info("Unable to load data file %s/%s", data_bucket, data_filename)
        raise

    archive_bucket = utils.get_data_archive_bucket_name()
    archive_filename = ''
    try:
        archive_filename = utils.get_data_archive_filename(batch_id)
        archive_path = GcsfsFilePath.from_absolute_path(f'gs://{archive_bucket}/{archive_filename}')
        gcs_file_system.upload_from_string(path=archive_path, contents=file_contents, content_type="text/json")
    except Exception:
        logging.error("Unable to archive the data file to %s/%s", archive_bucket, archive_filename)
        raise

    json_list = file_contents.splitlines()

    recipient_data: List[dict] = []
    for json_str in json_list:
        try:
            item = json.loads(json_str)
        except Exception as err:
            logging.error("Unable to parse JSON found in the file %s. Offending json string is: '%s'. <%s> %s",
                          data_filename, json_str, type(err).__name__, err)
        else:
            recipient_data.append(item)

    logging.info("Retrieved %s recipients from data file %s", len(recipient_data), data_filename)
    return [
        Recipient.from_report_json({
            **recipient,
            utils.KEY_BATCH_ID: batch_id,
        })
        for recipient in recipient_data
    ]
