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
for use in the emails. When data retrieval is complete this process initiates chart generation for each recipient using
Pub/Sub.
"""

import json
import logging
from typing import List

import recidiviz.reporting.email_generation as email_generation
import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.reporting.context.available_context import get_report_context


def start(state_code: str, report_type: str) -> str:
    """Begins data retrieval a new batch of email reports.

    Start with collection of data from the calculation pipelines. Send messages to start chart generation.

    Args:
        state_code: The state for which to generate reports
        report_type: The type of report to send

    Returns: The batch id for the newly started batch
    """
    batch_id = utils.generate_batch_id()
    logging.info("New batch started for %s and %s. Batch id = %s", state_code, report_type, batch_id)

    recipient_data = retrieve_data(state_code, report_type, batch_id)

    for recipient in recipient_data:
        recipient[utils.KEY_BATCH_ID] = batch_id
        report_context = get_report_context(state_code, report_type, recipient)
        email_generation.generate(report_context)

    return batch_id


def retrieve_data(state_code: str, report_type: str, batch_id: str) -> List:
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
    try:
        data_filename = utils.get_data_filename(state_code, report_type)
        file_contents_string = utils.load_string_from_storage(data_bucket, data_filename)
    except:
        logging.info("Unable to load data file %s/%s", data_bucket, data_filename)
        raise

    archive_bucket = utils.get_data_archive_bucket_name()
    archive_filename = ''
    try:
        archive_filename = utils.get_data_archive_filename(batch_id)
        utils.upload_string_to_storage(archive_bucket, archive_filename, file_contents_string, "text/json")
    except Exception:
        logging.error("Unable to archive the data file to %s/%s", archive_bucket, archive_filename)
        raise

    json_list = file_contents_string.splitlines()

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
    return recipient_data
