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
from typing import Any, Dict, Generator, List, Optional

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.results import MultiRequestResult
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.reporting import email_generation
from recidiviz.reporting.constants import Batch, ReportType
from recidiviz.reporting.context.available_context import get_report_context
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.data_retrieval import (
    retrieve_data_for_outliers_supervision_officer_supervisor,
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
    batch: Batch,
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
        message_body_override: Optional override for the message body in the email.

    Returns: A MultiRequestResult containing the email addresses for which reports were successfully generated for
            and failed to generate for
    """

    logging.info(
        "New batch started for %s (region: %s) and %s. Batch id = %s",
        batch.state_code,
        region_code,
        batch.report_type,
        batch.batch_id,
    )

    recipients: List[Recipient] = retrieve_data(batch)
    recipients = filter_recipients(recipients, region_code, email_allowlist)

    if test_address:
        logging.info("Overriding batch emails with test address: %s", test_address)

        # If a test address is used, do not include the additional email addresses in the new recipient object.
        recipients = [
            Recipient(
                email_address=utils.format_test_address(
                    test_address, recipient.email_address
                ),
                state_code=recipient.state_code,
                district=recipient.district,
                data=recipient.data,
            )
            for recipient in recipients
        ]

    if message_body_override is not None:
        logging.info(
            "Overriding default message body in batch emails (batch id = %s)",
            batch.batch_id,
        )
        recipients = [
            Recipient(
                email_address=recipient.email_address,
                state_code=recipient.state_code,
                district=recipient.district,
                data={**recipient.data, "message_body_override": message_body_override},
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
            report_context = get_report_context(batch, recipient)
            email_generation.generate(batch, recipient, report_context)
        except Exception:
            failed_email_addresses.append(recipient.email_address)
            logging.error(
                "Failed to generate report email for %s", recipient, exc_info=True
            )
        else:
            succeeded_email_addresses.append(recipient.email_address)

    _write_batch_metadata(
        batch=batch,
        **metadata,
    )
    return MultiRequestResult(
        successes=succeeded_email_addresses, failures=failed_email_addresses
    )


def retrieve_data(
    batch: Batch,
) -> List[Recipient]:
    """Retrieves the data for email generation of the given report type for the given state.

    For OutliersSupervisionOfficerSupervisor, get the data from the CloudSql instance.

    Args:
        batch: The identifier for this batch

    Returns:
        A list of recipients

    Raises:
        Non-recoverable errors that should stop execution. Attempts to catch and handle errors that are recoverable.
        Provides logging for debug purposes whenever possible.
    """
    if batch.report_type == ReportType.OutliersSupervisionOfficerSupervisor:
        outliers_config = get_outliers_backend_config(batch.state_code.value)
        # Get data from querier
        results_by_supervisor = (
            retrieve_data_for_outliers_supervision_officer_supervisor(batch)
        )

        # Archive data
        results_by_supervisor_json = {
            supervisor_id: supervisor_data.to_dict()
            for supervisor_id, supervisor_data in results_by_supervisor.items()
        }
        results_by_supervisor_str = json.dumps(results_by_supervisor_json)
        _create_report_json_archive(batch, results_by_supervisor_str)

        return [
            Recipient(
                email_address=supervisor_info.recipient_email_address,
                state_code=batch.state_code,
                data={
                    "report": supervisor_info,
                    "config": outliers_config,
                },
                additional_email_addresses=supervisor_info.additional_recipients,
            )
            for supervisor_info in results_by_supervisor.values()
        ]

    raise ValueError("unexpected report type for retrieving data")


def _create_report_json_archive(batch: Batch, report_json: str) -> None:
    gcs_file_system = GcsfsFactory.build()
    archive_bucket = utils.get_data_archive_bucket_name()
    archive_filename = utils.get_data_archive_filename(batch)

    try:
        archive_path = GcsfsFilePath.from_absolute_path(
            f"gs://{archive_bucket}/{archive_filename}"
        )
        gcs_file_system.upload_from_string(
            path=archive_path, contents=report_json, content_type="text/json"
        )
    except Exception:
        logging.error(
            "Unable to archive the data file to %s/%s", archive_bucket, archive_filename
        )
        raise


def _json_lines(json_file: str) -> Generator[Dict[str, Any], None, None]:
    for json_str in json_file.splitlines():
        try:
            yield json.loads(json_str)
        except Exception as err:
            logging.error(
                "Unable to parse JSON. Offending json string is: '%s'. <%s> %s",
                json_str,
                type(err).__name__,
                err,
            )


def _write_batch_metadata(
    *,
    batch: Batch,
    **metadata_fields: str,
) -> None:
    gcsfs = GcsfsFactory.build()
    gcsfs.upload_from_string(
        path=gcsfs_path_for_batch_metadata(batch),
        contents=json.dumps(
            {**metadata_fields, "report_type": batch.report_type.value}
        ),
        content_type="text/json",
    )
