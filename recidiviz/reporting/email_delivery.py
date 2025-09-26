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

"""Functionality for delivery of email reports.

This module implements delivery of emails. The expectation is that the emails are in the appropriate Storage bucket
and conform to the following:

1) The bucket contains a folder whose name is the batch id
2) Inside the folder is a single html file for each recipient
3) Each file name is [email address].html
4) The contents of the file are ready to be sent with no further modification
"""
import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.results import MultiRequestResult
from recidiviz.reporting.constants import DEFAULT_EMAIL_SUBJECT, Batch, ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.constants import (
    ADDITIONAL_EMAIL_ADDRESSES_KEY,
    SUBJECT_LINE_KEY,
)
from recidiviz.reporting.email_sent_metadata import EmailSentMetadata
from recidiviz.utils.sendgrid_client_wrapper import (
    SendGridClientWrapper,
    get_enforced_tls_only_state_codes,
)


def deliver(
    batch: Batch,
    redirect_address: Optional[str] = None,
    cc_addresses: Optional[List[str]] = None,
    subject_override: Optional[str] = None,
    email_allowlist: Optional[List[str]] = None,
) -> MultiRequestResult[str, str]:
    """Delivers emails for the given batch.

    Delivers emails to the email address specified in the generated email filename.

    If a redirect_address is provided, emails are sent to the redirect_address and the original recipient's email
    address's name (without the domain) is appended to the name (i.e. dev+recipient_name@recidiviz.org).

    Args:
        batch: The batch of emails we're sending for
        redirect_address: (optional) An email address to which all emails will be sent
        cc_addresses: (optional) A list of email addressed to include on the cc line
        subject_override: (optional) The subject line to override to.
        email_allowlist: (optional) A subset list of email addresses that are to receive the email. If not provided,
        all recipients will be sent emails to.

    Returns:
        A MultiRequestResult containing successes and failures for the emails that were sent

    Raises:
        Raises errors related to external services like Google Storage but continues attempting to send subsequent
        emails if it receives an exception while attempting to send.  In that case the error is logged.
    """
    logging.info("Delivering emails for batch %s", batch)

    if redirect_address:
        logging.info(
            "Redirecting all emails for batch %s to be sent to %s",
            batch.batch_id,
            redirect_address,
        )

    if cc_addresses:
        logging.info(
            "CCing the following addresses: [%s]",
            ",".join(address for address in cc_addresses),
        )

    (
        from_email_name,
        from_email_address,
        reply_to_address,
        reply_to_name,
    ) = get_sender_info(batch.report_type)

    content_bucket = utils.get_email_content_bucket_name()
    html_files = load_files_from_storage(content_bucket, utils.get_html_folder(batch))
    attachment_files = load_files_from_storage(
        content_bucket, utils.get_attachments_folder(batch)
    )

    if len(html_files.items()) == 0:
        msg = f"No files found for batch {batch} in the bucket {content_bucket}"
        logging.error(msg)
        raise IndexError(msg)

    if email_allowlist is not None:
        html_files = {
            email: content
            for email, content in html_files.items()
            if email in email_allowlist
        }

    succeeded_email_sends: List[str] = []
    failed_email_sends: List[str] = []
    sendgrid = (
        SendGridClientWrapper(key_type="enforced_tls_only")
        if batch.state_code in get_enforced_tls_only_state_codes()
        else SendGridClientWrapper()
    )

    for recipient_email_address in html_files:
        additional_cc_addresses: list[str] = []
        html_path = utils.get_html_filepath(
            batch,
            recipient_email_address,
        )

        file_metadata = get_file_metadata(html_path)

        if subject_override:
            subject = subject_override
        elif file_metadata and (
            subject_from_metadata := file_metadata.get(SUBJECT_LINE_KEY)
        ):
            subject = subject_from_metadata
        else:
            subject = DEFAULT_EMAIL_SUBJECT

        if (
            redirect_address is None
            and file_metadata
            and ADDITIONAL_EMAIL_ADDRESSES_KEY in file_metadata
        ):
            # If there is no redirect address, then we expect the additional emails to exist in the metadata.
            # See recidiviz/reporting/email_generation.py for where the metadata is written.
            additional_cc_addresses = (
                json.loads(file_metadata[ADDITIONAL_EMAIL_ADDRESSES_KEY])
                if file_metadata[ADDITIONAL_EMAIL_ADDRESSES_KEY]
                else []
            )

            if additional_cc_addresses:
                logging.info(
                    "CCing the following addresses on email to %s: [%s] ",
                    recipient_email_address,
                    ",".join(address for address in additional_cc_addresses),
                )

        all_cc_addresses = (
            cc_addresses + additional_cc_addresses
            if cc_addresses
            else additional_cc_addresses
        )

        try:
            utils.validate_email_address(recipient_email_address)
        except ValueError:
            logging.error(
                "Invalid recipient email address: %s", recipient_email_address
            )
            failed_email_sends.append(recipient_email_address)
            continue
        except Exception as e:
            logging.error(
                "Error on validate_email_address for [%s]: %s",
                recipient_email_address,
                e,
            )
            failed_email_sends.append(recipient_email_address)
            continue

        try:
            sent_successfully = sendgrid.send_message(
                to_email=recipient_email_address,
                from_email=from_email_address,
                from_email_name=from_email_name,
                subject=subject,
                html_content=html_files[recipient_email_address],
                redirect_address=redirect_address,
                cc_addresses=all_cc_addresses if all_cc_addresses else None,
                text_attachment_content=attachment_files.get(recipient_email_address),
                reply_to_email=reply_to_address,
                reply_to_name=reply_to_name,
                disable_unsubscribe=True,
            )
        except Exception as e:
            logging.error(
                "Exception on send_message for %s: %s", recipient_email_address, e
            )
            sent_successfully = False

        if sent_successfully:
            succeeded_email_sends.append(recipient_email_address)
        else:
            failed_email_sends.append(recipient_email_address)

    if len(succeeded_email_sends) != 0:
        # modifying custom metadata only in metadata and not metadata.json
        # first need to get the data from gcs, then add that to the EmailSentMetadata
        # object, and then finally write that object back to gcs
        sent_date = datetime.now()
        gcsfs = GcsfsFactory.build()
        email_sent_metadata = EmailSentMetadata.build_from_gcs(batch, gcsfs)
        email_sent_metadata.add_new_email_send_result(
            sent_date=sent_date,
            total_delivered=len(succeeded_email_sends),
            redirect_address=redirect_address,
        )
        email_sent_metadata.write_to_gcs(batch, gcsfs)

    logging.info(
        "Sent %s emails. %s emails failed to send",
        len(succeeded_email_sends),
        len(failed_email_sends),
    )
    return MultiRequestResult(
        successes=succeeded_email_sends, failures=failed_email_sends
    )


def email_from_file_name(file_name: str) -> str:
    """Extract the email address from a blob name.

    Args:
        file_name: Assumes that the file name is of the form of [email address].html
    """
    return re.sub(r"(.html|.txt)$", "", file_name)


def load_files_from_storage(bucket_name: str, batch_id_path: str) -> Dict[str, str]:
    """Loads the files for this batch and bucket name from Cloud Storage.

    This function is guaranteed to either return a dictionary with 1 or more results or throw an exception if there is
    a problem loading any of the files.

    Args:
        bucket_name: The bucket name to find the batch_id files
        batch_id_path: The path, containing the state_code, to the identifier for this batch

    Returns:
        A dict whose keys are the email addresses of recipients and values are strings of the email body or file
        attachment content to send.

    Raises:
        Passes through exceptions from Storage and raises its own if there are no results in this batch.
    """
    try:
        gcs_file_system = GcsfsFactory.build()
        paths = [
            path
            for path in gcs_file_system.ls(bucket_name, blob_prefix=batch_id_path)
            if isinstance(path, GcsfsFilePath)
        ]
    except Exception:
        logging.error(
            "Unable to list files in folder. Bucket = %s, batch_id = %s",
            bucket_name,
            batch_id_path,
        )
        raise

    files = {}
    for path in paths:
        try:
            body = gcs_file_system.download_as_string(path)
        except Exception:
            logging.error(
                "Unable to load file %s from bucket %s", path.blob_name, bucket_name
            )
            raise

        email_address = email_from_file_name(path.file_name)
        files[email_address] = body

    return files


def get_file_metadata(file_path: GcsfsFilePath) -> Optional[Dict[str, str]]:
    """
    Gets the metadata for the object at the given path if it exists in the fs.

    Args:
        file_path: The GCS file path to get metadata for
    """
    try:
        gcs_file_system = GcsfsFactory.build()
        metadata = gcs_file_system.get_metadata(path=file_path)
    except Exception:
        logging.error("Error while attempting to get metadata at %s", file_path)
        raise

    return metadata


def get_sender_info(report_type: ReportType) -> Tuple[str, str, str, str]:
    if report_type == ReportType.OutliersSupervisionOfficerSupervisor:
        from_email_name = "Recidiviz Alerts"
    else:
        logging.error("Unsupported report type: %s", report_type.value)
        raise ValueError(f"Unsupported report type: {report_type.value}")

    from_email_address = "reports@recidiviz.org"
    reply_to_address = "feedback@recidiviz.org"
    reply_to_name = "Recidiviz Support"

    return from_email_name, from_email_address, reply_to_address, reply_to_name
