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

"""Functionality for the generation of email reports.

Generates the HTML for email reports and stores it for later delivery.
"""

import json
import logging

from string import Template

from google.cloud import pubsub_v1

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.reporting.context.report_context import ReportContext


def generate(report_context: ReportContext) -> None:
    """Generates and uploads the HTML file and the file attachment for the identified recipient's email.

    Receives the full user data, applies it to the HTML template and stores the result in Cloud Storage.

    Uploads the attachment content to Cloud Storage as a .txt file.

    Args:
        report_context: The context for a single recipient
    """
    prepared_data = report_context.get_prepared_data()
    check_for_required_keys(prepared_data)

    html_template_path = report_context.get_html_template_filepath()
    html_content = generate_html_content(html_template_path, prepared_data)
    attachment_content = prepared_data['attachment_content']

    html_path = utils.get_html_filepath(
            prepared_data[utils.KEY_BATCH_ID],
            prepared_data[utils.KEY_EMAIL_ADDRESS],
    )
    upload_file_contents_to_gcs(file_path=html_path, file_contents=html_content, content_type='text/html')

    if attachment_content:
        attachment_path = utils.get_attachment_filepath(
            prepared_data[utils.KEY_BATCH_ID],
            prepared_data[utils.KEY_EMAIL_ADDRESS],
        )

        upload_file_contents_to_gcs(file_path=attachment_path,
                                    file_contents=attachment_content,
                                    content_type='text/plain')


def generate_html_content(html_template_path: str, prepared_data: dict) -> str:
    """Generates the HTML file for the identified recipient's email."""
    try:
        with open(html_template_path) as html_template:
            template = Template(html_template.read())
            html_content = template.substitute(prepared_data)
        return html_content
    except KeyError as err:
        logging.error("Attribute required for HTML template missing from recipient data: "
                      "batch id = %s, email address = %s, attribute = %s",
                      prepared_data[utils.KEY_BATCH_ID], prepared_data[utils.KEY_EMAIL_ADDRESS], err)
        raise
    except Exception:
        logging.error("Unexpected error during templating. Recipient data = %s", prepared_data)
        raise


def upload_file_contents_to_gcs(file_path: GcsfsFilePath,
                                file_contents: str,
                                content_type: str) -> None:
    """Uploads a file's content to Cloud Storage.

    Args:
        file_path: The GCS path to write to
        file_contents: The content to upload to the Cloud Storage file path.
        content_type: The content type for the file that will be uploaded to Cloud Storage.
    """
    try:
        gcs_file_system = GcsfsFactory.build()
        gcs_file_system.upload_from_string(path=file_path, contents=file_contents, content_type=content_type)
    except Exception:
        logging.error("Error while attempting upload of %s", file_path)
        raise


def check_for_required_keys(recipient_data: dict) -> None:
    """Checks recipient_data for required information and raises errors.

    Raises errors if email address, state id, or batch id are missing from recipient_data. This module cannot
    function without them so callers should allow the raised errors to propagate up the stack.
    """
    keys = [utils.KEY_EMAIL_ADDRESS, utils.KEY_STATE_CODE, utils.KEY_BATCH_ID]

    for key in keys:
        if key not in recipient_data:
            raise KeyError(f"Unable to generate email due to missing key {key}. "
                           f"Recipient data = {json.dumps(recipient_data)}")


def start_chart_generation(report_context: ReportContext) -> None:
    """Starts chart generation for a recipient.

    Uses Pub/Sub to send a message to the chart function. The message contains all of the recipient's data since the
    chart function will pass it along to the next step in the generation process.

    Args:
        report_context: The report context containing the data and chart type
    """
    publisher = pubsub_v1.PublisherClient()

    prepared_data = report_context.get_prepared_data()
    payload = json.dumps(prepared_data)  # no error checking here since we already validated the JSON previously
    # TODO(#3260): Generalize this with report context
    topic = utils.get_chart_topic()
    publisher.publish(topic, payload.encode("utf-8"))
