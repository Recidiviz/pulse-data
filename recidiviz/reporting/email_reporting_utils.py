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

""" Utilities and constants shared across python modules in this package
"""
import logging
import os
import re

from datetime import datetime

from google.cloud import storage

from recidiviz.utils import metadata
from recidiviz.utils import secrets


_PO_REPORT_CDN_STATIC_IP_KEY = 'po_report_cdn_static_IP'


def get_env_var(key: str) -> str:
    """Retrieve an environment variable

    Args:
        key: The key of the env var to retrieve

    Returns:
        The value of the variable. Guaranteed to return if it is set.

    Raises:
        KeyError if the environment variable does not exist
    """
    value = os.environ.get(key)
    if value is None:
        raise KeyError(f"The environment variable '{key}' is not set")
    return value


# Keys used to identify elements in dictionaries
KEY_BATCH_ID = "batch_id"
KEY_REPORT_TYPE = "report_type"
KEY_EMAIL_ADDRESS = "email_address"
KEY_STATE_CODE = "state_code"


def format_test_address(test_address: str, recipient_email_address: str) -> str:
    """Format a test_address to create a unique email address with the recipient_email_address's username.

    Example: tester+recipient_username@testers-domain.ext

    If either email address is invalid, it will raise a ValueError.
    """
    validate_email_address(recipient_email_address)
    validate_email_address(test_address)

    [recipient_name, _domain] = recipient_email_address.split("@")
    [test_name, domain] = test_address.split("@")
    return f"{test_name}+{recipient_name}@{domain}"


def validate_email_address(email_address: str) -> None:
    """Basic sanity check that the email address is formatted correctly.

    Example of a valid email address pattern: any_number_of_letters.123+-@123.multiple.domain-names.com
    """
    valid_email_pattern = r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'
    if not re.match(valid_email_pattern, email_address):
        raise ValueError(f"Invalid email address format: [{email_address}]")


def get_project_id() -> str:
    return metadata.project_id()


def get_data_storage_bucket_name() -> str:
    return f'{get_project_id()}-report-data'


def get_data_archive_bucket_name() -> str:
    return f'{get_project_id()}-report-data-archive'


def get_html_bucket_name() -> str:
    return f'{get_project_id()}-report-html'


def get_images_bucket_name() -> str:
    return f'{get_project_id()}-report-images'


def get_cdn_static_ip() -> str:
    cdn_ip = secrets.get_secret(_PO_REPORT_CDN_STATIC_IP_KEY)
    if not cdn_ip:
        raise ValueError(f'Could not get secret value for `CDN static IP`. Provided with '
                         f'key={_PO_REPORT_CDN_STATIC_IP_KEY}')

    return cdn_ip


# TODO(#3265): Convert from HTTP to HTTPS when a certificate has been set up
def get_static_image_path(state_code: str, report_type: str) -> str:
    return f'http://{get_cdn_static_ip()}/{state_code}/{report_type}/static'


def get_data_filename(state_code: str, report_type: str) -> str:
    return f'{report_type}/{state_code}/{report_type}_data.json'


def get_data_archive_filename(batch_id: str) -> str:
    return f'{batch_id}.json'


def get_properties_filename(state_code: str, report_type: str) -> str:
    return f'{report_type}/{state_code}/properties.json'


def get_html_filename(batch_id: str, email_address: str) -> str:
    return f'{batch_id}/{email_address}.html'


def get_template_filename(state_code: str, report_type: str) -> str:
    return f'{report_type}/{state_code}/template.html'


# TODO(#3260): Make this general-purpose to work for any report type's chart
def get_chart_topic() -> str:
    return f'projects/{get_project_id()}/topics/report_po_comparison_chart'


def load_string_from_storage(bucket_name: str, filename: str) -> str:
    """Load object from Cloud Storage and return as string.

    Args:
        bucket_name: The identifier of the Cloud Storage bucket
        filename: The identifier of the object within the bucket

    Returns:
        String form of the object decoded using UTF-8

    Raises:
        All errors.  Callers are expected to handle.
    """
    storage_client = storage.Client()

    logging.info("Downloading %s/%s...", bucket_name, filename)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    contents = blob.download_as_string().decode("utf-8")

    logging.debug("Downloaded %s/%s.", bucket_name, filename)

    return contents


def upload_string_to_storage(bucket_name: str, filename: str, contents: str, content_type: str = 'text/plain') -> None:
    """Upload a string into Cloud Storage.

    Creates a new object in the given bucket with the given filename.

    Args:
        bucket_name: The identifier of the Cloud Storage bucket
        filename: The identifier of the object within the bucket
        contents: A string to put in the object
        content_type: Optional parameter if the content is something other than plain text

    Raises:
        All errors.  Callers are expected to handle.
    """
    storage_client = storage.Client()

    logging.info("Uploading %s/%s...", bucket_name, filename)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(contents, content_type=content_type)

    logging.info("Uploaded %s/%s.", bucket_name, filename)


def generate_batch_id() -> str:
    """Create a new batch id.

    Create a batch ID using the current date and time.

    Returns:
        String in the format YYYYMMDDHHMMSS. Note that this is a string not
        and integer because callers will exclusively use this in situations
        requiring a string.
    """
    dt = datetime.now()
    format_str = "%Y%m%d%H%M%S"
    return dt.strftime(format_str)
