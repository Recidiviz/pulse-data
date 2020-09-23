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
from datetime import datetime
import logging
import os

from google.cloud import storage


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


def get_project_id() -> str:
    return get_env_var('GCP_PROJECT')


def get_data_storage_bucket_name() -> str:
    return f'{get_project_id()}-report-data'


def get_data_archive_bucket_name() -> str:
    return f'{get_project_id()}-report-data-archive'


def get_html_bucket_name() -> str:
    return f'{get_project_id()}-report-html'


def get_images_bucket_name() -> str:
    return f'{get_project_id()}-report-images'


def get_cdn_static_ip() -> str:
    return get_env_var('CDN_STATIC_IP')


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

    logging.debug("Downloading %s/%s...", bucket_name, filename)

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

    logging.debug("Uploading %s/%s...", bucket_name, filename)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(contents, content_type=content_type)

    logging.debug("Uploaded %s/%s.", bucket_name, filename)


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
