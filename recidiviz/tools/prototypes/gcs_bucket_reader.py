# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""
Defines the GCSBucketReader class which reads a blob from a Google Cloud Storage bucket.

Usage:

pipenv run python -m recidiviz.tools.prototypes.gcs_bucket_reader \
  --bucket-name={} --blob-name={}

"""

import argparse
import logging
from typing import Optional

import attr
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket-name",
        type=str,
        required=True,
        help="The name of the GCS Storage Bucket.",
    )
    parser.add_argument(
        "--blob-name",
        type=str,
        required=True,
        help="The name of the blob to download.",
    )
    return parser


@attr.define(kw_only=True)
class GCSBucketReader:
    """
    An interface for reading objects from a Google Cloud Storage bucket.

    This class initializes a Google Cloud Storage client and provides methods to
    download blobs from the specified bucket.

    Parameters:
        bucket_name (str): Required. The name of the Google Cloud Storage bucket.

    Methods:
        download_blob(blob_name: str) -> Optional[bytes]:
            Downloads the specified blob from the bucket into memory. Returns the contents of the blob as bytes
            if successful, or None if an error occurs.
    """

    bucket_name: str = attr.field()
    storage_client: storage.Client = attr.field(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        """Initialize a GCSClient with the specified bucket name."""
        try:
            self.storage_client = storage.Client()
        except GoogleCloudError as e:
            logger.error(
                "GCSBucketReader: Failed to create Google Cloud Storage client: %s", e
            )
        except Exception as e:
            logger.error(
                "GCSBucketReader: An unexpected error occurred during client initialization: %s",
                e,
            )

    def _decode_blob_contents(
        self, blob_contents: bytes, encoding: str = "utf-8"
    ) -> Optional[str]:
        """
        Decodes the blob contents from bytes to a string.

        Returns:
            Optional[str]: The decoded string if successful, or None if an error occurs.
        """
        try:
            return blob_contents.decode(encoding)
        except (UnicodeDecodeError, AttributeError) as e:
            logger.error("GCSBucketReader: Failed to decode blob contents: %s", e)
            return None

    def download_blob(self, blob_name: str) -> Optional[str]:
        """
        Downloads a blob into memory. Assumes that the blob should be decoded using
        utf-8 encoding.

        Parameters:
            blob_name (str): Required. The name of the blob to download from the bucket.

        Returns:
            Optional[str]: The contents of the blob as a string if successful, or None
            if an error occurs.
        """
        if self.storage_client is None:
            logger.error("GCSBucketReader: Storage client is not initialized.")
            return None

        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)
            bytes_content = blob.download_as_bytes()
            return self._decode_blob_contents(bytes_content)

        except NotFound as e:
            logger.error(
                "GCSBucketReader Error: %s. The specified bucket or blob was not found.",
                e,
            )
        except GoogleCloudError as e:
            logger.error("GCSBucketReader Google Cloud error: %s", e)
        except Exception as e:
            logger.error("GCSBucketReader: An unexpected error occurred: %s", e)
        return None


if __name__ == "__main__":
    # Example usage.
    args = create_parser().parse_args()
    gcs_reader = GCSBucketReader(bucket_name=args.bucket_name)
    content = gcs_reader.download_blob(blob_name=args.blob_name)
    if content is not None:
        logger.info("Blob contents: %s", content)
