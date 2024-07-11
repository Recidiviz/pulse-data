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
"""Tests for filename_normalization.py."""
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from cloudevents.http import CloudEvent
from google.api_core.exceptions import GoogleAPIError

from recidiviz.cloud_functions.ingest_filename_normalization import (
    handle_zipfile,
    normalize_filename,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath


def set_env_vars() -> None:
    os.environ["PROJECT_ID"] = "recidiviz-test"
    os.environ["DRY_RUN"] = "false"
    os.environ[
        "ZIPFILE_HANDLER_FUNCTION_URL"
    ] = "https://us-central1-recidiviz-test.cloudfunctions.net/handle_zipfile"


class TestNormalizeFilename(TestCase):
    """Tests for normalize_filename cloud function."""

    def setUp(self) -> None:
        set_env_vars()
        self.bucket = "test_bucket"
        self.relative_file_path = "test_file"
        attributes = {
            "id": "5e9f24a",
            "type": "google.cloud.storage.object.v1.finalized",
            "source": "sourceUrlHere",
        }
        data = {
            "bucket": self.bucket,
            "name": self.relative_file_path,
            "generation": 1,
            "metageneration": 1,
            "timeCreated": "2021-10-10 00:00:00.000000Z",
            "updated": "2021-11-11 00:00:00.000000Z",
        }
        self.event = CloudEvent(attributes, data)

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_successful_normalization(
        self, _mock_is_normalized: MagicMock, mock_fs: MagicMock
    ) -> None:
        path_instance = GcsfsFilePath(
            bucket_name=self.bucket, blob_name=self.relative_file_path
        )

        normalize_filename(self.event)
        mock_fs.mv_raw_file_to_normalized_path.assert_called_with(path_instance)

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_raise_unexpected_error(
        self, _mock_is_normalized: MagicMock, mock_fs: MagicMock
    ) -> None:
        mock_fs.mv_raw_file_to_normalized_path.side_effect = GoogleAPIError()

        with self.assertRaises(GoogleAPIError):
            normalize_filename(self.event)

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.cloud_functions_log"
    )
    def test_swallow_nonretryable_error(
        self,
        mock_logging: MagicMock,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        mock_fs.mv_raw_file_to_normalized_path.side_effect = ValueError()

        normalize_filename(self.event)
        mock_logging.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_successful_normalization_dry_run(
        self, _mock_is_normalized: MagicMock, mock_fs: MagicMock
    ) -> None:
        os.environ["DRY_RUN"] = "true"

        normalize_filename(self.event)
        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()

    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.cloud_functions_log"
    )
    def test_missing_bucket_or_name(self, mock_logging: MagicMock) -> None:
        bad_event = self.event
        bad_event.data["name"] = ""

        normalize_filename(bad_event)
        mock_logging.assert_called_once()

    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.GcsfsPath.from_bucket_and_blob_name"
    )
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.cloud_functions_log"
    )
    def test_incorrect_path_type(
        self, mock_logging: MagicMock, mock_path: MagicMock
    ) -> None:
        mock_path.return_value = GcsfsDirectoryPath(bucket_name=self.bucket)

        normalize_filename(self.event)
        mock_logging.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization._invoke_zipfile_handler"
    )
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_unnormalized_zip_file_handling(
        self,
        _mock_is_normalized: MagicMock,
        mock_invoke_zip_handler: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        zip_event = self.event
        zip_event.data["name"] = f"{self.relative_file_path}.zip"

        normalize_filename(zip_event)
        mock_invoke_zip_handler.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization._invoke_zipfile_handler"
    )
    def test_zip_file_handling_dry_run(
        self, mock_invoke_zip_handler: MagicMock, mock_fs: MagicMock
    ) -> None:
        os.environ["DRY_RUN"] = "true"

        zip_event = self.event
        zip_event.data["name"] = f"{self.relative_file_path}.zip"

        normalize_filename(zip_event)
        mock_invoke_zip_handler.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch("recidiviz.cloud_functions.ingest_filename_normalization.requests")
    @patch("recidiviz.cloud_functions.ingest_filename_normalization._get_access_token")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=True,
    )
    def test_normalized_zip_file_handling(
        self,
        _mock_is_normalized: MagicMock,
        _mock_get_token: MagicMock,
        mock_requests: MagicMock,
        _mock_fs: MagicMock,
    ) -> None:
        zip_event = self.event
        zip_event.data["name"] = f"unprocessed{self.relative_file_path}.zip"

        normalize_filename(zip_event)
        mock_requests.post.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=True,
    )
    def test_file_already_normalized(
        self, _mock_is_normalized: MagicMock, mock_fs: MagicMock
    ) -> None:
        zip_event = self.event
        zip_event.data["name"] = f"processed{self.relative_file_path}"

        normalize_filename(self.event)
        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()


class TestHandleZipfile(TestCase):
    """Tests for handle_zipfile cloud function."""

    def setUp(self) -> None:
        set_env_vars()
        request_data = {
            "bucket": "recidiviz-test-direct-ingest-state-us-xx",
            "name": "test_file.zip",
        }
        self.mock_request = MagicMock()
        self.mock_request.get_json.return_value = request_data

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    def test_zip_file_handling(self, mock_fs: MagicMock) -> None:
        handle_zipfile(self.mock_request)
        mock_fs.unzip.assert_called_once()
        mock_fs.mv.assert_called_once()
        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()
