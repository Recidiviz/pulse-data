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
import base64
import datetime
import json
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
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)


def set_env_vars() -> None:
    os.environ["PROJECT_ID"] = "recidiviz-test"
    os.environ[
        "ZIPFILE_HANDLER_FUNCTION_URL"
    ] = "https://us-central1-recidiviz-test.cloudfunctions.net/handle_zipfile"


class TestNormalizeFilename(TestCase):
    """Tests for normalize_filename cloud function."""

    def setUp(self) -> None:
        set_env_vars()
        self.bucket = "recidiviz-test-direct-ingest-state-us-xx"
        self.relative_file_path = "test_file"
        self.event = self._build_pubsub_cloudevent(self.relative_file_path)

    def _build_pubsub_cloudevent(
        self,
        file_name: str,
        bucket: str | None = None,
        custom_time: str | None = None,
    ) -> CloudEvent:
        attributes = {
            "id": "5e9f24a",
            "type": "google.cloud.storage.object.v1.finalized",
            "source": "sourceUrlHere",
        }
        data = {
            "bucket": bucket or self.bucket,
            "name": file_name,
        }
        if custom_time is not None:
            data["customTime"] = custom_time
        message = {
            "message": {
                "data": base64.b64encode(json.dumps(data).encode("utf-8")).decode()
            }
        }
        return CloudEvent(attributes, message)

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_successful_normalization(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        path_instance = GcsfsFilePath(
            bucket_name=self.bucket, blob_name=self.relative_file_path
        )

        normalize_filename(self.event)

        mock_fs.mv_raw_file_to_normalized_path.assert_called_with(
            path_instance, dt=None, normalized_from_file_name=self.relative_file_path
        )

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_direct_upload_uses_custom_time_when_present(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        # A file with no region cleaner still picks up the preserved customTime as its
        # update_datetime when the finalize event carries one.
        event = self._build_pubsub_cloudevent(
            file_name=self.relative_file_path,
            custom_time="2026-08-19T14:05:03.716152Z",
        )

        normalize_filename(event)

        mock_fs.mv_raw_file_to_normalized_path.assert_called_with(
            GcsfsFilePath(bucket_name=self.bucket, blob_name=self.relative_file_path),
            dt=datetime.datetime(
                2026, 8, 19, 14, 5, 3, 716152, tzinfo=datetime.timezone.utc
            ),
            normalized_from_file_name=self.relative_file_path,
        )

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_raise_unexpected_error(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
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

    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.cloud_functions_log"
    )
    def test_missing_bucket_or_name(self, mock_logging: MagicMock) -> None:
        bad_event = self._build_pubsub_cloudevent(file_name="")

        normalize_filename(bad_event)

        mock_logging.assert_called_once()

    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.GcsfsPath.from_bucket_and_blob_name"
    )
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.cloud_functions_log"
    )
    def test_incorrect_path_type(
        self,
        mock_logging: MagicMock,
        mock_path: MagicMock,
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
        zip_event = self._build_pubsub_cloudevent(
            file_name=f"{self.relative_file_path}.zip"
        )

        normalize_filename(zip_event)

        mock_invoke_zip_handler.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_called_once()

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
        mock_requests.post.return_value.status_code = 200
        zip_event = self._build_pubsub_cloudevent(
            file_name=f"unprocessed{self.relative_file_path}.zip"
        )

        normalize_filename(zip_event)

        mock_requests.post.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch("recidiviz.cloud_functions.ingest_filename_normalization.requests")
    @patch("recidiviz.cloud_functions.ingest_filename_normalization._get_access_token")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=True,
    )
    def test_raises_zipfile_error(
        self,
        _mock_is_normalized: MagicMock,
        _mock_get_token: MagicMock,
        mock_requests: MagicMock,
        _mock_fs: MagicMock,
    ) -> None:
        mock_requests.post.return_value.status_code = 504
        zip_event = self._build_pubsub_cloudevent(
            file_name=f"unprocessed{self.relative_file_path}.zip"
        )

        with self.assertRaises(RuntimeError):
            normalize_filename(zip_event)

        mock_requests.post.assert_called_once()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=True,
    )
    @patch("recidiviz.cloud_functions.ingest_filename_normalization.requests.post")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.google.oauth2.id_token.fetch_id_token"
    )
    def test_file_already_normalized(
        self,
        _mock_fetch_id_token: MagicMock,
        _mock_post: MagicMock,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        _mock_post.return_value.status_code = 200
        zip_event = self._build_pubsub_cloudevent(
            file_name=f"processed{self.relative_file_path}.zip"
        )

        normalize_filename(zip_event)

        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_us_nyc_timestamped_name_is_cleaned_and_dated(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        nyc_bucket = "recidiviz-test-direct-ingest-state-us-nyc"
        event = self._build_pubsub_cloudevent(
            file_name="PIC_FEED_081920260030.csv",
            bucket=nyc_bucket,
            custom_time="2026-08-19T14:05:03.716152Z",
        )

        normalize_filename(event)

        # The file is moved to a normalized path built from the stripped name, with
        # update_datetime set to the object's preserved creation time (customTime), not
        # the filename or now.
        mock_fs.mv.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_called_once_with(
            GcsfsFilePath(
                bucket_name=nyc_bucket, blob_name="PIC_FEED_081920260030.csv"
            ),
            dt=datetime.datetime(
                2026, 8, 19, 14, 5, 3, 716152, tzinfo=datetime.timezone.utc
            ),
            normalized_from_file_name="PIC_FEED.csv",
        )

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_us_nyc_name_without_timestamp_is_normalized(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        nyc_bucket = "recidiviz-test-direct-ingest-state-us-nyc"
        event = self._build_pubsub_cloudevent(
            file_name="PIC_FEED.csv", bucket=nyc_bucket
        )

        normalize_filename(event)

        mock_fs.mv.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_called_once_with(
            GcsfsFilePath(bucket_name=nyc_bucket, blob_name="PIC_FEED.csv"),
            dt=None,
            normalized_from_file_name="PIC_FEED.csv",
        )

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    @patch(
        "recidiviz.cloud_functions.ingest_filename_normalization.DirectIngestGCSFileSystem.is_normalized_file_path",
        return_value=False,
    )
    def test_non_nyc_timestamped_name_is_not_cleaned(
        self,
        _mock_is_normalized: MagicMock,
        mock_fs: MagicMock,
    ) -> None:
        # A different state has no cleaner, so a 12-digit-suffixed name is
        # normalized as-is rather than stripped.
        event = self._build_pubsub_cloudevent(file_name="PIC_FEED_081420261204.csv")

        normalize_filename(event)

        mock_fs.mv.assert_not_called()
        mock_fs.mv_raw_file_to_normalized_path.assert_called_once_with(
            GcsfsFilePath(
                bucket_name=self.bucket, blob_name="PIC_FEED_081420261204.csv"
            ),
            dt=None,
            normalized_from_file_name="PIC_FEED_081420261204.csv",
        )


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
        mock_fs.gunzip.assert_not_called()
        mock_fs.mv.assert_called_once()
        mock_fs.mv_raw_file_to_normalized_path.assert_not_called()

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    def test_gz_file_handling(self, mock_fs: MagicMock) -> None:
        """A .gz file is decompressed rather than unzipped."""
        self.mock_request.get_json.return_value = {
            "bucket": "recidiviz-test-direct-ingest-state-us-xx",
            "name": "test_file.csv.gz",
        }

        handle_zipfile(self.mock_request)

        mock_fs.gunzip.assert_called_once()
        mock_fs.unzip.assert_not_called()
        mock_fs.mv.assert_called_once()

    def test_normalized_gz_name_is_recognized_by_the_real_parser(self) -> None:
        """A normalized .gz name must be seen as normalized, or normalize_filename would
        rename it again on every object-finalize event and never reach decompression.

        Deliberately uses the real is_normalized_file_path rather than mocking it.
        """
        normalized_gz = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-test-direct-ingest-state-us-xx/"
            "unprocessed_2026-08-07T04:50:03:716152_raw_test_file.csv.gz"
        )
        self.assertTrue(
            DirectIngestGCSFileSystem.is_normalized_file_path(normalized_gz)
        )
        self.assertTrue(normalized_gz.has_gz_extension)
        self.assertTrue(normalized_gz.has_compressed_extension)

        unnormalized_gz = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-test-direct-ingest-state-us-xx/test_file.csv.gz"
        )
        self.assertFalse(
            DirectIngestGCSFileSystem.is_normalized_file_path(unnormalized_gz)
        )

    @patch("recidiviz.cloud_functions.ingest_filename_normalization.fs")
    def test_uncompressed_file_is_left_alone(self, mock_fs: MagicMock) -> None:
        self.mock_request.get_json.return_value = {
            "bucket": "recidiviz-test-direct-ingest-state-us-xx",
            "name": "test_file.csv",
        }

        handle_zipfile(self.mock_request)

        mock_fs.gunzip.assert_not_called()
        mock_fs.unzip.assert_not_called()
        mock_fs.mv.assert_not_called()
