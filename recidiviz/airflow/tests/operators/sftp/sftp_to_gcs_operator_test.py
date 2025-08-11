# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for the SftpToGcsOperator"""
import datetime
import unittest
from unittest.mock import MagicMock, create_autospec, patch

import pytz
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator import (
    RecidivizSftpToGcsOperator,
)
from recidiviz.airflow.tests.operators.sftp.sftp_test_utils import (
    FakeSftpDownloadDelegateFactory,
    FakeUsXxSftpDownloadDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


@patch.object(
    SftpDownloadDelegateFactory, "build", return_value=FakeUsXxSftpDownloadDelegate()
)
class TestSftpToGcsOperatorProperty(unittest.TestCase):
    """Tests for the SftpToGcsOperator"""

    def test_creates_correct_download_path(
        self, _mock_sftp_delegate: MagicMock
    ) -> None:
        operator = RecidivizSftpToGcsOperator(
            task_id="test-task",
            project_id="recidiviz-testing",
            region_code="US_XX",
            remote_file_path="outside_folder/inside_folder/file.txt",
            sftp_timestamp=int(
                datetime.datetime(2023, 1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).timestamp()
            ),
        )
        expected_file = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name="recidiviz-testing-direct-ingest-state-us-xx-sftp",
            blob_name="2023-01-01T01:00:00:000000/inside_folder/file.txt",
        )
        self.assertEqual(
            expected_file.abs_path(), operator.build_download_path().abs_path()
        )

    def test_creates_correct_download_path_with_dash(
        self, _mock_sftp_delegate: MagicMock
    ) -> None:
        operator = RecidivizSftpToGcsOperator(
            task_id="test-task",
            project_id="recidiviz-testing",
            region_code="US_XX",
            remote_file_path="outside_folder/inside-folder-with-dash/file-with-dash.txt",
            sftp_timestamp=int(
                datetime.datetime(2023, 1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).timestamp()
            ),
        )
        expected_file = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name="recidiviz-testing-direct-ingest-state-us-xx-sftp",
            blob_name="2023-01-01T01:00:00:000000/inside-folder-with-dash/file-with-dash.txt",
        )
        self.assertEqual(
            expected_file.abs_path(), operator.build_download_path().abs_path()
        )


class TestSftpToGcsOperator(unittest.TestCase):
    """Tests for the SftpToGcsOperator"""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()
        self.gcsfs_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.get_gcsfs_from_hook",
            return_value=self.fs,
        )
        self.gcsfs_patcher.start()
        self.hook_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.RecidivizSFTPHook",
        )
        self.hook_mock = self.hook_patcher.start()
        self.delegate_factory_patcher = patch(
            "recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator.SftpDownloadDelegateFactory",
            FakeSftpDownloadDelegateFactory,
        )
        self.delegate_factory_patcher.start()
        self.mock_context = create_autospec(Context)

    def tearDown(self) -> None:
        self.gcsfs_patcher.stop()
        self.hook_patcher.stop()
        self.delegate_factory_patcher.stop()

    def test_remove_files_called(self) -> None:
        operator = RecidivizSftpToGcsOperator(
            task_id="test-task",
            project_id="recidiviz-testing",
            region_code="US_LL",
            remote_file_path="outside_folder/inside-folder-with-dash/file-with-dash.txt",
            sftp_timestamp=int(
                datetime.datetime(2023, 1, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).timestamp()
            ),
        )
        operator.execute(self.mock_context)

        self.hook_mock().get_conn().remove.assert_called_once_with(
            "outside_folder/inside-folder-with-dash/file-with-dash.txt"
        )
