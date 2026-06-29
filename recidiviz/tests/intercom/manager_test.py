# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for IntercomAPIManager"""

import io
import os
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from recidiviz.intercom.client import IntercomAPIClient
from recidiviz.intercom.manager import (
    UPDATE_DATETIME,
    IntercomAPIManager,
    extract_base_name,
)
from recidiviz.intercom.types import IntercomExportJobResponse, IntercomJobStatus


class TestIntercomAPIManager(unittest.TestCase):
    """Tests IntercomAPIManager methods error handling."""

    def setUp(self) -> None:
        self.mock_client = MagicMock(spec=IntercomAPIClient)

        self.intercom_api_manager = IntercomAPIManager(
            client=self.mock_client, execution_datetime=datetime.now(timezone.utc)
        )

        self.job_identifier = "orzzsbd7hk67xyu"
        self.sleep_patcher = patch("time.sleep", return_value=None)
        self.sleep_patcher.start()

    def tearDown(self) -> None:
        self.sleep_patcher.stop()

    def helper_create_export_info(
        self, status: IntercomJobStatus
    ) -> IntercomExportJobResponse:
        """
        Returns an IntercomExportJobResponse to be passed into the tested methods.
        """
        return IntercomExportJobResponse(
            job_identifier="orzzsbd7hk67xyu",
            status=status,
            download_url="example.com",
            download_expires_at="1674917488",
        )

    def test_poll_export_status(self) -> None:
        """
        Test that IntercomAPIManager.poll_export_status() properly returns an export job.
        """
        export_info = self.helper_create_export_info(status=IntercomJobStatus.COMPLETED)
        self.mock_client.get_export_status.return_value = export_info

        assert (
            self.intercom_api_manager.poll_export_status(self.job_identifier)
            == export_info
        )

    def test_poll_export_status_value_error(self) -> None:
        """
        Test that IntercomAPIManager.poll_export_status() properly raises ValueError.
        """
        export_info = self.helper_create_export_info(status=IntercomJobStatus.FAILED)
        self.mock_client.get_export_status.return_value = export_info

        with self.assertRaises(ValueError) as context:
            self.intercom_api_manager.poll_export_status(
                job_identifier=export_info.job_identifier
            )

        self.assertIn(
            f"Export job [{self.job_identifier}] failed", str(context.exception)
        )

    def test_poll_export_status_timeout_error(self) -> None:
        """
        Test that IntercomAPIManager.poll_export_status() properly raises TimeoutError.
        """

        with self.assertRaises(TimeoutError) as context:
            self.intercom_api_manager.poll_export_status(
                job_identifier=self.job_identifier, max_attempts=5
            )

        self.assertIn(
            f"Export job [{self.job_identifier}] did not complete within 50 seconds",
            str(context.exception),
        )

    def test_download_and_process_export(self) -> None:
        """
        Test IntercomAPIManager.download_and_process_export() properly downloads, names and stores
        files in the temporary directory with the correct update datetime.
        """
        csv_files = {
            "receipt_20260114.csv": "id,event\n1,opened\n2,clicked\n",
            "hard_bounce_20260114.csv": "id,reason\n10,mailbox_full\n",
        }
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w") as zip_file:
            for filename, contents in csv_files.items():
                zip_file.writestr(filename, contents)
        self.mock_client.download_export_data.return_value = zip_buffer.getvalue()

        with tempfile.TemporaryDirectory() as output_dir:
            file_paths = self.intercom_api_manager.download_and_process_export(
                job_identifier=self.job_identifier, output_dir=output_dir
            )

            self.assertEqual(
                {
                    "receipt": os.path.join(output_dir, "receipt_20260114.csv"),
                    "hard_bounce": os.path.join(output_dir, "hard_bounce_20260114.csv"),
                },
                file_paths,
            )

            expected_update_datetime = pd.Timestamp(
                self.intercom_api_manager.execution_datetime
            )
            for base_name, path in file_paths.items():
                df = pd.read_csv(path, parse_dates=[UPDATE_DATETIME])
                self.assertIn(UPDATE_DATETIME, df.columns)
                self.assertTrue(
                    (df[UPDATE_DATETIME] == expected_update_datetime).all(),
                    msg=f"{base_name} has unexpected {UPDATE_DATETIME} values: {df[UPDATE_DATETIME].tolist()}",
                )

    def test_download_and_process_export_fails(self) -> None:
        """
        Test IntercomAPIManager.download_and_process_export() error handling to ensure
        that it properly raises HTTP status errors from IntercomAPIClient.download_export_data()
        """

        self.mock_client.download_export_data.side_effect = (
            requests.exceptions.HTTPError("Simulated 500 Server Error")
        )

        with self.assertRaises(requests.exceptions.HTTPError):
            self.intercom_api_manager.download_and_process_export(
                job_identifier=self.job_identifier,
                output_dir="",
            )

    def test_extract_base_name_single_word(self) -> None:
        """
        Test extract_base_name() properly extracts a single-word base name
        of an Intercom export CSV filename.
        """
        filename = "receipt_20251121.csv"

        base_name = extract_base_name(filename)

        assert base_name == "receipt"

    def test_extract_base_name_multi_word(self) -> None:
        """
        Test extract_base_name() properly extracts a multi-word base name
        of an Intercom export CSV filename.
        """
        filename = "hard_bounce_20251121.csv"

        base_name = extract_base_name(filename)

        assert base_name == "hard_bounce"

    def test_extract_base_name_value_error(self) -> None:
        """
        Test extract_base_name() properly raises ValueError.
        """
        filename = "20251121_goal_success.csv"

        with self.assertRaises(ValueError) as context:
            extract_base_name(filename=filename)

        self.assertIn(
            f"Filename [{filename}] does not match expected Intercom export",
            str(context.exception),
        )
