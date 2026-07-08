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
"""Tests for Intercom outbound content data export CSV header verification"""

import os
import shutil
import tempfile
import unittest

from recidiviz.entrypoints.intercom_outbound_data_export import verify_headers


class TestIntercomCSVHeaderVerification(unittest.TestCase):
    """Tests for Intercom outbound content data export CSV header verification"""

    def setUp(self) -> None:
        self.output_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir)

    def _write_csv(self, filename: str, header_line: str) -> str:
        path = os.path.join(self.output_dir, filename)
        with open(path, mode="w", encoding="utf-8") as f:
            f.write(header_line + "\n")
        return path

    def test_verify_headers(self) -> None:
        """
        Test that verify_headers() properly verifies correct headers.
        """
        file_paths = {
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "receipt_id,hard_bounced_at,update_datetime",
            ),
            "answer": self._write_csv(
                "answer_20260114.csv",
                "receipt_id,answered_at,question_id,question,response_type,response,update_datetime",
            ),
        }

        verify_headers(file_paths)

    def test_verify_headers_key_error(self) -> None:
        """
        Test that verify_headers() properly raises KeyError for CSV files that
        don't match the naming of any of the Intercom YAML files.
        """
        file_paths = {
            "call_back": self._write_csv(
                "call_back_20260114.csv",
                "id,event,update_datetime",
            ),
            "subscribe": self._write_csv(
                "subscribe_20260114.csv",
                "id,reason,update_datetime",
            ),
        }

        with self.assertRaises(KeyError) as context:
            verify_headers(file_paths)

        self.assertIn("No source table config found", str(context.exception))

    def test_verify_headers_column_not_found(self) -> None:
        """
        Test that verify_headers() properly raises ValueError for CSV files with
        headers that don't match the corresponding schema for the BigQuery table.
        """
        file_paths = {
            "receipt": self._write_csv(
                "receipt_20260114.csv",
                "id,event,update_datetime",
            ),
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "id,reason,update_datetime",
            ),
        }

        with self.assertRaises(ValueError) as context:
            verify_headers(file_paths)

        self.assertIn("not found in source table YAML", str(context.exception))

    def test_verify_headers_columns_incorrectly_ordered(self) -> None:
        """
        Test that verify_headers() properly raises ValueError for CSV files with
        headers that don't match the corresponding schema for the BigQuery table.
        """
        file_paths = {
            "hard_bounce": self._write_csv(
                "hard_bounce_20260114.csv",
                "hard_bounced_at,receipt_id,update_datetime",
            ),
            "answer": self._write_csv(
                "answer_20260114.csv",
                "answered_at,receipt_id,question_id,question,response_type,response,update_datetime",
            ),
        }

        with self.assertRaises(ValueError) as context:
            verify_headers(file_paths)

        self.assertIn(
            "CSV header does not match the ordering of the YAML schema fields",
            str(context.exception),
        )
