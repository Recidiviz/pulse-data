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
"""Tests for document_upload_status_table.py."""

import unittest
from datetime import datetime, timezone

from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
)
from recidiviz.documents.store.document_upload_status_table import (
    COLLECTION_NAME,
    DOCUMENT_CONTENTS_ID,
    DOCUMENT_UPLOAD_SUCCESS,
    ERROR_MESSAGE,
    RUN_ID,
    STATUS,
    UPLOAD_DATETIME,
    DocumentUploadStatusTable,
)


class TestDocumentUploadStatusTable(unittest.TestCase):
    """Tests for DocumentUploadStatusTable."""

    def test_to_csv_row_matches_schema(self) -> None:
        expected_columns = [
            DOCUMENT_CONTENTS_ID,
            COLLECTION_NAME,
            RUN_ID,
            UPLOAD_DATETIME,
            STATUS,
            DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
            ERROR_MESSAGE,
        ]
        self.assertEqual(DocumentUploadStatusTable.column_names(), expected_columns)

        dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        row = DocumentUploadStatusTable.to_csv_row(
            document_contents_id="abc",
            collection_name="FAKE_CASE_NOTES",
            run_id="run_1",
            upload_datetime=dt,
            status=DOCUMENT_UPLOAD_SUCCESS,
            document_length_bytes=1024,
            error_message=None,
        )
        self.assertEqual(
            row,
            (
                "abc",
                "FAKE_CASE_NOTES",
                "run_1",
                "2026-01-01T00:00:00+00:00",
                DOCUMENT_UPLOAD_SUCCESS,
                1024,
                None,
            ),
        )
        self.assertEqual(len(row), len(expected_columns))
