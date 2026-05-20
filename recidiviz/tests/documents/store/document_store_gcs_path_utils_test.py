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
"""Tests for document_store_gcs_path_utils.py."""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
    gcs_path_for_task_output,
)


class TestDocumentStoreGcsPathUtils(unittest.TestCase):
    """Tests for document_store_gcs_path_utils.py."""

    def setUp(self) -> None:
        self.project_id = "recidiviz-testing"
        self.state_code = StateCode.US_XX
        self.run_id = "manual__2026-05-12T17:21:11.122491+00:00"

    def test_gcs_path_for_document(self) -> None:
        path = gcs_path_for_document(self.project_id, self.state_code, "abc123")
        self.assertEqual(
            path.bucket_name, "recidiviz-testing-us-xx-document-blob-storage"
        )
        self.assertEqual(path.blob_name, "abc123.txt")

    def test_gcs_path_for_task_output(self) -> None:
        path = gcs_path_for_task_output(
            self.project_id, self.state_code, self.run_id, 2, 5
        )
        self.assertEqual(
            path.bucket_name,
            "recidiviz-testing-us-xx-temp-document-store-output",
        )
        self.assertEqual(
            path.blob_name, "manual__2026-05-12T17:21:11_122491+00:00/task_2_5.csv"
        )
