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
"""Tests for run_sandbox_extraction_pipeline_phase.py."""
import unittest

from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_extraction_pipeline_phase import (
    Phase,
    _wrap_query_with_segment,
)


class TestWrapQueryWithSegment(unittest.TestCase):
    def test_basic_wrapping(self) -> None:
        query = "SELECT document_id FROM my_table"
        wrapped = _wrap_query_with_segment(query, segment_index=3, total_segments=10)
        self.assertIn("FARM_FINGERPRINT(document_id)", wrapped)
        self.assertIn("MOD(", wrapped)
        self.assertIn(", 10) = 3", wrapped)
        # Original query is a subquery
        self.assertIn("SELECT document_id FROM my_table", wrapped)

    def test_segment_zero(self) -> None:
        wrapped = _wrap_query_with_segment("SELECT * FROM t", 0, 4)
        self.assertIn("MOD(ABS(FARM_FINGERPRINT(document_id)), 4) = 0", wrapped)

    def test_single_segment(self) -> None:
        wrapped = _wrap_query_with_segment("SELECT * FROM t", 0, 1)
        self.assertIn("MOD(ABS(FARM_FINGERPRINT(document_id)), 1) = 0", wrapped)


class TestPhaseEnum(unittest.TestCase):
    def test_all_phases_present(self) -> None:
        phases = [p.value for p in Phase]
        self.assertIn("DOC_UPLOAD", phases)
        self.assertIn("EXTRACTION", phases)
        self.assertIn("VIEW_DEPLOY", phases)
        self.assertEqual(len(phases), 3)
