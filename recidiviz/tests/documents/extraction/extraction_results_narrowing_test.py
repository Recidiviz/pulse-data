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
"""Tests for ExtractionResultsNarrowing."""
from unittest import TestCase

from recidiviz.documents.extraction.extraction_results_narrowing import (
    ExtractionResultsNarrowing,
)


class ExtractionResultsNarrowingTest(TestCase):
    """Tests the WHERE clause a narrowing renders."""

    def test_version_only_clause(self) -> None:
        narrowing = ExtractionResultsNarrowing(
            extractor_version_id="abc123", extraction_job_id=None
        )
        self.assertEqual(
            "WHERE extractor_version_id = 'abc123'",
            narrowing.build_where_clause_sql(),
        )

    def test_version_and_job_clause(self) -> None:
        narrowing = ExtractionResultsNarrowing(
            extractor_version_id="abc123", extraction_job_id="def456"
        )
        self.assertEqual(
            "WHERE extractor_version_id = 'abc123'\n"
            "    AND extraction_job_id = 'def456'",
            narrowing.build_where_clause_sql(),
        )

    def test_empty_version_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^String value should not be empty\.$"
        ):
            ExtractionResultsNarrowing(extractor_version_id="", extraction_job_id=None)

    def test_empty_job_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^String value should not be empty\.$"
        ):
            ExtractionResultsNarrowing(
                extractor_version_id="abc123", extraction_job_id=""
            )
