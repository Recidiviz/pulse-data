# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for scraped_data"""
from unittest import TestCase

from recidiviz.common.constants.person_characteristics import Race
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape.errors import ScraperError
from recidiviz.ingest.scrape.task_params import ScrapedData, SingleCount


class TestScrapedData(TestCase):
    """Test that scraped data construction is validated."""

    def testRaiseWithNothing(self) -> None:
        with self.assertRaises(ScraperError):
            ScrapedData(ingest_info=None, persist=True)

    def testIngestInfo(self) -> None:
        ScrapedData(
            ingest_info=IngestInfo(people=[Person(race=Race("ASIAN"))]), persist=True
        )

    def testSingleCount(self) -> None:
        ScrapedData(single_counts=[SingleCount(count=123)], persist=True)

    def testBoth(self) -> None:
        ScrapedData(
            ingest_info=IngestInfo(people=[Person(race=Race("ASIAN"))]),
            single_counts=[SingleCount(count=123)],
            persist=True,
        )
