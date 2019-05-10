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
"""Tests for the direct ingest parser.py."""
import datetime
from unittest import TestCase

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.collector import DirectIngestCollector
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest

_ROSTER_JSON_PATH = fixtures.as_filepath('roster.json')
_FAKE_START_TIME = datetime.datetime(year=2019, month=1, day=2)


class UsMaMiddlesexDirectIngestParser(IndividualIngestTest, TestCase):
    def testParse(self):
        direct = DirectIngestCollector('us_ma_middlesex')

        metadata = IngestMetadata(
            direct.region.region_code,
            direct.region.jurisdiction_id,
            _FAKE_START_TIME,
            direct.region.get_enum_overrides())

        with open(_ROSTER_JSON_PATH) as fp:
            ingest_info = direct.parse(fp)

        expected_info = IngestInfo(
            people=[
                Person(
                    birthdate='1979',
                    gender='Female',
                    race='White',
                    bookings=[
                        Booking(
                            admission_date='2019-01-02')
                    ]),
                Person(
                    birthdate='1990',
                    gender='Male',
                    race='Other',
                    bookings=[
                        Booking(
                            admission_date='2019-02-01')])])
        self.validate_ingest(ingest_info, expected_info, metadata)
