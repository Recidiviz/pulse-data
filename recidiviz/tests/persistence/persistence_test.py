# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Tests for persistence."""

from datetime import datetime
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.persistence.persistence import Persistence
from recidiviz.tests.utils import fakes


SURNAME_1 = 'test_surname_1'
SURNAME_2 = 'test_surname_2'
BIRTHDATE_1 = datetime(year=1993, month=11, day=15)
BIRTHDATE_2 = datetime(year=1996, month=2, day=11)


class TestPersistence(object):
    """Test that the persistence layer correctly writes to the SQL database."""

    def setup_method(self, _test_method):
        self.fake_database, _ = fakes.create_in_memory_sqlite_database()

    def test_twoDifferentPeople_persistsBoth(self):
        # Arrange
        persistence = Persistence(self.fake_database)

        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1)
        ingest_info.create_person(surname=SURNAME_2)

        # Act
        persistence.write(ingest_info)
        result = self.fake_database.read_people()

        # Assert
        assert len(result) == 2
        assert result[0].surname == SURNAME_1
        assert result[1].surname == SURNAME_2

    def test_sameTwoPeople_persistsOne(self):
        # Arrange
        persistence = Persistence(self.fake_database)

        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1)
        ingest_info.create_person(surname=SURNAME_1)

        # Act
        persistence.write(ingest_info)
        result = self.fake_database.read_people()

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1

    def test_readSinglePersonByName(self):
        # Arrange
        persistence = Persistence(self.fake_database)

        ingest_info = IngestInfo()
        ingest_info.create_person(surname=SURNAME_1, birthdate=BIRTHDATE_1)
        ingest_info.create_person(surname=SURNAME_2, birthdate=BIRTHDATE_2)

        # Act
        persistence.write(ingest_info)
        result = self.fake_database.read_people(surname=SURNAME_1)

        # Assert
        assert len(result) == 1
        assert result[0].surname == SURNAME_1
        assert result[0].birthdate == BIRTHDATE_1
