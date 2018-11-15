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
"""Contains logic for communicating with the persistence layer."""


class Persistence(object):
    def __init__(self, database):
        """
        Setup persistence communication using the provided Database.

        :param database: The database to persist into.
        """
        self._database = database

    def write(self, ingest_info):
        """
        Persist each person in the ingest_info. If a person with the given
        surname/birthday already exists, then skip that person.

        :param ingest_info: The IngestInfo containing the surname.
        """
        for person in ingest_info.person:
            person_already_exists = self._database.read_people(person.surname,
                                                               person.birthdate)
            if person_already_exists:
                continue

            self._database.write_person(person.surname, person.birthdate)
