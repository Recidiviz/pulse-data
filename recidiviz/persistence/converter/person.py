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
# ============================================================================
"""Converts an ingest_info proto Person to a persistence entity."""
from recidiviz.common.constants.person import Ethnicity, Race, Gender
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import fn, normalize, \
    split_full_name, parse_date, calculate_birthdate_from_age


def copy_fields_to_builder(proto, person_builder):
    """Mutates the provided |person_builder| by converting an ingest_info proto
     Person.

     Note: This will not copy children into the Builder!
     """
    new = person_builder

    new.external_id = fn(normalize, 'person_id', proto)
    new.surname, new.given_names = _parse_name(proto)
    new.birthdate, new.birthdate_inferred_from_age = _parse_birthdate(proto)
    new.race, new.ethnicity = _parse_race_and_ethnicity(proto)
    new.region = None  # TODO: Decide where this should be filled out
    new.gender = fn(Gender.from_str, 'gender', proto)
    new.place_of_residence = fn(normalize, 'place_of_residence', proto)


def _parse_name(proto):
    """Parses name into (surname, given_names)."""
    surname = fn(normalize, 'surname', proto)
    given_names = fn(normalize, 'given_names', proto)
    fullname = fn(split_full_name, 'full_name', proto)

    if (surname is not None or given_names is not None) and \
            fullname is not None:
        raise ValueError('Cannot have full_name and surname/given_names')
    elif fullname is not None:
        return fullname
    else:
        return surname, given_names


def _parse_birthdate(proto):
    parsed_birthdate = None
    parsed_birthdate_is_inferred = None

    birthdate = fn(parse_date, 'birthdate', proto)
    birthdate_inferred_by_age = fn(calculate_birthdate_from_age, 'age', proto)
    if birthdate is not None:
        parsed_birthdate = birthdate
        parsed_birthdate_is_inferred = False
    elif birthdate_inferred_by_age is not None:
        parsed_birthdate = birthdate_inferred_by_age
        parsed_birthdate_is_inferred = True

    return parsed_birthdate, parsed_birthdate_is_inferred


def _parse_race_and_ethnicity(proto):
    if converter_utils.race_is_actually_ethnicity(proto):
        race = None
        ethnicity = fn(Ethnicity.from_str, 'race', proto)
    else:
        race = fn(Race.from_str, 'race', proto)
        ethnicity = fn(Ethnicity.from_str, 'ethnicity', proto)

    return race, ethnicity
