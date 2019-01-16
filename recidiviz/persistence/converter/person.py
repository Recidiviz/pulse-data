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
import csv
import io

from typing import Optional, List

from recidiviz.common.constants.person import Ethnicity, Race, Gender
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import fn, normalize, \
    parse_date, calculate_birthdate_from_age, parse_external_id


def copy_fields_to_builder(person_builder, proto, metadata):
    """Mutates the provided |person_builder| by converting an ingest_info proto
     Person.

     Note: This will not copy children into the Builder!
     """
    new = person_builder

    new.external_id = fn(parse_external_id, 'person_id', proto)
    new.full_name = _parse_name(proto)
    new.birthdate, new.birthdate_inferred_from_age = _parse_birthdate(proto)
    new.race, new.ethnicity = _parse_race_and_ethnicity(proto,
                                                        metadata.enum_overrides)
    new.race_raw_text = fn(normalize, 'race', proto)
    new.ethnicity_raw_text = fn(normalize, 'ethnicity', proto)
    new.gender = fn(Gender.from_str, 'gender', proto, metadata.enum_overrides)
    new.gender_raw_text = fn(normalize, 'gender', proto)
    new.place_of_residence = fn(normalize, 'place_of_residence', proto)

    new.region = metadata.region


def _parse_name(proto) -> Optional[str]:
    """Parses name into a single string."""
    full_name = fn(normalize, 'full_name', proto)
    given_names = fn(normalize, 'given_names', proto)
    middle_names = fn(normalize, 'middle_names', proto)
    surname = fn(normalize, 'surname', proto)

    if full_name and (given_names or middle_names or surname):
        raise ValueError('Cannot have full_name and surname/middle/given_names')

    if full_name:
        return full_name
    if given_names or middle_names or surname:
        return _to_csv([given_names, middle_names, surname])
    return None


def _to_csv(strings: List[str]) -> str:
    """Convert the provided strings to a CSV string.

    Note: We use the csv.writer library to ensure that commas are correctly
    escaped in the event that a comma exists within a provided string.
    """
    string_buffer = io.StringIO()
    csv.writer(string_buffer).writerow(strings)
    return string_buffer.getvalue().strip()


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


def _parse_race_and_ethnicity(proto, enum_overrides):
    if converter_utils.race_is_actually_ethnicity(proto):
        race = None
        ethnicity = fn(Ethnicity.from_str, 'race', proto, enum_overrides)
    else:
        race = fn(Race.from_str, 'race', proto)
        ethnicity = fn(Ethnicity.from_str, 'ethnicity', proto, enum_overrides)

    return race, ethnicity
