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
import json
import re
from typing import Optional

import attr
from uszipcode import SearchEngine

from recidiviz.common.constants.person import (RESIDENCY_STATUS_SUBSTRING_MAP,
                                               Ethnicity, Gender, Race,
                                               ResidencyStatus)
from recidiviz.common.date import parse_date
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import (
    calculate_birthdate_from_age, fn, normalize, parse_external_id)

# Suffixes used in county names in uszipcode library
USZIPCODE_COUNTY_SUFFIXES = [
    'BOROUGH',
    'CENSUS AREA',
    'CITY',
    'CITY AND BOROUGH',
    'COUNTY',
    'MUNICIPIO',
    'PARISH'
]


ZIP_CODE_SEARCH = SearchEngine(simple_zipcode=True)


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
    new.gender = fn(Gender.parse, 'gender', proto, metadata.enum_overrides)
    new.gender_raw_text = fn(normalize, 'gender', proto)
    new.residency_status = fn(
        _parse_residency_status, 'place_of_residence', proto)
    new.resident_of_region = fn(
        _parse_is_resident, 'place_of_residence', proto, metadata.region)

    new.region = metadata.region


def _parse_name(proto) -> Optional[str]:
    """Parses name into a single string."""
    names = Names(
        full_name=fn(normalize, 'full_name', proto),
        given_names=fn(normalize, 'given_names', proto),
        middle_names=fn(normalize, 'middle_names', proto),
        surname=fn(normalize, 'surname', proto),
        name_suffix=fn(normalize, 'name_suffix', proto))
    return names.combine()


@attr.s(auto_attribs=True, frozen=True)
class Names():
    """Holds the various name fields"""
    full_name: Optional[str]
    given_names: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    name_suffix: Optional[str]

    def __attrs_post_init__(self):
        if self.full_name and any((self.given_names, self.middle_names,
                                   self.surname, self.name_suffix)):
            raise ValueError('Cannot have full_name and surname/middle/'
                             'given_names/name_suffix')

        if any((self.middle_names, self.name_suffix)) and \
                not any((self.given_names, self.surname)):
            raise ValueError('Cannot set only middle_names/name_suffix.')

    def combine(self) -> Optional[str]:
        """Writes the names out as a json string, skipping fields that are None.

        Note: We don't have any need for parsing these back into their parts,
        but this gives us other advantages. It handles escaping the names, and
        allows us to add fields in the future without changing the serialization
        of existing names.
        """
        filled_names = attr.asdict(self, filter=lambda a, v: v is not None)
        return json.dumps(filled_names, sort_keys=True) \
               if filled_names else None


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
    if converter_utils.race_is_actually_ethnicity(proto, enum_overrides):
        race = None
        ethnicity = fn(Ethnicity.parse, 'race', proto, enum_overrides)
    else:
        race = fn(Race.parse, 'race', proto, enum_overrides)
        ethnicity = fn(Ethnicity.parse, 'ethnicity', proto, enum_overrides)

    return race, ethnicity


def _parse_is_resident(place_of_residence: str, region: str) -> Optional[bool]:
    """Returns (True) if person is resident of |region|, (False) if they are
    not, and (None) if it cannot be determined
    """

    zip_code = None
    zip_code_matches = re.findall(r'\d{5}', place_of_residence)
    if not zip_code_matches:
        return None
    # If more than one match is present, take the last one (to account for
    # cases where there is a 5-digit address)
    zip_code = zip_code_matches[-1]

    # Region code us_xx is state, us_xx_xxx... is county
    if len(region) == 5:
        return _parse_is_state_resident(zip_code, region)
    return _parse_is_county_resident(zip_code, region)


def _parse_is_county_resident(
        residence_zip_code: str, region: str) -> Optional[bool]:
    # Remove 'us_xx_' prefix
    region_county = region[6:]
    # Replace underscores with spaces because uszipcode uses spaces
    normalized_region_county = region_county.upper().replace('_', ' ')

    residence_county = ZIP_CODE_SEARCH.by_zipcode(residence_zip_code).county
    if not residence_county:
        return None

    # uszipcode county names only contain hyphens and periods as special
    # characters
    normalized_residence_county = \
        residence_county.upper().replace('-', ' ').replace('.', '')

    # Compare region county to base version of residence county, as well as
    # residence county with any matching suffixes stripped
    possible_county_names = {normalized_residence_county}
    for suffix in USZIPCODE_COUNTY_SUFFIXES:
        suffix_length = len(suffix)
        if normalized_residence_county[-(suffix_length):] == suffix:
            possible_county_names.add(
                normalized_residence_county[:-(suffix_length + 1)])
    for county_name in possible_county_names:
        if normalized_region_county == county_name:
            return True
    return False


def _parse_is_state_resident(
        residence_zip_code: str, region: str) -> Optional[bool]:
    region_state_code = region[-2:].upper()
    residence_state_code = \
        ZIP_CODE_SEARCH.by_zipcode(residence_zip_code).state.upper()
    if not residence_state_code:
        return None
    return region_state_code == residence_state_code


def _parse_residency_status(place_of_residence: str) -> ResidencyStatus:
    normalized_place_of_residence = place_of_residence.upper()
    for substring, residency_status in RESIDENCY_STATUS_SUBSTRING_MAP.items():
        if substring in normalized_place_of_residence:
            return residency_status
    # If place of residence is provided and no other status is explicitly
    # provided, assumed to be permanent
    return ResidencyStatus.PERMANENT
