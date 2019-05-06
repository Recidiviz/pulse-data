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
# ============================================================================
"""Converts an ingest_info proto Person to a persistence entity."""

import json
from typing import Optional

import attr

from recidiviz.common.constants.person import (RESIDENCY_STATUS_SUBSTRING_MAP,
                                               Ethnicity, Gender, Race,
                                               ResidencyStatus)
from recidiviz.common.str_field_utils import parse_date, normalize
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    fn, parse_external_id, calculate_birthdate_from_age
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import \
    EnumMappings


def copy_fields_to_builder(state_person_builder, proto, metadata):
    """Mutates the provided |state_person_builder| by converting an
    ingest_info proto Person.

    Note: This will not copy children into the Builder!
    """
    enum_fields = {
        'gender': Gender,
        'race': Race,
        'ethnicity': Ethnicity,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    new = state_person_builder

    # Enum mappings
    new.race = enum_mappings.get(Race)
    new.race_raw_text = fn(normalize, 'race', proto)
    new.ethnicity = enum_mappings.get(Ethnicity)
    new.ethnicity_raw_text = fn(normalize, 'ethnicity', proto)
    new.gender = enum_mappings.get(Gender)
    new.gender_raw_text = fn(normalize, 'gender', proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'state_person_id', proto)
    new.full_name = _parse_name(proto)
    new.birthdate, new.birthdate_inferred_from_age = _parse_birthdate(proto)
    new.residency_status = fn(
        _parse_residency_status, 'place_of_residence', proto)


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
class Names:
    """Holds the various name fields"""
    full_name: Optional[str]
    given_names: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    name_suffix: Optional[str]

    def __attrs_post_init__(self):
        if self.full_name and any((self.given_names, self.middle_names,
                                   self.surname, self.name_suffix)):
            raise ValueError("Cannot have full_name and surname/middle/"
                             "given_names/name_suffix")

        if any((self.middle_names, self.name_suffix)) and \
                not any((self.given_names, self.surname)):
            raise ValueError("Cannot set only middle_names/name_suffix.")

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


def _parse_residency_status(place_of_residence: str) -> ResidencyStatus:
    normalized_place_of_residence = place_of_residence.upper()
    for substring, residency_status in RESIDENCY_STATUS_SUBSTRING_MAP.items():
        if substring in normalized_place_of_residence:
            return residency_status
    # If place of residence is provided and no other status is explicitly
    # provided, assumed to be permanent
    return ResidencyStatus.PERMANENT
