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
"""Validates that data in converted Entity objects conforms to data assumptions."""

from typing import List, Tuple, Callable

from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_validator.county.county_validator import (
    validate_county_person,
)
from recidiviz.persistence.entity_validator.state.state_validator import (
    validate_state_person,
)


def validate(people: List[EntityPersonType]) -> Tuple[List[EntityPersonType], int]:
    """Validates a list of EntityPersonType entities and returns the valid people and the number of people with
    validation errors.
    """
    data_validation_errors = 0
    validated_people = []
    for person in people:
        validator = _get_validator(person)
        if validator(person):
            validated_people.append(person)
        else:
            data_validation_errors += 1
    return validated_people, data_validation_errors


def _get_validator(person: EntityPersonType) -> Callable[..., bool]:
    if isinstance(person, county_entities.Person):
        return validate_county_person

    if isinstance(person, state_entities.StatePerson):
        return validate_state_person

    raise ValueError(
        f"StatePerson entity to validate was not of expected type "
        f"county_entities.Person or state_entities.StatePerson "
        f"but [{person.__class__.__name__}]"
    )
