# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Python representations of the tables in our state schema which represent data once it
has been run through the normalization portions of our pipelines.
"""
from datetime import date
from typing import Any, List, Optional, Type

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.state.state_person import (
    StateGender,
    StateResidencyStatus,
)
from recidiviz.persistence.entity.base_entity import (
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)


##### VALIDATORS #####
def validate_person_backedge(entity_cls: Type, person: Any) -> None:
    if person is None:
        return
    if not isinstance(person, NormalizedStatePerson):
        raise ValueError(
            f"Found person set on class {entity_cls.__name__} with incorrect type "
            f"[{type(person)}]"
        )


@attr.s(eq=False, kw_only=True)
class NormalizedStatePersonExternalId(NormalizedStateEntity, ExternalIdEntity):
    """Models an external id associated with a particular StatePerson."""

    # State Code
    # State providing the external id
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Primary key
    person_external_id_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    person: Optional["NormalizedStatePerson"] = attr.ib(default=None)

    @person.validator
    def _person_validator(self, _attribute: attr.Attribute, person: Any) -> None:
        validate_person_backedge(type(self), person)

    @classmethod
    def global_unique_constraints(cls) -> List[UniqueConstraint]:
        return [
            UniqueConstraint(
                name="person_external_ids_unique_within_type_and_region",
                fields=[
                    "state_code",
                    "id_type",
                    "external_id",
                ],
            )
        ]


@attr.s(eq=False, kw_only=True)
class NormalizedStatePerson(
    NormalizedStateEntity,
    HasMultipleExternalIdsEntity[NormalizedStatePersonExternalId],
    RootEntity,
):
    """Models a StatePerson moving through the criminal justice system."""

    # State Code
    state_code: str = attr.ib(validator=attr_validators.is_str)

    # Attributes

    #   - Where
    current_address: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    #   - What
    full_name: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)

    birthdate: Optional[date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    gender: Optional[StateGender] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateGender)
    )
    gender_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # NOTE: This may change over time - we track these changes in history tables
    residency_status: Optional[StateResidencyStatus] = attr.ib(
        default=None, validator=attr_validators.is_opt(StateResidencyStatus)
    )
    residency_status_raw_text: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    current_email_address: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    current_phone_number: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Primary key
    person_id: int = attr.ib(validator=attr_validators.is_int)

    # Cross-entity relationships
    external_ids: List["NormalizedStatePersonExternalId"] = attr.ib(
        validator=attr_validators.is_list_of(NormalizedStatePersonExternalId),
    )

    # TODO(#30075): Add remainder of child entity relationship lists as we introduce new
    #  entities.

    def get_external_ids(self) -> list["NormalizedStatePersonExternalId"]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "person"
