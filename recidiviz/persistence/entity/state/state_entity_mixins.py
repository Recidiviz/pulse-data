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
"""Mixin classes for entities in the state dataset."""
from typing import List, Optional, TypeVar

import attr

from recidiviz.common import attr_validators
from recidiviz.common.date import DateOrDateTime, assert_datetime_less_than


@attr.s(eq=False)
class StateEntityMixin:
    """Set of attributes and methods that we expect all entities to have in the state and normalized_state datasets."""

    # TODO(#5508) Change type to StateCode. We can use a converter
    # function here to still take strings from ingest views.
    state_code: str = attr.ib(validator=attr_validators.is_str)


@attr.s(eq=False)
class SequencedEntityMixin:
    """Set of attributes for an entity that can be ordered in a sequence."""

    # TODO(#32385): Enforce that this field is nonnull for NormalizedStateEntity
    #  entities.
    sequence_num: int | None = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )


SequencedEntityMixinT = TypeVar("SequencedEntityMixinT", bound=SequencedEntityMixin)


class LedgerEntityMixin(SequencedEntityMixin):
    """Mixin interface for 'ledgers' — periods of time relating back to another entity, where we
    initially have the start date/datetime for each period.

    Ledger entities must have a date/datetime field for the 'start' of the ledger that cannot be in the future.
    They may also have:
       - one or more pairs of date/datetime fields, where the first of the pair must be before the second
       - a string that uniquely identifies this entity when combined with the datetime field and sequence number
    """

    @property
    def ledger_datetime_field(self) -> DateOrDateTime:
        """A ledger entity has a single field denoting a 'start' or 'update' of its period of time. Return it here."""
        raise NotImplementedError("Must define a start datetime field")

    @property
    def ledger_partition_columns(self) -> List[str]:
        """A list of field names that uniquely partition this entity before sorting.
        These fields will be combined with ledger_datetime_field and sequence_num so that we can correctly
        determine end dates for each ledger entity in a later normalization step.
        """
        return []

    @property
    def partition_key(self) -> str:
        """Builds a string to uniquely identify this entity."""
        partition_string = "-".join(
            str(getattr(self, field)) for field in self.ledger_partition_columns
        )
        return f"{self.ledger_datetime_field.isoformat()}-{self.sequence_num}-{partition_string}"

    def assert_datetime_less_than(
        self,
        before: Optional[DateOrDateTime],
        after: Optional[DateOrDateTime],
        before_description: str = "",
        after_description: str = "",
    ) -> None:
        """Raises a ValueError if the given "before" date/datetime is after the "after" one.
        Both field names must be datetime.datetime or datetime.date fields.
        """
        try:
            assert_datetime_less_than(before, after)
        # The class name is helpful in ingest, so catching and re-raising with that info.
        except ValueError as exc:
            _pii = self.limited_pii_repr() if hasattr(self, "limited_pii_repr") else ""
            raise ValueError(
                f"Found {self.__class__.__name__} {_pii} with "
                f"{before_description} datetime {before} after {after_description} datetime {after}."
            ) from exc


LedgerEntityMixinT = TypeVar("LedgerEntityMixinT", bound=LedgerEntityMixin)
