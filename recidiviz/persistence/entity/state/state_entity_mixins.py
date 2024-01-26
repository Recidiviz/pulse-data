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
import abc
import datetime
from typing import Optional, TypeVar, Union

import attr

from recidiviz.common import attr_validators
from recidiviz.common.date import assert_datetime_less_than
from recidiviz.persistence.entity.state.entity_field_validators import pre_norm_opt


@attr.s(eq=False)
class SequencedEntityMixin:
    """Set of attributes for a normalized entity that can be ordered in a sequence."""

    sequence_num: Optional[int] = attr.ib(
        default=None, validator=pre_norm_opt(attr_validators.is_int)
    )


SequencedEntityMixinT = TypeVar("SequencedEntityMixinT", bound=SequencedEntityMixin)


class LedgerEntityMixin(SequencedEntityMixin):
    """Mixin interface for 'ledgers' — periods of time relating back to another entity, where we
    initially have the start date/datetime for each period.

    Ledger entities must have a date/datetime field for the 'start' of the ledger that cannot be in the future.
    They may also have one or more pairs of date/datetime fields, where the first date/datetime of the pair
    must be before the second.
    """

    @classmethod
    @abc.abstractmethod
    def get_ledger_datetime_field(cls) -> str:
        """A ledger entity has a single field denoting a 'start' or 'update' of its period of time. Return it here."""
        raise NotImplementedError("Must define a start datetime field")

    def assert_datetime_less_than(
        self,
        before: Optional[Union[datetime.date, datetime.datetime]],
        after: Optional[Union[datetime.date, datetime.datetime]],
    ) -> None:
        """Raises a ValueError if the given "before" date/datetime is after the "after" one.
        Both field names must be datetime.datetime or datetime.date fields.
        """
        try:
            assert_datetime_less_than(before, after)
        # The class name is helpful in ingest, so catching and re-raising with that info.
        except ValueError as exc:
            raise ValueError(
                f"Found {self.__class__.__name__} with datetime {before} after datetime {after}."
            ) from exc
