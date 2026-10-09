# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Input data models for sentence calculator."""

import abc
from abc import ABCMeta
from collections.abc import Sequence
from datetime import date
from enum import Enum, EnumMeta

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.recidiviz_attr_validators import is_state_code


@attr.define(frozen=True, kw_only=True)
class Term:
    """A sentence term expressed in years, months, and days.

    Used for min_term and max_term fields in Sentence. The conversion to days
    is either directly computed from the calendar or handled by the calculator
    engine using the formula:
        total_days = (years * days_per_year) + (months * days_per_month) + days
    where days_per_year and days_per_month are configured for the calculation
    regime. Whether to use flat days-per-month/year or calendar days is also
    configured per regime.
    """

    years: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    months: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)
    days: int = attr.ib(default=0, validator=attr_validators.is_non_negative_int)


class _ABCEnumMeta(ABCMeta, EnumMeta):
    """Metaclass combining ABCMeta and EnumMeta.

    `enum.Enum` uses `EnumMeta`, and `abc.ABC` uses `ABCMeta`; a class can't
    inherit from bases with unrelated metaclasses, so OffenseCategory and
    OffenseGroup below use this combined metaclass to be both at once.
    """


class OffenseCategory(Enum, metaclass=_ABCEnumMeta):
    """Abstract base for state-specific offense category enums.

    Defines no members itself. Each state supplies its own concrete category
    values.
    """


class OffenseGroup(Enum, metaclass=_ABCEnumMeta):
    """Abstract base for state-specific offense group enums.

    Defines no members itself. Each state supplies its own concrete group
    values.
    """


class SentenceType(Enum):
    """Type of sentence.

    Currently this only captures the 'exceptional' types. It might later
    track determinate vs indeterminate sentences."""

    DEATH = "DEATH"
    LWO = "LWO"  # Life without possibility of parole
    LIFE = "LIFE"
    OTHER = "OTHER"  # A determinate or indeterminate sentence shorter than life


@attr.define(frozen=True, kw_only=True)
class Sentence(abc.ABC):
    """A single sentence node in a sentence calculation DAG.

    This represents one sentence with its term lengths, offense characteristics,
    and relationship to other consecutive sentences via parent_sentence_id.

    Terms on indeterminate sentences can be specified using the Term structure
    (years/months/days) via min_term and max_term fields.

    Fields below are common to every state's sentences and are defined
    directly on this class. Fields whose concrete type varies by state (e.g.
    offense_category, offense_group) are left abstract here and must be
    supplied by a state-specific subclass, since only that subclass knows
    which concrete enum type applies. Use create_sentence from
    recidiviz.sentence_calculator.models.state_inputs to build the right
    subclass for a given state code.
    """

    sentence_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier that uniquely identifies this sentence within a calculation."""

    min_term: Term = attr.ib(validator=attr.validators.instance_of(Term))
    """Time until eligible to go before the parole board."""

    max_term: Term = attr.ib(validator=attr.validators.instance_of(Term))
    """Time until the sentence is complete."""

    sentence_type: SentenceType | None = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.in_(SentenceType)),
    )
    """Exceptional sentence type (e.g. life, life without parole, death), if any."""

    start_date: date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    """Date this sentence begins, if known."""

    parent_sentence_id: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    """ID of the sentence this sentence runs consecutive to. None if it's a root sentence."""

    initial_time_served: Term = attr.ib(
        factory=Term, validator=attr.validators.instance_of(Term)
    )
    """Credit for time already served prior to this sentence (jail time).
    Converted to days using the regime's conversion settings (fixed or
    calendar-based). (The factory ensures this instance isn't reused.)"""

    @property
    @abc.abstractmethod
    def offense_category(self) -> OffenseCategory:
        """Severity category of the offense underlying this sentence."""

    @property
    @abc.abstractmethod
    def offense_group(self) -> OffenseGroup:
        """Offense type grouping (e.g. violence, sex) for this sentence."""


@attr.define(frozen=True, kw_only=True)
class SentenceCalculationInput:
    """Used by the calculator to determine the path through the sentence graph
    that defines the end date.
    """

    state_code: StateCode = attr.ib(validator=is_state_code)
    # Must be a Sequence because list[ArbitraryStateSentence] isn't a
    # subtype of list[Sentence]
    sentences: Sequence[Sentence] = attr.ib(
        validator=attr_validators.is_sequence_of(Sentence)
    )
    # Regime ID is used to load the regime rules in the calculator.
    law_regime_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
