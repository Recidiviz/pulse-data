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
"""Nevada-specific Sentence input models."""

from datetime import date
from functools import total_ordering

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.inputs import (
    OffenseCategory,
    OffenseGroup,
    Sentence,
)
from recidiviz.utils.types import assert_type


class NevadaOffenseCategory(OffenseCategory):
    """
    Categories are buckets that give a rough measure of the severity of an
    offense. A is the most severe, E the least.
    """

    CATEGORY_A = "A"
    CATEGORY_B = "B"
    CATEGORY_C = "C"
    CATEGORY_D = "D"
    CATEGORY_E = "E"

    UNKNOWN = "UNKNOWN"


class NevadaOffenseGroup(OffenseGroup):
    """Groups relate to the type of offense."""

    # We only need to check against VIO and SEX for computing sentence dates
    VIOLENCE = "VIOLENCE"
    SEX = "SEX"
    PROPERTY = "PROPERTY"
    DRUG = "DRUG"
    # "Other" is literally one of their categories
    OTHER = "OTHER"

    # For fields that are present but not one of the above known types
    UNKNOWN = "UNKNOWN"


# Ranked most to least severe. UNKNOWN is least severe since it carries no severity
# information. This ordering is used to compare sentence severity.
NEVADA_OFFENSE_CATEGORY_SEVERITY_ORDER: list[NevadaOffenseCategory] = [
    NevadaOffenseCategory.CATEGORY_A,
    NevadaOffenseCategory.CATEGORY_B,
    NevadaOffenseCategory.CATEGORY_C,
    NevadaOffenseCategory.CATEGORY_D,
    NevadaOffenseCategory.CATEGORY_E,
    NevadaOffenseCategory.UNKNOWN,
]


def nevada_category_severity_rank(category: OffenseCategory) -> int:
    """Returns the severity rank of a Nevada offense category: lower is more severe."""
    return NEVADA_OFFENSE_CATEGORY_SEVERITY_ORDER.index(
        assert_type(category, NevadaOffenseCategory)
    )


# In actuality, SEX is more severe than VIO, but in practice this doesn't make any
# difference. This makes the distinction between severe / not severe easier to see.
NEVADA_MORE_SEVERE_OFFENSE_GROUPS: set[NevadaOffenseGroup] = {
    NevadaOffenseGroup.VIOLENCE,
    NevadaOffenseGroup.SEX,
}


def nevada_group_severity_rank(group: OffenseGroup) -> int:
    """Returns the severity rank of a Nevada offense group: lower is more severe."""
    nevada_group = assert_type(group, NevadaOffenseGroup)
    return 0 if nevada_group in NEVADA_MORE_SEVERE_OFFENSE_GROUPS else 1


@attr.define(frozen=True, kw_only=True)
class NevadaSentence(Sentence):
    """Nevada-specific Sentence implementation.

    Overrides offense_category/offense_group with Nevada's concrete enum
    types, and adds the Nevada-only offense_id and
    qualified_for_enhanced_good_time fields.
    """

    offense_category: NevadaOffenseCategory = attr.ib(
        validator=attr.validators.in_(NevadaOffenseCategory)
    )
    offense_group: NevadaOffenseGroup = attr.ib(
        validator=attr.validators.in_(NevadaOffenseGroup)
    )

    # A code that uniquely identifies the offense this sentence is tied to.
    # For Nevada, this is the NOC (Nevada Offense Code).
    offense_id: int | None = attr.ib(default=None, validator=attr_validators.is_opt_int)

    # Whether this person earns work/education time on top of statutory good
    # time, which the PRE_413 credit_rate rules key on (NRS 209.4465(2)).
    qualified_for_enhanced_good_time: bool = attr.ib(
        default=False, validator=attr_validators.is_bool
    )


@total_ordering
@attr.define(frozen=True, kw_only=True)
class NvSentenceWithSortableEndDate:
    """A Sentence paired with its computed end date, sortable by end date with
    Nevada severity as a tie-breaker.

    This class would be generalizable to other states, were those states to
    implement sorted offense categories and groups.
    This is a wrapper rather than a subclass so that sentence fields don't need to be
    copied individually and kept in sync should the fields on Sentence change.
    """

    sentence: Sentence = attr.ib(
        validator=attr.validators.instance_of(Sentence)  # type: ignore[type-abstract]
    )
    """The wrapped sentence."""

    end_date: date = attr.ib(validator=attr_validators.is_date)
    """The date this sentence is projected to end."""

    @classmethod
    def from_sentence(
        cls, sentence: Sentence, end_date: date
    ) -> "NvSentenceWithSortableEndDate":
        return cls(sentence=sentence, end_date=end_date)

    def _is_more_severe(self, other: "NvSentenceWithSortableEndDate") -> bool:
        """Returns True if this sentence is more severe than the other sentence.

        Severity is determined by the following criteria:
        1. Higher offense group
        2. If offense groups are equally severe, higher offense category
        3. If offense categories are equally severe, lower sentence ID (to ensure
           deterministic ordering)
        """
        self_group_rank = nevada_group_severity_rank(self.sentence.offense_group)
        other_group_rank = nevada_group_severity_rank(other.sentence.offense_group)
        if self_group_rank != other_group_rank:
            return self_group_rank < other_group_rank
        if self.sentence.offense_category != other.sentence.offense_category:
            return nevada_category_severity_rank(
                self.sentence.offense_category
            ) < nevada_category_severity_rank(other.sentence.offense_category)
        return self.sentence.sentence_id < other.sentence.sentence_id

    def __lt__(self, other: "NvSentenceWithSortableEndDate") -> bool:
        if self.end_date != other.end_date:
            return self.end_date < other.end_date
        return other._is_more_severe(self)  # pylint: disable=protected-access
