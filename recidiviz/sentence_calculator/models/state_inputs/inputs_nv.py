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

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.inputs import (
    OffenseCategory,
    OffenseGroup,
    Sentence,
)


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
