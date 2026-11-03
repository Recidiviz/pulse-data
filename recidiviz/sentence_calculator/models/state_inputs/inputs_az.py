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
"""Arizona-specific Sentence input models."""

from datetime import date

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.inputs import (
    OffenseCategory,
    OffenseGroup,
    Sentence,
)


class ArizonaOffenseCategory(OffenseCategory):
    """Felony class of the offense. Class 1 is the most severe, class 6 the
    least."""

    CLASS_1 = "CLASS_1"
    CLASS_2 = "CLASS_2"
    CLASS_3 = "CLASS_3"
    CLASS_4 = "CLASS_4"
    CLASS_5 = "CLASS_5"
    CLASS_6 = "CLASS_6"

    UNKNOWN = "UNKNOWN"


class ArizonaOffenseGroup(OffenseGroup):
    """Type of offense, as far as the time computation rules distinguish them."""

    VIOLENT = "VIOLENT"
    SEX = "SEX"
    DANGEROUS_CRIME_AGAINST_CHILDREN = "DANGEROUS_CRIME_AGAINST_CHILDREN"
    # Personal possession or use of a drug, which earns release credits at a
    # higher rate under the Drug Possession Release program.
    DRUG_POSSESSION = "DRUG_POSSESSION"
    OTHER = "OTHER"

    # For fields that are present but not one of the above known types
    UNKNOWN = "UNKNOWN"


@attr.define(frozen=True, kw_only=True)
class ArizonaSentence(Sentence):
    """Arizona-specific Sentence implementation.

    Overrides offense_category and offense_group with Arizona's concrete enum
    types, and adds the fields Arizona's law regimes read: the offense and
    sentencing dates, the statute, and the flags the court sets at sentencing.
    """

    offense_category: ArizonaOffenseCategory = attr.ib(
        validator=attr.validators.in_(ArizonaOffenseCategory)
    )
    offense_group: ArizonaOffenseGroup = attr.ib(
        validator=attr.validators.in_(ArizonaOffenseGroup)
    )

    offense_date: date = attr.ib(validator=attr_validators.is_date)
    """Date the offense was committed. Arizona chooses the law regime that
    governs a sentence by this date."""

    date_sentenced: date = attr.ib(validator=attr_validators.is_date)
    """Date the court imposed the sentence. When a consecutive sentence's start
    is computed from the prior sentence's end, it is floored at this date."""

    statute: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Arizona Revised Statutes section of the offense, e.g. "13-1105". The
    regime files compare it against statute lists such as the offenses that
    must serve 85% of the term."""

    subsection: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Subsection within the statute, e.g. "(A)(1)", for the statute lists that
    name one. None when the offense has no subsection."""

    is_flat: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the sentence is flat time, served day for day with no earned
    release credits."""

    is_community_supervision_imposed: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the court imposed a term of community supervision to follow the
    prison term. When the court waives it, the sentence ends at the earned
    release date."""

    is_prison_a_condition_of_probation: bool = attr.ib(
        validator=attr_validators.is_bool
    )
    """Whether the prison term was imposed as a condition of probation (ARS
    13-901). The regime files compute these sentences differently from a prison
    sentence."""
