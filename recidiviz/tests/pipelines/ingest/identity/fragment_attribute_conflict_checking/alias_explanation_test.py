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
"""Tests for aliases_explain_name_component_conflict."""
import unittest

from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.alias_explanation import (
    aliases_explain_name_component_conflict,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.name import (
    Name,
    NameComponent,
)


def _name(
    *,
    surname: str | None = None,
    given_name: str | None = None,
    middle_name: str | None = None,
    name_suffix: str | None = None,
) -> Name:
    """Builds a Name carrying the given components."""
    return Name(
        surname=surname,
        given_name=given_name,
        middle_name=middle_name,
        name_suffix=name_suffix,
    )


class TestAliasesExplainNameComponentConflict(unittest.TestCase):
    """Tests for aliases_explain_name_component_conflict."""

    def test_conflicting_name_matches_other_fragments_alias(self) -> None:
        # b also goes by a's surname, so the alias explains the divergence.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="WILLIAMS"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(surname="WILLIAMS"),),
            )
        )

    def test_match_in_either_direction_explains(self) -> None:
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="WILLIAMS"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(_name(surname="JOHNSON"),),
                aliases_b=(),
            )
        )

    def test_alias_match_is_abbreviation_aware(self) -> None:
        # normalize_name expands ST to SAINT, so the recorded alias
        # "SAINT CLAIRE" explains the primary "ST CLAIRE".
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="ST CLAIRE"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(surname="SAINT CLAIRE"),),
            )
        )

    def test_alias_match_tolerates_spelling_drift(self) -> None:
        # Loose match: one Levenshtein edit between the name and the alias.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="GUSTAFSON"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(surname="GUSTAFSEN"),),
            )
        )

    def test_whole_alias_row_must_match(self) -> None:
        # The alias "MARIA GONZALEZ" is the whole name "MARIA SMITH" goes by,
        # so it explains the GONZALEZ/SMITH surname divergence.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="MARIA", surname="GONZALEZ"),
                name_b=_name(given_name="MARIA", surname="SMITH"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(given_name="MARIA", surname="GONZALEZ"),),
            )
        )

    def test_alias_omitting_a_component_still_explains(self) -> None:
        # A sparsely populated alias row is not penalized for what it omits:
        # a surname-only alias contradicts nothing.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="MARIA", surname="GONZALEZ"),
                name_b=_name(given_name="MARIA", surname="SMITH"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(surname="GONZALEZ"),),
            )
        )

    def test_alias_middle_initial_does_not_block(self) -> None:
        # The alias's middle initial "L" abbreviates the primary's "LEE", and
        # a lone initial does not contradict a name it abbreviates.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(
                    given_name="KIMBERLY", middle_name="LEE", surname="JOHNSON"
                ),
                name_b=_name(given_name="TAMMY", surname="JOHNSON"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(
                    _name(given_name="KIMBERLY", middle_name="L", surname="JOHNSON"),
                ),
            )
        )

    def test_alias_given_name_initial_does_not_block(self) -> None:
        # The alias records only the initial "J" for a given name of "JOHN";
        # an initial is a half-recorded value, not a contradiction, on any
        # component.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="JOHN", surname="WILLIAMS"),
                name_b=_name(given_name="JOHN", surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(given_name="J", surname="WILLIAMS"),),
            )
        )

    def test_alias_spelling_drift_on_another_component_does_not_block(self) -> None:
        # "GUSTAFSEN" loosely matches "GUSTAFSON" (one edit apart), so it does
        # not contradict.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="KIMBERLY", surname="GUSTAFSON"),
                name_b=_name(given_name="TAMMY", surname="GUSTAFSON"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(_name(given_name="KIMBERLY", surname="GUSTAFSEN"),),
            )
        )

    def test_alias_with_equivalent_suffix_form_explains(self) -> None:
        # "JUNIOR" and "JR" canonicalize to the same generation, so the
        # spellings do not disagree.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="JOHN", surname="SMITH", name_suffix="JR"),
                name_b=_name(given_name="ROBERT", surname="SMITH", name_suffix="JR"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(
                    _name(given_name="JOHN", surname="SMITH", name_suffix="JUNIOR"),
                ),
            )
        )

    def test_alias_without_suffix_still_explains(self) -> None:
        # A suffix missing from the alias never disagrees, like any other
        # unrecorded value.
        self.assertTrue(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="JOHN", surname="SMITH", name_suffix="JR"),
                name_b=_name(given_name="ROBERT", surname="SMITH", name_suffix="JR"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(_name(given_name="JOHN", surname="SMITH"),),
            )
        )

    def test_alias_contradicting_on_another_component_does_not_explain(
        self,
    ) -> None:
        # The alias surname matches, but the alias is JOHN GONZALEZ, not MARIA
        # GONZALEZ, so it is not evidence that this MARIA also goes by
        # GONZALEZ.
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="MARIA", surname="GONZALEZ"),
                name_b=_name(given_name="MARIA", surname="SMITH"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(_name(given_name="JOHN", surname="GONZALEZ"),),
            )
        )

    def test_alias_with_genuinely_different_middle_does_not_explain(self) -> None:
        # The alias asserts middle name "JEAN" where the primary records the
        # initial "R"; the sources genuinely disagree about the middle name,
        # so the alias is not evidence for the given-name divergence.
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="LORNA", middle_name="R", surname="COOPER"),
                name_b=_name(given_name="DELLA", surname="COOPER"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(
                    _name(given_name="LORNA", middle_name="JEAN", surname="COOPER"),
                ),
            )
        )

    def test_alias_with_different_surname_does_not_explain(self) -> None:
        # The alias pairs the matching given name with an unrelated surname,
        # which contradicts the other primary's surname, so this pair stays in
        # conflict even though the primaries agree on surname; a cluster like
        # this needs a recorded override to merge.
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="BUD", surname="WILLIAMS"),
                name_b=_name(given_name="CHARLES", surname="WILLIAMS"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(_name(given_name="CHARLES", surname="JOHNSON"),),
                aliases_b=(),
            )
        )

    def test_alias_with_different_generation_suffix_does_not_explain(self) -> None:
        # The alias is JOHN SMITH SR, a different generation than the primary
        # JOHN SMITH JR, so it likely names a relative rather than another
        # name this person goes by.
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(given_name="JOHN", surname="SMITH", name_suffix="JR"),
                name_b=_name(given_name="ROBERT", surname="SMITH", name_suffix="JR"),
                conflicting_component=NameComponent.GIVEN_NAME,
                aliases_a=(),
                aliases_b=(
                    _name(given_name="JOHN", surname="SMITH", name_suffix="SR"),
                ),
            )
        )

    def test_shared_alias_alone_does_not_explain(self) -> None:
        # Only primary-vs-alias matches count: the fragments share an alias,
        # but neither primary name matches the other's aliases.
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="WILLIAMS"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(_name(surname="SMITH"),),
                aliases_b=(_name(surname="SMITH"),),
            )
        )

    def test_unrelated_alias_does_not_explain(self) -> None:
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="WILLIAMS"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(_name(surname="BROWN"),),
                aliases_b=(_name(surname="DAVIS"),),
            )
        )

    def test_no_aliases_never_explains(self) -> None:
        self.assertFalse(
            aliases_explain_name_component_conflict(
                name_a=_name(surname="WILLIAMS"),
                name_b=_name(surname="JOHNSON"),
                conflicting_component=NameComponent.SURNAME,
                aliases_a=(),
                aliases_b=(),
            )
        )
