# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for text_analysis.py"""
import unittest
from typing import Set

from parameterized import parameterized
from thefuzz import fuzz

from recidiviz.common.text_analysis import (
    RegexFuzzyMatcher,
    ScoringFuzzyMatcher,
    TextAnalyzer,
    TextEntity,
    TextMatchingConfiguration,
)

REGEX_MATCHER = RegexFuzzyMatcher(search_regex=".*hello.*")
DEFAULT_MATCHER = ScoringFuzzyMatcher(search_term="hi")
PARTIAL_RATIO_MATCHER = ScoringFuzzyMatcher(
    search_term="bye", matching_function=fuzz.partial_ratio
)


class FakeTextEntity(TextEntity):
    HELLO = [REGEX_MATCHER, DEFAULT_MATCHER]
    GOODBYE = [PARTIAL_RATIO_MATCHER]


class TestTextAnalyzer(unittest.TestCase):
    """Test the TextAnalyzer methods"""

    def setUp(self) -> None:
        self.text_matching_delegate = TextMatchingConfiguration(
            stop_words_to_remove={"in", "out"},
            text_entities=[FakeTextEntity.HELLO, FakeTextEntity.GOODBYE],
        )
        self.text_analyzer = TextAnalyzer(self.text_matching_delegate)

    @parameterized.expand(
        [
            ("default_match", "hello", {FakeTextEntity.HELLO}),
            ("partial_ratio", "GoodBye ", {FakeTextEntity.GOODBYE}),
            ("regex", "HI", {FakeTextEntity.HELLO}),
            ("no_match", "unrelated text", set()),
            (
                "multiple_matches",
                "Hello Goodbye",
                {FakeTextEntity.HELLO, FakeTextEntity.GOODBYE},
            ),
        ]
    )
    def test_text_analyzer_extract_entities(
        self, _name: str, text: str, expected: Set[FakeTextEntity]
    ) -> None:
        result = self.text_analyzer.extract_entities(text)
        self.assertEqual(result, expected)

    def test_fuzzy_matching_exact_matches(self) -> None:
        """Tests token_set_ratio, which is used for exact word matches, irrespective
        of position within string, order of appearance, other characters present."""
        self.assertTrue(
            DEFAULT_MATCHER.matches("they said hi to me at the grocery store")
        )
        self.assertFalse(
            DEFAULT_MATCHER.matches("they said hello to me at the grocery store")
        )

    def test_fuzzy_matching_similar_root(self) -> None:
        """Tests partial_set_ratio, which is used for words appearing with the same
        root, irrespective of order or other characters present."""
        self.assertTrue(PARTIAL_RATIO_MATCHER.matches("they said their byes to me"))
        self.assertTrue(PARTIAL_RATIO_MATCHER.matches("they said bye to me"))
        self.assertFalse(PARTIAL_RATIO_MATCHER.matches("they said by to me"))

    def test_fuzzy_matching_more_than_one_search_word(self) -> None:
        """Tests re.search, which is used for more than one search word, and the
        search words may not appear exactly the same way as the search text."""
        self.assertTrue(REGEX_MATCHER.matches("they said helloooo"))
        self.assertTrue(REGEX_MATCHER.matches("hellogoodbyehello"))

    def test_normalization_with_stemming(self) -> None:
        """Tests the optional `stem_tokens` argument to `normalize_text`,
        which is used to reduce words to their root form."""
        self.assertNotEqual(
            self.text_analyzer.normalize_text("felonies"),
            self.text_analyzer.normalize_text("felony"),
        )
        self.assertEqual(
            self.text_analyzer.normalize_text("felonies", stem_tokens=True),
            self.text_analyzer.normalize_text("felony", stem_tokens=True),
        )
