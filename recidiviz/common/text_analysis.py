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
"""Utility class for performing natural language processing on given strings and fields.

Primarily supporting fuzzy matching of free text against configuration-declared entities
in order to support ingest and calculations."""
import abc
import os
import re
from enum import Enum, EnumMeta
from typing import Callable, List, Optional, Set, Tuple

import attr
from nltk import data
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import ToktokTokenizer
from thefuzz import fuzz

from recidiviz.common.data_sets import nltk_data

DEFAULT_MATCHING_SCORE_CUTOFF = 90

# A set of substitutions used to normalize input text, executed in order
TEXT_NORMALIZERS: List[Tuple[str, str]] = [
    # words with a number, "@", website, or encoding string
    (r"\S*(\d|@|http|www|\ufffd)\S*", " "),
    # all non characters (numbers, punctuation, non-spaces)
    ("[^a-z ]+", " "),  # remove anything not a character or space
    # multiple whitespaces
    (r"\s+", " "),
]

_nltk_path = os.path.dirname(nltk_data.__file__)
if not _nltk_path in data.path:
    data.path.append(_nltk_path)


@attr.s(kw_only=True)
class FuzzyMatcher(abc.ABC):
    """Abstract class for performing fuzzy matching of free text against a search term."""

    def matches(self, text: str) -> bool:
        """Abstract method to determine if a piece of free text matches the search term."""


@attr.s(kw_only=True)
class ScoringFuzzyMatcher(FuzzyMatcher):
    """Performs fuzzy matching by having the matching_function produce a continuous score,
    indicating the approximate amount of matching. If needed, can be compared to a set
    cutoff to produce a binary decision of yes or no match."""

    search_term: str = attr.ib()
    matching_function: Callable[[str, str], int] = attr.ib(default=fuzz.token_set_ratio)
    score_cutoff: int = attr.ib(default=DEFAULT_MATCHING_SCORE_CUTOFF)

    def matches(self, text: str) -> bool:
        return self.matching_function(self.search_term, text) >= self.score_cutoff


@attr.s(kw_only=True)
class RegexFuzzyMatcher(FuzzyMatcher):
    """Performs fuzzy matching by having the matching_function produce a binary output,
    indicating a match exists or not."""

    search_regex: str = attr.ib()
    matching_function: Callable[[str, str], Optional[re.Match]] = attr.ib(
        default=re.search
    )

    def matches(self, text: str) -> bool:
        return self.matching_function(self.search_regex, text) is not None


class TextEntity(Enum, metaclass=EnumMeta):
    """Meta Enum class to represent the various flags we expect to be true based on
    the fuzzy matching results of given free text. Each flag is associated with a set
    of fuzzy matchers."""

    def __init__(self, fuzzy_matchers: List[FuzzyMatcher]) -> None:
        self.fuzzy_matchers = fuzzy_matchers

    def matches(self, normalized_text: str) -> bool:
        """Indicates that a text flag matches the normalized text by looping through all
        of the fuzzy matchers. As soon as the first fuzzy matcher matches the text, we
        say that the flag matches and break before continuing to the rest of the matchers."""
        for fuzzy_matcher in self.fuzzy_matchers:
            if fuzzy_matcher.matches(normalized_text):
                return True
        return False


@attr.s
class TextMatchingConfiguration:
    """Configuration passed into the TextAnalyzer that includes all settings needed in order
    to determine whether indicators / flags are matched against."""

    stop_words_to_remove: Set[str] = attr.ib(default=set())
    text_entities: List[TextEntity] = attr.ib(default=[])


class TextAnalyzer:
    """Contains all of the natural-language processing functionality needed in order to
    be able to process, clean and match free text against given configuration criteria."""

    def __init__(self, configuration: TextMatchingConfiguration) -> None:
        self.configuration = configuration
        self.stop_words = {
            self._clean_text(word) for word in stopwords.words("english")
        }
        self.stop_words_to_remove = {
            self._clean_text(word) for word in self.configuration.stop_words_to_remove
        }
        self.stop_words -= self.stop_words_to_remove
        self.tokenizer = ToktokTokenizer()
        self.stemmer = SnowballStemmer(language="english")

    def _clean_text(self, text: str) -> str:
        """Cleans text by lowercasing, removing any unwanted characters and extra
        whitespaces."""
        text = text.lower()
        for expression, replacement in TEXT_NORMALIZERS:
            text = re.sub(expression, replacement, text)
        return text.strip()

    def _tokenize(self, text: str) -> Set[str]:
        """Tokenizes the text and removes any stop words."""
        tokenized_text = self.tokenizer.tokenize(text)
        return set(tokenized_text) - self.stop_words

    def _stem(self, token: str) -> str:
        """Stems the token (reduces the word to its root form)."""
        return self.stemmer.stem(token)

    def normalize_text(self, text: str, stem_tokens: bool = False) -> str:
        """Normalizes a text string, by lowercasing, removing punctuation,
        irregular white space, and stop words. Optionally stems the tokens."""
        tokens = sorted(self._tokenize(self._clean_text(text)))
        if stem_tokens:
            tokens = [self._stem(token) for token in tokens]
        return " ".join(tokens)

    def extract_entities(self, text: str) -> Set[TextEntity]:
        """Returns the set of TextEntity that apply to the input text."""
        normalized_text = self.normalize_text(text)
        matched_entities = set()
        for text_entity in self.configuration.text_entities:
            if text_entity.matches(normalized_text):
                matched_entities.add(text_entity)

        return matched_entities
