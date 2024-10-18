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
"""Test for unescaped quote utils."""

import unittest

from recidiviz.common.constants.csv import (
    CARRIAGE_RETURN,
    DEFAULT_CSV_LINE_TERMINATOR,
    DEFAULT_CSV_QUOTE_CHAR,
    DEFAULT_CSV_SEPARATOR,
)
from recidiviz.utils.unescaped_quote_classification import (
    SingleUnescapedQuote,
    SingleUnescapedQuoteState,
    find_single_first_unescaped_quote,
)


class TestSingleUnescapedQuote(unittest.TestCase):
    """Tests for SingleUnescapedQuote class"""

    line_term = DEFAULT_CSV_LINE_TERMINATOR.encode()
    separator = DEFAULT_CSV_SEPARATOR.encode()
    carriage_return = CARRIAGE_RETURN.encode()
    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()

    def test_empties(self) -> None:

        empty_tests = [
            (b"", b"", self.carriage_return),
            (b"", b"", self.line_term),
            (b",,", b",", self.carriage_return),
            (b"", b",", self.carriage_return),
            (b"", b",", self.line_term),
        ]
        for prev_, next_, line_term in empty_tests:

            assert (
                SingleUnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                ).get_quote_state(
                    line_term=line_term,
                    separator=self.separator,
                )
                == SingleUnescapedQuoteState.INDETERMINATE
            )

    def test_prev_char_sep(self) -> None:
        _tests = {
            (
                self.line_term,
                b"a,",
                b",a",
            ): SingleUnescapedQuoteState.START_OR_END_OF_QUOTED_CELL,
            (
                self.line_term,
                b"a,",
                b"\nx",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"a,",
                b"\r\n",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"a,",
                b"\r",
            ): SingleUnescapedQuoteState.INDETERMINATE,  # \n is cutoff
            (self.line_term, b"a,", b"a,"): SingleUnescapedQuoteState.START_QUOTED_CELL,
            (self.line_term, b",,", b"a,"): SingleUnescapedQuoteState.START_QUOTED_CELL,
        }

        for (line_term, prev_, next_), state in _tests.items():

            assert (
                SingleUnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                ).get_quote_state(
                    line_term=line_term,
                    separator=self.separator,
                )
                == state
            )

    def test_prev_char_line_term(self) -> None:
        _tests = {
            (
                self.line_term,
                b"\n",
                b",",
            ): SingleUnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            (
                self.line_term,
                b"\n",
                b"\n",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"\r\n",
                b"\r\n",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"\r\n",
                b"a,",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_LINE,
            (self.line_term, b"\nx", b"\r"): SingleUnescapedQuoteState.INVALID,
        }

        for (line_term, prev_, next_), state in _tests.items():

            assert (
                SingleUnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                ).get_quote_state(
                    line_term=line_term,
                    separator=self.separator,
                )
                == state
            )

    def test_prev_char_is_not_special_at_least_to_the_csv_reader(self) -> None:
        _tests = {
            (self.line_term, b"a", b",a"): SingleUnescapedQuoteState.END_QUOTED_CELL,
            (
                self.line_term,
                b"a",
                b"\nx",
            ): SingleUnescapedQuoteState.END_OF_QUOTED_LINE,
            (self.line_term, b"x", b"x"): SingleUnescapedQuoteState.INVALID,
            (
                self.carriage_return,
                b"dd",
                b"\r\n",
            ): SingleUnescapedQuoteState.END_OF_QUOTED_LINE,
            (self.line_term, b"dd", b"\r\n"): SingleUnescapedQuoteState.INVALID,
        }

        for (line_term, prev_, next_), state in _tests.items():

            assert (
                SingleUnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                ).get_quote_state(
                    line_term=line_term,
                    separator=self.separator,
                )
                == state
            )

    def test_multi_byte_sep(self) -> None:
        _tests = {
            # multiple byte separator
            (
                "£",
                "a£",
                "£a",
            ): SingleUnescapedQuoteState.START_OR_END_OF_QUOTED_CELL,
            (
                "£",
                "a£",
                "\nx",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "£",
                "a£",
                "\na",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "£",
                "a\n",
                "£",
            ): SingleUnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            (
                "£",
                "a\n",
                "a£",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_LINE,
            # multi char
            (
                "££",
                "aaa\n",
                "a£a",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_LINE,
            (
                "££",
                "a£",
                "£a",
            ): SingleUnescapedQuoteState.INDETERMINATE,
            (
                "££",
                "££",
                "\nxaaa",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "££",
                "££",
                "\n£d",
            ): SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "££",
                "££\n",
                "££",
            ): SingleUnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
        }

        for (separator, prev_, next_), state in _tests.items():

            print(separator, prev_.encode(), next_.encode())
            assert (
                SingleUnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_.encode(),
                    next_chars=next_.encode(),
                ).get_quote_state(
                    line_term=self.line_term,
                    separator=separator.encode(),
                )
                == state
            )


class TestFindUnescapedQuote(unittest.TestCase):
    """Tests for find_first_unescaped_quote function"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()

    def test_single_byte_only(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Cannot have a quote char that is more than a single byte"
        ):
            find_single_first_unescaped_quote(b"", b"to", 2)

    def test_empty(self) -> None:
        buffers = [
            b"",
            b"asdf",
            b"'",
            b'heheonlydoublequotes""nosingles',
            b'heheonlytriplequotes""nosingles',
        ]
        for buffer in buffers:
            assert find_single_first_unescaped_quote(buffer, self.quote_char, 2) is None

    def test_finds_simple(self) -> None:
        buffers = [
            (
                b'here is a simple case",told ya',
                SingleUnescapedQuote(
                    index=21,
                    prev_chars=b"se",
                    next_chars=b",t",
                    quote_char=self.quote_char,
                ),
            ),
            (
                b'here is a simple case""","told ya"',
                SingleUnescapedQuote(
                    index=25,
                    prev_chars=b'",',
                    next_chars=b"to",
                    quote_char=self.quote_char,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_single_first_unescaped_quote(buffer, self.quote_char, 2)
                == expected_result
            )

    def test_finds_complex(self) -> None:
        buffers = [
            (
                b'here is a harder case: "" <-- that is escaped! case",told ya',
                SingleUnescapedQuote(
                    index=51,
                    prev_chars=b"se",
                    next_chars=b",t",
                    quote_char=self.quote_char,
                ),
            ),
            (
                b'harder again: "" (escaped) and then (unescaped)""""","told ya"',
                SingleUnescapedQuote(
                    index=53,
                    prev_chars=b'",',
                    next_chars=b"to",
                    quote_char=self.quote_char,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_single_first_unescaped_quote(buffer, self.quote_char, 2)
                == expected_result
            )

    def test_alt_quote(self) -> None:
        quote_char = "'".encode()
        buffers = [
            (
                b"here is a simpler case: '' <-- that is escaped! case',told ya",
                SingleUnescapedQuote(
                    index=52, prev_chars=b"e", next_chars=b",", quote_char=quote_char
                ),
            ),
            (
                b"harder again: '' (escaped) and then (unescaped)''''','told ya'",
                SingleUnescapedQuote(
                    index=53,
                    prev_chars=b",",
                    next_chars=b"t",
                    quote_char=quote_char,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_single_first_unescaped_quote(buffer, quote_char, 1)
                == expected_result
            )
