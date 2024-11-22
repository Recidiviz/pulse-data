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
    UnescapedQuote,
    UnescapedQuoteState,
    find_first_unescaped_quote,
)


class TestUnescapedQuote(unittest.TestCase):
    """Tests for UnescapedQuote class"""

    line_term = (DEFAULT_CSV_LINE_TERMINATOR.encode(),)
    separator = DEFAULT_CSV_SEPARATOR.encode()
    carriage_return = (CARRIAGE_RETURN.encode(),)
    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    line_and_carriage = (DEFAULT_CSV_LINE_TERMINATOR.encode(), CARRIAGE_RETURN.encode())

    def test_invalid(self) -> None:

        invalid_tests = [
            [b"n\n", b"\r\n"],
            [b"n\n", b"\n"],
        ]
        for line_terminators in invalid_tests:

            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, the line terminators MUST be identical except for the newline \[\\n\] and carriage return \[\\r\\n\]. Instead, found:.*",
            ):
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=b"",
                    next_chars=b"",
                    quote_count=1,
                ).get_quote_state(
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=line_terminators,
                )

    def test_invalid_identical(self) -> None:
        invalid_tests = [
            [b"\n", b"\n"],
            [b"\r\n", b"\r\n"],
        ]
        for line_terminators in invalid_tests:

            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, they must not be identical. Instead, found:.*",
            ):
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=b"",
                    next_chars=b"",
                    quote_count=1,
                ).get_quote_state(
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=line_terminators,
                )

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
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                    quote_count=1,
                ).get_quote_state(
                    line_terminators=list(line_term),
                    separator=self.separator,
                    encoding="utf-8",
                )
                == UnescapedQuoteState.INDETERMINATE
            )

    def test_prev_char_sep(self) -> None:
        _tests = {
            # examples:
            #   - unquoted cell,",quoted cell starting w a sep"
            #   - unquoted cell,""",quoted cell starting w a sep after 3 quotes"
            #   - quoted cell ending with a sep,",unquoted cell
            #   - quoted cell ending with a sep and 3 quotes,""",unquoted cell
            (
                self.line_term,
                b"a,",
                b",a",
            ): UnescapedQuoteState.START_OR_END_OF_QUOTED_CELL,
            # examples:
            #   - unquoted cell,"\n quoted cell starting w a newline"
            #   - unquoted cell,"""\n quoted cell starting w a newline after 3 quotes"
            #   - quoted cell ending with a sep,"\n unquoted cell at start of newline
            #   - quoted cell ending with a sep and 3 quotes,"""\n unquoted cell at start of newline
            (
                self.line_term,
                b"a,",
                b"\nx",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"a,",
                b"\r\n",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"a,",
                b"\r",
            ): UnescapedQuoteState.INDETERMINATE,  # \n is cutoff
            # examples:
            #   - unquoted cell,"quoted cell starting w a non-special char"
            #   - unquoted cell then an empty cell,,"quoted cell starting w a non-special char"
            #   - unquoted cell then a quoted empty cell,"","quoted cell starting w a non-special char"
            #   - unquoted cell,"""quoted cell starting w a newline after 3 quotes"
            (self.line_term, b"a,", b"a,"): UnescapedQuoteState.START_QUOTED_CELL,
            (self.line_term, b",,", b"a,"): UnescapedQuoteState.START_QUOTED_CELL,
        }

        for (line_term, prev_, next_), state in _tests.items():

            assert (
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                    quote_count=1,
                ).get_quote_state(
                    line_terminators=list(line_term),
                    separator=self.separator,
                    encoding="utf-8",
                )
                == state
            )

    def test_prev_char_line_term(self) -> None:
        _tests = {
            # examples:
            #   - unquoted cell end of line\n",start of quoted line starting w a sep"
            #   - unquoted cell end of line\n""",quoted cell starting w a sep after a quote"
            #   - quoted cell ending with a newline \n",unquoted cell
            #   - quoted cell ending with a newline \n","quoted cell"
            #   - quoted cell ending with a newline and 3 quotes\n""",unquoted cell
            (
                self.line_term,
                b"\n",
                b",",
            ): UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"\n",
                b",",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                self.line_and_carriage,  # this is two chars and the look back isn't enough!
                b"a\n",
                b",a",
            ): UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"\r\n",
                b",a",
            ): UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            # examples:
            #   - unquoted cell end of line\n"\nstart of quoted line starting w a newline"
            #   - unquoted cell end of line\n"""\nquoted cell starting w a newline after a quote"
            #   - quoted cell ending with a newline \n"\n unquoted cell on new line
            #   - quoted cell ending with a newline \n"\n"quoted cell on new line"
            #   - quoted cell ending with a newline and 3 quotes\n"""\n unquoted cell on new line
            (
                self.line_term,
                b"\n",
                b"\n",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,  # this is two chars and the look back isn't enough!
                b"\n",
                b"\n",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                self.line_and_carriage,
                b"a\n",
                b"\na",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"\r\n",
                b"\na",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"a\n",
                b"\r\n",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"\r\n",
                b"\r\n",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"\r\n",
                b"\r\n",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            # examples:
            #   - unquoted cell end of line\n"start of quoted line"
            #   - unquoted cell end of line\n"""quoted cell starting w a quote"
            (
                self.line_term,
                b"\n",
                b"a",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            (
                self.line_and_carriage,  # this is two chars and the look back isn't enough!
                b"\n",
                b"a",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                self.line_and_carriage,
                b"\r\n",
                b"aa",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"a\n",
                b"aa",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"\r\n",
                b"a,",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            (self.line_term, b"\nx", b"\r"): UnescapedQuoteState.INVALID,
        }

        for (line_terms, prev_, next_), state in _tests.items():
            assert (
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                    quote_count=1,
                ).get_quote_state(
                    line_terminators=list(line_terms),
                    separator=self.separator,
                    encoding="utf-8",
                )
                == state
            )

    def test_prev_char_is_not_special_at_least_to_the_csv_reader(self) -> None:
        _tests = {
            # examples:
            #   - quoted cell end of line",start of unquoted line"
            #   - quoted cell end of line","start of quoted line"
            #   - quoted cell end of line""", start of unquoted line"
            #   - quoted cell end of line""","start of quoted line"
            #   - quoted cell end of line""","""start of quoted line"
            (self.line_term, b"a", b",a"): UnescapedQuoteState.END_QUOTED_CELL,
            (
                self.line_and_carriage,
                b"a",
                b",a",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                self.line_and_carriage,
                b"aa",
                b",a",
            ): UnescapedQuoteState.END_QUOTED_CELL,
            # examples:
            #   - quoted cell end of line"\n start of unquoted line"
            #   - quoted cell end of line"\n"start of quoted line"
            #   - quoted cell end of line"""\n start of unquoted line"
            #   - quoted cell end of line"""\n"start of quoted line"
            #   - quoted cell end of line"""\n"""start of quoted line"
            (
                self.line_term,
                b"a",
                b"\nx",
            ): UnescapedQuoteState.END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"a",
                b"\nx",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                self.line_and_carriage,
                b"aa",
                b"\nx",
            ): UnescapedQuoteState.END_OF_QUOTED_LINE,
            (
                self.line_and_carriage,
                b"aa",
                b"\r\n",
            ): UnescapedQuoteState.END_OF_QUOTED_LINE,
            (
                self.carriage_return,
                b"dd",
                b"\r\n",
            ): UnescapedQuoteState.END_OF_QUOTED_LINE,
            (self.line_term, b"x", b"x"): UnescapedQuoteState.INVALID,
            (self.line_term, b"dd", b"\r\n"): UnescapedQuoteState.INVALID,
        }

        for (line_terms, prev_, next_), state in _tests.items():

            assert (
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_,
                    next_chars=next_,
                    quote_count=1,
                ).get_quote_state(
                    line_terminators=list(line_terms),
                    separator=self.separator,
                    encoding="utf-8",
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
            ): UnescapedQuoteState.START_OR_END_OF_QUOTED_CELL,
            (
                "£",
                "a£",
                "\nx",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "£",
                "a£",
                "\na",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "£",
                "a\n",
                "£",
            ): UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
            (
                "£",
                "a\n",
                "a£",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            # multi char
            (
                "££",
                "aaa\n",
                "a£a",
            ): UnescapedQuoteState.START_OF_QUOTED_LINE,
            (
                "££",
                "a£",
                "£a",
            ): UnescapedQuoteState.INDETERMINATE,
            (
                "££",
                "££",
                "\nxaaa",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "££",
                "££",
                "\n£d",
            ): UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE,
            (
                "££",
                "££\n",
                "££",
            ): UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE,
        }

        for (separator, prev_, next_), state in _tests.items():
            assert (
                UnescapedQuote(
                    index=0,
                    quote_char=self.quote_char,
                    prev_chars=prev_.encode(),
                    next_chars=next_.encode(),
                    quote_count=1,
                ).get_quote_state(
                    line_terminators=list(self.line_term),
                    separator=separator.encode(),
                    encoding="utf-8",
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
            find_first_unescaped_quote(
                buffer=b"", cursor=0, quote_char=b"to", min_buffer_peek=2
            )

    def test_empty(self) -> None:
        buffers = [
            b"",
            b"asdf",
            b"'",
            b'heheonlydoublequotes""nosingles',
            b'heheonlytriplequotes""nosingles',
        ]
        for buffer in buffers:
            assert (
                find_first_unescaped_quote(
                    buffer=buffer,
                    cursor=0,
                    quote_char=self.quote_char,
                    min_buffer_peek=2,
                )
                is None
            )

    def test_finds_simple(self) -> None:
        buffers = [
            (
                b'here is a simple case",told ya',
                UnescapedQuote(
                    index=21,
                    prev_chars=b"se",
                    next_chars=b",t",
                    quote_char=self.quote_char,
                    quote_count=1,
                ),
            ),
            (
                b'here is a simple case""",told ya',
                UnescapedQuote(
                    index=21,
                    prev_chars=b"se",
                    next_chars=b",t",
                    quote_char=self.quote_char,
                    quote_count=3,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_first_unescaped_quote(
                    buffer=buffer,
                    cursor=0,
                    quote_char=self.quote_char,
                    min_buffer_peek=2,
                )
                == expected_result
            )

    def test_finds_complex(self) -> None:
        buffers = [
            (
                b'here is a harder case: "" <-- that is escaped! case",told ya',
                UnescapedQuote(
                    index=51,
                    prev_chars=b"se",
                    next_chars=b",t",
                    quote_char=self.quote_char,
                    quote_count=1,
                ),
            ),
            (
                b'harder again: "" (escaped) and then (unescaped)""""","told ya"',
                UnescapedQuote(
                    index=47,
                    prev_chars=b"d)",
                    next_chars=b',"',
                    quote_char=self.quote_char,
                    quote_count=5,
                ),
            ),
            (
                b'harder again:"","","""","""yes"',
                UnescapedQuote(
                    index=24,
                    prev_chars=b'",',
                    next_chars=b"ye",
                    quote_char=self.quote_char,
                    quote_count=3,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_first_unescaped_quote(
                    buffer=buffer,
                    cursor=0,
                    quote_char=self.quote_char,
                    min_buffer_peek=2,
                )
                == expected_result
            )

    def test_alt_quote(self) -> None:
        quote_char = "'".encode()
        buffers = [
            (
                b"here is a simpler case: '' <-- that is escaped! case',told ya",
                UnescapedQuote(
                    index=52,
                    prev_chars=b"e",
                    next_chars=b",",
                    quote_char=quote_char,
                    quote_count=1,
                ),
            ),
            (
                b"harder again: '' (escaped) and then (unescaped)''''','told ya'",
                UnescapedQuote(
                    index=47,
                    prev_chars=b")",
                    next_chars=b",",
                    quote_char=quote_char,
                    quote_count=5,
                ),
            ),
        ]
        for buffer, expected_result in buffers:
            assert (
                find_first_unescaped_quote(
                    buffer=buffer, cursor=0, quote_char=quote_char, min_buffer_peek=1
                )
                == expected_result
            )
