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
"""Test for line terminator finding utils."""

import unittest

from recidiviz.common.constants.csv import (
    DEFAULT_CSV_LINE_TERMINATOR,
    DEFAULT_CSV_QUOTE_CHAR,
    DEFAULT_CSV_SEPARATOR,
)
from recidiviz.utils.quoted_csv_line_terminator_finder import (
    determine_quoting_state_for_buffer,
    find_line_terminator_for_quoted_csv,
    walk_to_find_unescaped_line_terminator,
)


class TestWalkToFindUnescapedNewline(unittest.TestCase):
    """Unit tests for walk_to_find_unescaped_line_terminator"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    line_term = DEFAULT_CSV_LINE_TERMINATOR.encode()

    def test_walk_to_find_unquoted_line_term_empty(self) -> None:
        tests = [
            (b"", False, None),
            (b"", True, None),
            (b"aaaaaaaa", False, None),
            (b"aaaaaaaa", True, None),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_unescaped_line_terminator(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminator=self.line_term,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_simple(self) -> None:
        tests = [
            (b"this,is,a,simple,unquoted,csv\nfun,fun,fun", False, 29),
            (b'this,is,a,simple,"quoted\n","csv"\nfun,"fun",fun', False, 32),
            (b'this,is,a,quoted,field",csv\n"and,another"\n', True, 27),
            (b'\nthis,is,a,"newline"', False, 0),
            (b'this","is","a","fully","quoted","csv"\n"fun","fun","fun"', True, 37),
            (b"this,is,all,a,quoted,field\nfun,fun,fun", True, None),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_unescaped_line_terminator(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminator=self.line_term,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_tricky(self) -> None:
        tests = [
            (b'unquoted field,"""this is a quoted field\n""",csv\nfun,', False, 48),
            (b'unquoted,"\n""\n""\n""this is a quoted field"""""""\nfun,f', False, 48),
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', True, 44),
            (b',"","","","""","""""","""\n""",""""\n"","","",""', False, 34),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_unescaped_line_terminator(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminator=self.line_term,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_alt_values(self) -> None:
        tests = [
            (b"'", b"\r\n", b"this,only,has,a,newline,not,the,term,csv\n", False, None),
            (b"'", b"\r\n", b"this,is,a,simple,unquoted,csv\r\nfun,fun,fun", False, 29),
            (b"'", b"\r\n", b"unquoted \n <-not one... but is one->\r\n", False, 36),
            (
                b"'",
                b"\r\n",
                b"this,is,a,simple,'quoted\r\n','csv'\r\nfun,'fun',fun",
                False,
                33,
            ),
            (
                b"'",
                b"\r\n",
                b"this,is,a,quoted,field',csv\r\n'and,another'\r\n",
                True,
                27,
            ),
            (b"'", b"\r\n", b"\r\nthis,is,a,'newline'", False, 0),
            (
                b"'",
                b"\r\n",
                b"this','is','a','fully','quoted','csv'\r\n'fun'",
                True,
                37,
            ),
            (b"'", b"\r\n", b"this,is,all,a,quoted,field\r\nfun,fun,fun", True, None),
        ]

        for (
            quote_char,
            line_term,
            buffer,
            in_quoted_cell,
            expected_line_terminator_index,
        ) in tests:
            actual_line_terminator_index = walk_to_find_unescaped_line_terminator(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=quote_char,
                line_terminator=line_term,
            )
            assert actual_line_terminator_index == expected_line_terminator_index


class TestDetermineQuotingStateForBuffer(unittest.TestCase):
    """Unit tests for determine_quoting_state_for_buffer"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    line_term = DEFAULT_CSV_LINE_TERMINATOR.encode()
    separator = DEFAULT_CSV_SEPARATOR.encode()

    def test_determine_quoting_state_for_buffer_simple(self) -> None:
        tests = [
            # none
            (b"this,is,a,simple,unquoted,csv\nfun,fun,fun", None, None),
            (b"this,is,all,a,quoted,field\nfun,fun,fun", None, None),
            # START_QUOTED_CELL
            (b'this,is,a,simple,"quoted\n","csv"\nfun,"fun",fun', True, 18),
            (b'\nthis,is,a,"newline"', True, 12),
            # END_QUOTED_CELL
            (b'this,is,a,quoted,field",csv\n"and,another"\n', False, 23),
            (b'this","is","a","fully","quoted","csv"\n"fun","fun","fun"', False, 5),
            # END_OF_QUOTED_LINE
            (b'this,is,a,quoted,field"\ncsv,"and,another"\n', False, 23),
            # START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE
            (b'quoted,"\ncsv', None, None),
            (b'quoted,"\ncsv,"hello!', True, 14),
            # START_OR_END_OF_QUOTED_CELL
            (b'quoted,",not quoted', None, None),
            (b'quoted,",not quoted,"quoted"', True, 21),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE
            (b'unquoted\n",quoted not closed', None, None),
            (b'unquoted\n",quoted and closed",unquoted', False, 29),
            (b'unquoted,"\nunquoted', None, None),
            (b'unquoted,"\nunquoted,but,eventually,"quoted,"\n', True, 36),
            # START_OF_QUOTED_LINE
            (b'this,is,a,quoted,field\n"csv,"and,another"\n', False, 22),
        ]

        for buffer, expected_in_quoted_cell, expected_cursor in tests:
            result = determine_quoting_state_for_buffer(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                line_terminator=self.line_term,
            )
            if result is None:
                assert result == expected_in_quoted_cell
                assert result == expected_cursor
            else:
                actual_cursor, actual_in_quoted_cell = result
                assert actual_in_quoted_cell == expected_in_quoted_cell
                assert actual_cursor == expected_cursor

    def test_determine_quoting_state_for_buffer_tricky(self) -> None:
        tests = [
            # START_QUOTED_CELL
            (b'unquoted field,""",this is a quoted field\n","csv"\nfun,', True, 45),
            # END_QUOTED_CELL
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun,f', False, 42),
            # END_OF_QUOTED_LINE
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', False, 44),
            # START_OF_QUOTED_LINE
            (b',"","","","""","""""","""\n""",""""\n"aaa","","",""', False, 34),
        ]

        for buffer, expected_in_quoted_cell, expected_cursor in tests:
            result = determine_quoting_state_for_buffer(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                line_terminator=self.line_term,
            )
            if result is None:
                assert result == expected_in_quoted_cell
                assert result == expected_cursor
            else:
                actual_cursor, actual_in_quoted_cell = result
                assert actual_in_quoted_cell == expected_in_quoted_cell
                assert actual_cursor == expected_cursor


class TestFindLineTerminatorForQuotedCsv(unittest.TestCase):
    """Unit tests for find_line_terminator_for_quoted_csv"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    line_term = DEFAULT_CSV_LINE_TERMINATOR.encode()
    separator = DEFAULT_CSV_SEPARATOR.encode()

    def test_find_line_terminator_for_quoted_csv_simple(self) -> None:
        tests = [
            # none
            (b"this,is,a,simple,unquoted,csv\nfun,fun,fun", None),
            (b"this,is,all,a,quoted,field\nfun,fun,fun", None),
            # START_QUOTED_CELL -> walk from 18 to 32
            (b'this,is,a,simple,"quoted\n","csv"\nfun,"fun",fun', 32),
            # START_QUOTED_CELL -> no newline after 12
            (b'\nthis,is,a,"newline"', None),
            # END_QUOTED_CELL -> walk from 23 to 27
            (b'this,is,a,quoted,field",csv\n"and,another"\n', 27),
            # END_QUOTED_CELL -> walk from 5 to 37
            (b'this","is","a","fully","quoted","csv"\n"fun","fun","fun"', 37),
            # END_OF_QUOTED_LINE -> walk from 23 to 24
            (b'this,is,a,quoted,field"\ncsv,"and,another"\n', 23),
            # START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE -> no walking
            (b'quoted,"\ncsv', None),
            # START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE -> no newline after 14
            (b'quoted,"\ncsv,"hello!', None),
            # START_OR_END_OF_QUOTED_CELL -> no walking
            (b'quoted,",not quoted', None),
            # START_OR_END_OF_QUOTED_CELL -> no newline after 21
            (b'quoted,",not quoted,"quoted"', None),
            # START_OR_END_OF_QUOTED_CELL -> walk 21 -> 29
            (b'quoted,",not quoted,"quoted"\n', 28),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no walking
            (b'unquoted\n",quoted not closed', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no newline after 29
            (b'unquoted\n",quoted and closed",unquoted', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> walk from 29 -> 38
            (b'unquoted\n",quoted and closed",unquoted\n', 38),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no walking
            (b'unquoted,"\nunquoted', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> walk from 36 -> 44
            (b'unquoted,"\nunquoted,but,eventually,"quoted,"\n', 44),
            # START_OF_QUOTED_LINE -> walk from 22 -> 22
            (b'this,is,a,quoted,field\n"csv,"and,another"\n', 22),
        ]

        for buffer, expected_line_term_index in tests:
            actual_line_term_index = find_line_terminator_for_quoted_csv(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                line_terminator=self.line_term,
            )
            assert actual_line_term_index == expected_line_term_index

    def test_find_line_terminator_for_quoted_csv_complex(self) -> None:
        tests = [
            # START_QUOTED_CELL -> walk from 44 -> 48
            (b'unquoted field,"""this is a quoted field\n","csv"\nfun,', 48),
            # END_QUOTED_CELL -> no newline after 42
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun,f', None),
            # END_QUOTED_CELL -> walk from 42 -> 46
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun\nf', 46),
            # END_OF_QUOTED_LINE -> walk from 44 -> 44
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', 44),
            # START_OF_QUOTED_LINE -> walk from 34 -> 34
            (b',"","","","""","""""","""\n""",""""\n"aaa","","",""', 34),
        ]
        for buffer, expected_line_term_index in tests:
            actual_line_term_index = find_line_terminator_for_quoted_csv(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                line_terminator=self.line_term,
            )
            assert actual_line_term_index == expected_line_term_index
