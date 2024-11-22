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
    CARRIAGE_RETURN,
    DEFAULT_CSV_LINE_TERMINATOR,
    DEFAULT_CSV_QUOTE_CHAR,
    DEFAULT_CSV_SEPARATOR,
)
from recidiviz.utils.quoted_csv_line_terminator_finder import (
    determine_quoting_state_for_buffer,
    find_end_of_line_for_quoted_csv,
    walk_to_find_line_end,
)


class TestWalkToFindLineEnd(unittest.TestCase):
    """Unit tests for walk_to_find_line_end"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    newline = [DEFAULT_CSV_LINE_TERMINATOR.encode()]
    two_line_terms = [DEFAULT_CSV_LINE_TERMINATOR.encode(), CARRIAGE_RETURN.encode()]

    def test_walk_to_find_unquoted_line_term_empty(self) -> None:
        tests = [
            (b"", False, None),
            (b"", True, None),
            (b"aaaaaaaa", False, None),
            (b"aaaaaaaa", True, None),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminators=self.newline,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_simple(self) -> None:
        tests = [
            (b"this,is,a,simple,unquoted,csv\nfun,fun,fun", False, 30),
            (b'this,is,a,simple,"quoted\n","csv"\nfun,"fun",fun', False, 33),
            (b'this,is,a,quoted,field",csv\n"and,another"\n', True, 28),
            (b'\nthis,is,a,"newline"', False, 1),
            (b'this","is","a","fully","quoted","csv"\n"fun","fun","fun"', True, 38),
            (b"this,is,all,a,quoted,field\nfun,fun,fun", True, None),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminators=self.newline,  # just \n
            )
            assert actual_line_terminator_index == expected_line_terminator_index

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminators=self.two_line_terms,  # with \n and \r\n is the same
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_tricky(self) -> None:
        tests = [
            (b'unquoted field,"""this is a quoted field\n""",csv\nfun,', False, 49),
            (b'unquoted,"\n""\n""\n""this is a quoted field"""""""\nfun,f', False, 49),
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', True, 45),
            (b',"","","","""","""""","""\n""",""""\n"","","",""', False, 35),
        ]

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminators=self.newline,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

        for buffer, in_quoted_cell, expected_line_terminator_index in tests:
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=self.quote_char,
                line_terminators=self.two_line_terms,
            )
            assert actual_line_terminator_index == expected_line_terminator_index

    def test_walk_to_find_unquoted_line_term_alt_values(self) -> None:
        tests = [
            (b"'", b"\r\n", b"this,only,has,a,newline,not,the,term,csv\n", False, None),
            (b"'", b"\r\n", b"this,is,a,simple,unquoted,csv\r\nfun,fun,fun", False, 31),
            (b"'", b"\r\n", b"unquoted \n <-not one... but is one->\r\n", False, 38),
            (
                b"'",
                b"\r\n",
                b"this,is,a,simple,'quoted\r\n','csv'\r\nfun,'fun',fun",
                False,
                35,
            ),
            (
                b"'",
                b"\r\n",
                b"this,is,a,quoted,field',csv\r\n'and,another'\r\n",
                True,
                29,
            ),
            (b"'", b"\r\n", b"\r\nthis,is,a,'newline'", False, 2),
            (
                b"'",
                b"\r\n",
                b"this','is','a','fully','quoted','csv'\r\n'fun'",
                True,
                39,
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
            actual_line_terminator_index = walk_to_find_line_end(
                buffer=buffer,
                in_quoted_cell=in_quoted_cell,
                quote_char=quote_char,
                line_terminators=[line_term],
            )
            assert actual_line_terminator_index == expected_line_terminator_index


class TestDetermineQuotingStateForBuffer(unittest.TestCase):
    """Unit tests for determine_quoting_state_for_buffer"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    newline = [DEFAULT_CSV_LINE_TERMINATOR.encode()]
    carriage = [CARRIAGE_RETURN.encode()]
    separator = DEFAULT_CSV_SEPARATOR.encode()
    two_line_terms = [DEFAULT_CSV_LINE_TERMINATOR.encode(), CARRIAGE_RETURN.encode()]

    def test_invalid_identical(self) -> None:
        invalid_tests = [
            ("\n", "\n"),
            ("\r\n", "\r\n"),
        ]
        for line_term, alt_line_term in invalid_tests:
            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, they must not be identical. Instead, found:.*",
            ):
                determine_quoting_state_for_buffer(
                    buffer=b"",
                    buffer_byte_start=0,
                    quote_char=self.quote_char,
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=[line_term.encode(), alt_line_term.encode()],
                )

    def test_invalid(self) -> None:
        invalid_tests = [
            ("‡\n", "\r\n"),
            ("‡\n", "\n"),
        ]
        for line_term, alt_line_term in invalid_tests:
            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, the line terminators MUST be identical except for the newline \[\\n\] and carriage return \[\\r\\n\]. Instead, found:.*",
            ):
                determine_quoting_state_for_buffer(
                    buffer=b"",
                    buffer_byte_start=0,
                    quote_char=self.quote_char,
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=[line_term.encode(), alt_line_term.encode()],
                )

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
            (b'this,is,a,quoted,field\n"csv,"and,another"\n', True, 24),
            (b'this,is,a,quoted,field\n"""csv,"and,another"\n', True, 26),
        ]

        for buffer, expected_in_quoted_cell, expected_cursor in tests:
            result = determine_quoting_state_for_buffer(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                encoding="utf-8",
                line_terminators=self.newline,
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
            (b',"","","","""","""""","""\n""",""""\n"aaa","","",""', True, 36),
        ]

        for buffer, expected_in_quoted_cell, expected_cursor in tests:
            result = determine_quoting_state_for_buffer(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                encoding="utf-8",
                line_terminators=self.newline,
            )
            if result is None:
                assert result == expected_in_quoted_cell
                assert result == expected_cursor
            else:
                actual_cursor, actual_in_quoted_cell = result
                assert actual_in_quoted_cell == expected_in_quoted_cell
                assert actual_cursor == expected_cursor

    def test_determine_quoting_state_for_buffer_carriage(self) -> None:
        tests = [
            # START_QUOTED_CELL
            (b'unquoted field,""",this is a quoted field\n","csv"\nfun,', True, 45),
            (b'unquoted field,""",this is a quoted field\r\n","csv"\r\nfun,', True, 46),
            # END_QUOTED_CELL
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun,f', False, 42),
            (b'unquoted,"\r\n""\r\n""\r\n""this is a quoted field",fun,f', False, 45),
            # END_OF_QUOTED_LINE
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', False, 44),
            (b'in a \r\nquoted \r\nfield "" <--- that\'s escaped "\r\nwaho', False, 46),
            # START_OF_QUOTED_LINE
            (b',"","","","""","""""","""\n""",""""\n"aaa","","",""', True, 36),
            (b',"","","","""","""""","""\r\n""",""""\r\n"aaa","","",""', True, 38),
        ]

        for buffer, expected_in_quoted_cell, expected_cursor in tests:
            result = determine_quoting_state_for_buffer(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                encoding="utf-8",
                line_terminators=self.two_line_terms,
            )
            if result is None:
                assert result == expected_in_quoted_cell
                assert result == expected_cursor
            else:
                actual_cursor, actual_in_quoted_cell = result
                assert actual_in_quoted_cell == expected_in_quoted_cell
                assert actual_cursor == expected_cursor


class TestFindEndOfLineForQuotedCSV(unittest.TestCase):
    """Unit tests for find_end_of_line_for_quoted_csv"""

    quote_char = DEFAULT_CSV_QUOTE_CHAR.encode()
    newline = [DEFAULT_CSV_LINE_TERMINATOR.encode()]
    separator = DEFAULT_CSV_SEPARATOR.encode()

    def test_invalid_identical(self) -> None:
        invalid_tests = [
            ("\n", "\n"),
            ("\r\n", "\r\n"),
        ]
        for line_term, alt_line_term in invalid_tests:
            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, they must not be identical. Instead, found:.*",
            ):
                determine_quoting_state_for_buffer(
                    buffer=b"",
                    buffer_byte_start=0,
                    quote_char=self.quote_char,
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=[line_term.encode(), alt_line_term.encode()],
                )

    def test_invalid(self) -> None:
        invalid_tests = [
            ("‡\n", "\r\n"),
            ("‡\n", "\n"),
        ]
        for line_term, alt_line_term in invalid_tests:
            with self.assertRaisesRegex(
                ValueError,
                r"If you are specifying multiple line terminators, the line terminators MUST be identical except for the newline \[\\n\] and carriage return \[\\r\\n\]. Instead, found:.*",
            ):
                determine_quoting_state_for_buffer(
                    buffer=b"",
                    buffer_byte_start=0,
                    quote_char=self.quote_char,
                    separator=self.separator,
                    encoding="utf-8",
                    line_terminators=[line_term.encode(), alt_line_term.encode()],
                )

    def test_find_line_terminator_for_quoted_csv_simple(self) -> None:
        tests = [
            # none
            (b"this,is,a,simple,unquoted,csv\nfun,fun,fun", None),
            (b"this,is,all,a,quoted,field\nfun,fun,fun", None),
            # START_QUOTED_CELL -> walk from 18 to 33
            (b'this,is,a,simple,"quoted\n","csv"\nfun,"fun",fun', 33),
            # START_QUOTED_CELL -> no newline after 12
            (b'\nthis,is,a,"newline"', None),
            # END_QUOTED_CELL -> walk from 23 to 28
            (b'this,is,a,quoted,field",csv\n"and,another"\n', 28),
            # END_QUOTED_CELL -> walk from 5 to 38
            (b'this","is","a","fully","quoted","csv"\n"fun","fun","fun"', 38),
            # END_OF_QUOTED_LINE -> walk from 23 to 24
            (b'this,is,a,quoted,field"\ncsv,"and,another"\n', 24),
            # START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE -> no walking
            (b'quoted,"\ncsv', None),
            # START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE -> no newline after 14
            (b'quoted,"\ncsv,"hello!', None),
            # START_OR_END_OF_QUOTED_CELL -> no walking
            (b'quoted,",not quoted', None),
            # START_OR_END_OF_QUOTED_CELL -> no newline after 21
            (b'quoted,",not quoted,"quoted"', None),
            # START_OR_END_OF_QUOTED_CELL -> walk 21 -> 29
            (b'quoted,",not quoted,"quoted"\n', 29),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no walking
            (b'unquoted\n",quoted not closed', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no newline after 29
            (b'unquoted\n",quoted and closed",unquoted', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> walk from 29 -> 39
            (b'unquoted\n",quoted and closed",unquoted\n', 39),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> no walking
            (b'unquoted,"\nunquoted', None),
            # END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE -> walk from 36 -> 45
            (b'unquoted,"\nunquoted,but,eventually,"quoted,"\n', 45),
            # START_OF_QUOTED_LINE -> no newline after 24
            (b'this,is,a,quoted,field\n"csv,"and,another"', None),
            # START_OF_QUOTED_LINE -> no unquoted newline after 24
            (b'this,is,a,quoted,field\n"csv,",and,another -> "\n', None),
            # START_OF_QUOTED_LINE -> walk from 24 -> 37
            (b'this,is,a,quoted,field\n"and,another"\n', 37),
        ]

        for buffer, expected_line_term_index in tests:
            actual_line_term_index = find_end_of_line_for_quoted_csv(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                encoding="utf-8",
                line_terminators=self.newline,
            )
            assert actual_line_term_index == expected_line_term_index

    def test_find_line_terminator_for_quoted_csv_complex(self) -> None:
        tests = [
            # START_QUOTED_CELL -> walk from 44 -> 49
            (b'unquoted field,"""this is a quoted field\n","csv"\nfun,', 49),
            # END_QUOTED_CELL -> no newline after 42
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun,f', None),
            # END_QUOTED_CELL -> walk from 42 -> 47
            (b'unquoted,"\n""\n""\n""this is a quoted field",fun\nf', 47),
            # END_OF_QUOTED_LINE -> walk from 44 -> 45
            (b'in a \nquoted \nfield "" <--- that\'s escaped "\nwahoooo', 45),
            # START_OF_QUOTED_LINE -> no newline after 34
            (b',"","","","""","""""","""\n""",""""\n"aaa","","",""', None),
            # START_OF_QUOTED_LINE -> walk from 34 -> 41
            (b',"","","","""","""""","""\n""",""""\n"aaa"\n"","",""', 41),
        ]
        for buffer, expected_line_term_index in tests:
            print(buffer)
            actual_line_term_index = find_end_of_line_for_quoted_csv(
                buffer=buffer,
                buffer_byte_start=0,
                quote_char=self.quote_char,
                separator=self.separator,
                encoding="utf-8",
                line_terminators=self.newline,
            )
            assert actual_line_term_index == expected_line_term_index
