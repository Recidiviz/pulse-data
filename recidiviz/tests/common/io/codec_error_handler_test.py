# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Unit tests for codec error handlers"""
import codecs
import io
from unittest import TestCase

from recidiviz.common.io.codec_error_handler import (
    ExceededDecodingErrorThreshold,
    LimitedErrorReplacementHandler,
)


class TestLimitedErrorReplacementHandler(TestCase):
    """Unit tests for limited error replacement!"""

    def test_no_errors(self) -> None:
        handler = LimitedErrorReplacementHandler(max_number_of_errors=1)
        codecs.register_error("test_empty", handler)

        to_decode = b"this is all good, i literally PROMISE"
        expected = "this is all good, i literally PROMISE"

        decoder = io.TextIOWrapper(
            io.BytesIO(to_decode), encoding="utf-8", errors="test_empty"
        )

        assert decoder.read() == expected
        assert not handler.exceptions

    def test_some_errors(self) -> None:
        handler = LimitedErrorReplacementHandler(max_number_of_errors=3)
        codecs.register_error("test_some_errors", handler)

        to_decode = b"this is only sort of good, see:\x87\x87~~"
        expected = "this is only sort of good, see:��~~"

        decoder = io.TextIOWrapper(
            io.BytesIO(to_decode), encoding="utf-8", errors="test_some_errors"
        )
        assert decoder.read() == expected
        assert handler.exceptions == [
            "'utf-8' codec can't decode byte 0x87 in position 31: invalid start byte",
            "'utf-8' codec can't decode byte 0x87 in position 32: invalid start byte",
        ]

    def test_more_than_zero(self) -> None:
        handler = LimitedErrorReplacementHandler(max_number_of_errors=0)
        codecs.register_error("test_more_than_allowed_errors", handler)

        to_decode = b"this is only sort of good, see:\x87~~"

        decoder = io.TextIOWrapper(
            io.BytesIO(to_decode),
            encoding="utf-8",
            errors="test_more_than_allowed_errors",
        )

        with self.assertRaisesRegex(
            ExceededDecodingErrorThreshold,
            r"Exceeded max number of decoding errors \[0\]:\n\t- 'utf-8' codec can't decode byte 0x87 in position 31: invalid start byte",
        ):
            decoder.read()

    def test_more_than_allowed_errors(self) -> None:
        handler = LimitedErrorReplacementHandler(max_number_of_errors=1)
        codecs.register_error("test_more_than_allowed_errors", handler)

        to_decode = b"this is only sort of good, see:\x87\x87~~"

        decoder = io.TextIOWrapper(
            io.BytesIO(to_decode),
            encoding="utf-8",
            errors="test_more_than_allowed_errors",
        )

        with self.assertRaisesRegex(
            ExceededDecodingErrorThreshold,
            r"Exceeded max number of decoding errors \[1\]:\n\t- 'utf-8' codec can't decode byte 0x87 in position 31: invalid start byte\n\t- 'utf-8' codec can't decode byte 0x87 in position 32: invalid start byte",
        ):
            decoder.read()

    def test_multi_byte_errors(self) -> None:
        handler = LimitedErrorReplacementHandler(max_number_of_errors=3)
        codecs.register_error("test_multi_byte_errors", handler)

        to_decode = b"this is a valid double dagger: [\xe2\x80\xa1] this is not: [\xe2\x28\xa1] this is: [\xe2\x80\xa1]"
        expected = "this is a valid double dagger: [‡] this is not: [�(�] this is: [‡]"

        decoder = io.TextIOWrapper(
            io.BytesIO(to_decode), encoding="utf-8", errors="test_multi_byte_errors"
        )
        assert decoder.read() == expected
        assert handler.exceptions == [
            "'utf-8' codec can't decode byte 0xe2 in position 51: invalid continuation byte",
            "'utf-8' codec can't decode byte 0xa1 in position 53: invalid start byte",
        ]
