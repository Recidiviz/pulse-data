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
"""Valid codec error handlers that can be registered with codecs.register_error"""
import logging

import attr

from recidiviz.common.constants.encoding import UTF_8
from recidiviz.utils.encoding import to_python_standard

ASCII_QUESTION_MARK = b"?"
UTF_REPLACEMENT_CHAR = "�"


class ExceededDecodingErrorThreshold(ValueError):
    pass


def get_replacement_char(encoding: str, replacement_override: bytes | None) -> str:
    """If |replacement_override| is provided, will return use |encoding| to decode
    replacement_override. If the encoding is utf-8, we will use the utf replacement char;
    otherwise, we will default to the ASCII question mark.
    """
    if replacement_override:
        return replacement_override.decode(encoding)

    if to_python_standard(encoding) == UTF_8:
        return UTF_REPLACEMENT_CHAR

    return ASCII_QUESTION_MARK.decode(encoding)


@attr.define
class UnparseableBytes:
    start_byte: int
    end_byte: int
    unparseable_bytes: bytes
    encoding: str

    def __str__(self) -> str:
        return f"[{self.unparseable_bytes!r}] with encoding [{self.encoding}] between [{self.start_byte}] and [{self.end_byte}]"

    @classmethod
    def from_decode_error(cls, err: UnicodeDecodeError) -> "UnparseableBytes":
        return cls(
            start_byte=err.start,
            end_byte=err.end,
            unparseable_bytes=err.object[err.start : err.end],
            encoding=err.encoding,
        )


class LimitedErrorReplacementHandler:
    """Class for limiting the number of errors seen for a particular input stream,
    replacing all unparseable bytes with the specified |replace_char| (defaults to ascii
    question mark or )"""

    def __init__(
        self, max_number_of_errors: int, replace_char: bytes | None = None
    ) -> None:
        # we create UnparseableBytes objects instead of just storing the UnicodeDecodeError
        # because the error can store the whole input buffer which we do not want to
        # store
        self.exceptions: list[UnparseableBytes] = []
        self._replace_char: bytes | None = replace_char
        self._max_number_of_errors = max_number_of_errors

    def __call__(self, err: UnicodeError) -> tuple[str | bytes, int]:
        if not isinstance(err, UnicodeDecodeError):
            raise ValueError(f"{self} is only configured to handle decode errors")

        ub = UnparseableBytes.from_decode_error(err)
        logging.info(ub)
        self.exceptions.append(ub)

        if len(self.exceptions) > self._max_number_of_errors:
            error_str = "\n".join(f"\t- {e}" for e in self.exceptions)
            raise ExceededDecodingErrorThreshold(
                f"Exceeded max number of decoding errors [{self._max_number_of_errors}]:\n{error_str}"
            )
        return get_replacement_char(err.encoding, self._replace_char), err.end
