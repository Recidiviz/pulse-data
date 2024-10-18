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
"""Utils for finding and classifying unescaped quotes."""
from enum import Enum

import attr


class SingleUnescapedQuoteState(Enum):
    """Represents the different roles a single, unescaped quote can be playing in a
    quoted CSV file.
    """

    START_QUOTED_CELL = "start_quoted_cell"
    END_QUOTED_CELL = "end_quoted_cell"

    # the quote here looks like ,",  --- don't know if the comma before or after is
    # being escaped
    START_OR_END_OF_QUOTED_CELL = "start_or_end_of_quoted_cell"

    END_OF_QUOTED_LINE = "end_of_quoted_line"
    START_OF_QUOTED_LINE = "start_of_quoted_line"

    # the quote here looks like \n", or \n"\n --- don't know which side is being escaped
    END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE = (
        "end_of_quoted_cell_or_start_of_quoted_line"
    )

    # the quote here looks like ,"\n --- don't know which side is being escaped
    START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE = (
        "start_of_quoted_cell_or_end_of_quoted_line"
    )

    # used to describe a unescaped quote that should not exist in a quoted csv
    # (i.e. x"x) -- this likely means that the csv is malformed
    INVALID = "invalid"
    # used when we dont have enough buffer to determine the state of the unescaped quote
    INDETERMINATE = "indeterminate"


@attr.define(kw_only=True)
class SingleUnescapedQuote:
    """Encapsulates the location and surrounding context for a single, unescaped quote
    that is surrounded by non-quote characters.
    """

    # The index of the start of this unescaped quote in the overall buffer
    index: int

    # The buffer of non-quote chars that directly precedes the unescaped quote
    prev_chars: bytes

    # The buffer of non-quote chars that directly follows the unescaped quote
    next_chars: bytes

    # The quote character(s)
    quote_char: bytes

    @property
    def next_non_quote_char_position(self) -> int:
        """Returns the index directly preceding the unescaped quote."""
        return self.index + len(self.quote_char)

    def get_quote_state(
        self, *, line_term: bytes, separator: bytes
    ) -> SingleUnescapedQuoteState:
        """Determines SingleUnescapedQuoteState of the SingleUnescapedQuote. Because of how we
        identify unescaped quotes, we can safely assume that there are no quote chars
        in the bytes directly preceding or succeeding the SingleUnescapedQuote.
        """
        required_peek = max(len(line_term), len(separator), len(self.quote_char))
        has_prev_chars = len(self.prev_chars) >= required_peek
        has_next_chars = len(self.next_chars) >= required_peek

        if not has_next_chars or not has_prev_chars:
            # if we dont know what came before or after the quote, that means we are
            # either at the start of end of the file/peeked buffer. we can't be sure
            # where we are.
            return SingleUnescapedQuoteState.INDETERMINATE

        previous_character_is_separator = (
            self.prev_chars[-1 * len(separator) :] == separator
        )
        previous_character_is_line_term = (
            self.prev_chars[-1 * len(line_term) :] == line_term
        )
        next_character_is_separator = self.next_chars[: len(separator)] == separator
        next_character_is_line_term = self.next_chars[: len(line_term)] == line_term

        # cases that start like ,"
        if previous_character_is_separator:
            if next_character_is_separator:
                # case: ,", so it could be the start or end of a quoted cell
                # and we have no way to knowing so we need to keep on looking
                return SingleUnescapedQuoteState.START_OR_END_OF_QUOTED_CELL
            if next_character_is_line_term:
                # case: ,"\n which means we are at the end of a line! yay!
                return (
                    SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE
                )

            # case: ,"x which means we are at the start of a quoted cell!
            return SingleUnescapedQuoteState.START_QUOTED_CELL

        # cases that start like \n"
        if previous_character_is_line_term:
            if next_character_is_separator:
                # case: \n", which means we are either:
                #   - at the end of a quoted cell (much, much more likely)
                #   - at the start of a new line
                return (
                    SingleUnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE
                )
            if next_character_is_line_term:
                # case: \n"\n which means we are either:
                #   - at the end of a quoted field + line break (i think more likely)
                #   - at the end of a line + start of a quoted field
                return (
                    SingleUnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE
                )

            # case: \n"d which means this is the start of a new line!
            return SingleUnescapedQuoteState.START_OF_QUOTED_LINE

        # cases that start like x"
        if not previous_character_is_line_term and not previous_character_is_separator:
            if next_character_is_separator:
                # case: x", which means end of quoted cell!
                return SingleUnescapedQuoteState.END_QUOTED_CELL
            if next_character_is_line_term:
                # case: x"\n which means end of line!
                return SingleUnescapedQuoteState.END_OF_QUOTED_LINE

            # case is like x"x which is just wacky, you cant do that sort of thing to
            # quotes in a quoted csv!!!
            return SingleUnescapedQuoteState.INVALID

        raise ValueError(
            "Reached unexpected state -- we should have been able to identify the state"
            "of where out quote sits within the characters around it"
        )

    @classmethod
    def none(cls) -> "SingleUnescapedQuote":
        """Returns an SingleUnescapedQuote object for when not quote is found"""
        return cls(index=-1, prev_chars=b"", next_chars=b"", quote_char=b"")


def find_single_first_unescaped_quote(
    buffer: bytes, quote_char: bytes, min_buffer_peek: int
) -> SingleUnescapedQuote:
    '''Searches |buffer| to find the first single, "unescaped" |quote_char| we can find.
    n.b. we will skip all non-single quotes, which means we are skipping:
        - double quotes ("") -- we are skipping these as they are either (a) an escaped
          quote or (b) a blank field. in both cases we'd expect to find a single quote,
          in case (a) because of max cell size and (b) because this likely means many
          fields are full quoted.
        - lines that start of end with quotes such as ("""). it's a little bit
          trickier to reason about what a multi-quote escaped quote means in the context
          of the surrounding characters. If it is the start of a field, the first
          quote is unescaped and then following quotes are not. If it is at the end of
          the field, the first n-1 quotes are escaped and the last quote is not.
          one improvement to this function and the SingleUnescapedQuote class is to
          handle these multi-quote escaped sequences with more confidence.
    '''
    if len(quote_char) > 1:
        raise ValueError("Cannot have a quote char that is more than a single byte")

    quote_count = 0
    cursor = 0
    while (relative_quote_index := buffer[cursor:].find(quote_char)) != -1:
        quote_count += 1
        absolute_quote_index = cursor + relative_quote_index
        while next_char := buffer[
            absolute_quote_index + quote_count : absolute_quote_index + quote_count + 1
        ]:
            if next_char != quote_char:
                break

            quote_count += 1

        if quote_count == 1:
            return SingleUnescapedQuote(
                index=absolute_quote_index,
                prev_chars=buffer[
                    max(
                        0, absolute_quote_index - min_buffer_peek
                    ) : absolute_quote_index
                ],
                next_chars=buffer[
                    (absolute_quote_index + 1) : (
                        absolute_quote_index + 1 + min_buffer_peek
                    )
                ],
                quote_char=quote_char,
            )

        # skip any quote count that is > 1
        cursor += relative_quote_index + quote_count
        quote_count = 0

    return SingleUnescapedQuote.none()
