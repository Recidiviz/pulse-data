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
"""Utils for safely finding line terminators in quoted CSV files"""

from recidiviz.utils.unescaped_quote_classification import (
    UnescapedQuoteState,
    find_first_unescaped_quote,
    validate_line_terminators,
)


def determine_quoting_state_for_buffer(
    *,
    buffer: bytes,
    buffer_byte_start: int,
    quote_char: bytes,
    separator: bytes,
    encoding: str,
    line_terminators: list[bytes],
) -> tuple[int, bool] | None:
    """Searches |buffer| to find a single, unescaped quote that we can use to determine
    where in the file we are. Returns values are:
        - cursor in |buffer| of the next non-quote character
        - if our cursor is inside of a quoted cell
    """
    validate_line_terminators(encoding=encoding, line_terminators=line_terminators)

    peek_into_buffer_size = max(
        len(quote_char),
        len(separator),
        *[len(line_terminator) for line_terminator in line_terminators],
    )

    cursor = 0
    in_quoted_cell: bool | None = None

    # first: we find the next single, unescaped quote we can find
    while (
        in_quoted_cell is None
        and (
            next_unescaped_quote := find_first_unescaped_quote(
                buffer=buffer,
                cursor=cursor,
                quote_char=quote_char,
                min_buffer_peek=peek_into_buffer_size,
            )
        )
        is not None
    ):
        # next: we determine the quote's state, or what that quote means about where
        # we are within the csv file
        quote_state = next_unescaped_quote.get_quote_state(
            separator=separator, encoding=encoding, line_terminators=line_terminators
        )
        # then: we determine if that state deterministically tells us if we are in
        # a quoted block or not
        match quote_state:
            case UnescapedQuoteState.START_QUOTED_CELL:
                in_quoted_cell = True
            case UnescapedQuoteState.END_QUOTED_CELL:
                in_quoted_cell = False
            case UnescapedQuoteState.END_OF_QUOTED_LINE:
                in_quoted_cell = False
            case UnescapedQuoteState.START_OF_QUOTED_LINE:
                in_quoted_cell = True
            # with the other cases, we cannot be confident of whether we are in a
            # quoted cell or not, so we must keep looking
            case UnescapedQuoteState.START_OF_QUOTED_CELL_OR_END_OF_QUOTED_LINE:
                pass
            case UnescapedQuoteState.START_OR_END_OF_QUOTED_CELL:
                pass
            case UnescapedQuoteState.END_OF_QUOTED_CELL_OR_START_OF_QUOTED_LINE:
                pass
            case UnescapedQuoteState.INDETERMINATE:
                pass
            case UnescapedQuoteState.INVALID:
                raise ValueError(
                    f"Found invalid quote "
                    f"[{buffer[next_unescaped_quote.index - peek_into_buffer_size:next_unescaped_quote.next_non_quote_char_position + peek_into_buffer_size]!r}]"
                    f"at buffer byte offset: [{buffer_byte_start + next_unescaped_quote.index}]."
                    f"If we are in a quoted mode all quotes that are not next to "
                    f"separators or newlines must be escaped. If you are seeing this"
                    f"message, we either have a bug in our quote finding code or "
                    f"the csv is improperly quoted."
                )

        cursor = next_unescaped_quote.next_non_quote_char_position

    # TODO(#30505) we can be smarter about where we make our educated guess where the
    # line term is if we cant definitively find it

    # lastly: if we haven't found a state that we can work with, we give up
    if in_quoted_cell is None or next_unescaped_quote is None:
        return None

    # if we have found a state that we can work with, we return it!
    return cursor, in_quoted_cell


def walk_to_find_line_end(
    *,
    buffer: bytes,
    in_quoted_cell: bool,
    quote_char: bytes,
    line_terminators: list[bytes],
) -> int | None:
    """Searches |buffer| for the first line terminator outside of a quote cell by
    flipping |in_quoted_cell| each time we encounter a quote in |buffer| until we find a
    line terminator that is not in a quoted cell.
    """
    peek_size_into_buffer = max(
        len(quote_char),
        *[len(line_terminator) for line_terminator in line_terminators],
    )

    for i in range(len(buffer)):
        next_bytes = buffer[i : i + peek_size_into_buffer]
        # toggle the in_quotes flag when encountering our quote char. if it is an
        # escaped quote (i.e. even count) this flag will remain the same
        if next_bytes.startswith(quote_char):
            in_quoted_cell = not in_quoted_cell
        else:
            for line_terminator in line_terminators:
                # if we find a newline and we're not inside quotes, we're good to go!
                if next_bytes.startswith(line_terminator) and not in_quoted_cell:
                    return i + len(line_terminator)

    return None


def find_end_of_line_for_quoted_csv(
    *,
    buffer: bytes,
    buffer_byte_start: int,
    quote_char: bytes,
    separator: bytes,
    encoding: str,
    line_terminators: list[bytes],
) -> int | None:
    """Searches |buffer| in an attempt to find an line terminator for a conceptual csv
    line (i.e. a |line_terminator| or |alt_line_terminator| character that is not inside of
    a quoted cell.) We do so by identifying any unescaped quotes to find a deterministic
    csv state in |buffer| and walking the rest of |buffer| until we find a |line_terminator|
    or |alt_line_terminator| we are confident is not in a quoted field.
    """
    validate_line_terminators(encoding=encoding, line_terminators=line_terminators)

    # Find the index after the first unescaped quote character which gives us enough context
    #  to understand if we're inside or outside a quoted block of characters.
    quoting_state = determine_quoting_state_for_buffer(
        buffer=buffer,
        buffer_byte_start=buffer_byte_start,
        quote_char=quote_char,
        separator=separator,
        encoding=encoding,
        line_terminators=line_terminators,
    )

    if quoting_state is None:
        return None

    post_quote_cursor, in_quoted_cell = quoting_state

    # Walk the buffer to find the index of the first conceptual CSV line terminator (e.g.
    # first line terminator not inside a quoted block).
    relative_line_term_index = walk_to_find_line_end(
        buffer=buffer[post_quote_cursor:],
        in_quoted_cell=in_quoted_cell,
        quote_char=quote_char,
        line_terminators=line_terminators,
    )

    if relative_line_term_index is None:
        return None

    return post_quote_cursor + relative_line_term_index
