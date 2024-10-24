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
"""Constants for non-standard characters."""

# Non-standard Unicode space characters
NON_BREAKING_SPACE = chr(0x00A0)  # Non-breaking space
OGHAM_SPACE_MARK = chr(0x1680)  # Ogham space mark
EN_QUAD = chr(0x2000)  # En quad
EM_QUAD = chr(0x2001)  # Em quad
EN_SPACE = chr(0x2002)  # En space
EM_SPACE = chr(0x2003)  # Em space
THREE_PER_EM_SPACE = chr(0x2004)  # Three-per-em space
FOUR_PER_EM_SPACE = chr(0x2005)  # Four-per-em space
SIX_PER_EM_SPACE = chr(0x2006)  # Six-per-em space
FIGURE_SPACE = chr(0x2007)  # Figure space
PUNCTUATION_SPACE = chr(0x2008)  # Punctuation space
THIN_SPACE = chr(0x2009)  # Thin space
HAIR_SPACE = chr(0x200A)  # Hair space
NARROW_NO_BREAK_SPACE = chr(0x202F)  # Narrow no-break space
MEDIUM_MATHEMATICAL_SPACE = chr(0x205F)  # Medium mathematical space
IDEOGRAPHIC_SPACE = chr(0x3000)  # Ideographic space

NON_STANDARD_UNICODE_SPACE_CHARACTERS = {
    NON_BREAKING_SPACE,
    OGHAM_SPACE_MARK,
    EN_QUAD,
    EM_QUAD,
    EN_SPACE,
    EM_SPACE,
    THREE_PER_EM_SPACE,
    FOUR_PER_EM_SPACE,
    SIX_PER_EM_SPACE,
    FIGURE_SPACE,
    PUNCTUATION_SPACE,
    THIN_SPACE,
    HAIR_SPACE,
    NARROW_NO_BREAK_SPACE,
    MEDIUM_MATHEMATICAL_SPACE,
    IDEOGRAPHIC_SPACE,
}

# ASCII control characters hex codes
ASCII_NULL = r"\x00"  # Null (NUL)
ASCII_SOH = r"\x01"  # Start of Heading (SOH)
ASCII_STX = r"\x02"  # Start of Text (STX)
ASCII_ETX = r"\x03"  # End of Text (ETX)
ASCII_EOT = r"\x04"  # End of Transmission (EOT)
ASCII_ENQ = r"\x05"  # Enquiry (ENQ)
ASCII_ACK = r"\x06"  # Acknowledge (ACK)
ASCII_BEL = r"\x07"  # Bell (BEL)
ASCII_BS = r"\x08"  # Backspace (BS)
ASCII_HT = r"\x09"  # Horizontal Tab (HT)
ASCII_VT = r"\x0B"  # Vertical Tab (VT)
ASCII_FF = r"\x0C"  # Form Feed (FF)
ASCII_LF = r"\x0A"  # Line Feed (LF)
ASCII_CR = r"\x0D"  # Carriage Return (CR)
ASCII_SO = r"\x0E"  # Shift Out (SO)
ASCII_SI = r"\x0F"  # Shift In (SI)
ASCII_DLE = r"\x10"  # Data Link Escape (DLE)
ASCII_DC1 = r"\x11"  # Device Control 1 (DC1)
ASCII_DC2 = r"\x12"  # Device Control 2 (DC2)
ASCII_DC3 = r"\x13"  # Device Control 3 (DC3)
ASCII_DC4 = r"\x14"  # Device Control 4 (DC4)
ASCII_NAK = r"\x15"  # Negative Acknowledge (NAK)
ASCII_SYN = r"\x16"  # Synchronous Idle (SYN)
ASCII_ETB = r"\x17"  # End of Transmission Block (ETB)
ASCII_CAN = r"\x18"  # Cancel (CAN)
ASCII_EM = r"\x19"  # End of Medium (EM)
ASCII_SUB = r"\x1A"  # Substitute (SUB)
ASCII_ESC = r"\x1B"  # Escape (ESC)
ASCII_FS = r"\x1C"  # File Separator (FS)
ASCII_GS = r"\x1D"  # Group Separator (GS)
ASCII_RS = r"\x1E"  # Record Separator (RS)
ASCII_US = r"\x1F"  # Unit Separator (US)
ASCII_DEL = r"\x7F"  # Delete (DEL)

ASCII_CONTROL_CHARS_HEX_CODES = {
    ASCII_NULL,
    ASCII_SOH,
    ASCII_STX,
    ASCII_ETX,
    ASCII_EOT,
    ASCII_ENQ,
    ASCII_ACK,
    ASCII_BEL,
    ASCII_BS,
    ASCII_HT,
    ASCII_VT,
    ASCII_FF,
    ASCII_LF,
    ASCII_CR,
    ASCII_SO,
    ASCII_SI,
    ASCII_DLE,
    ASCII_DC1,
    ASCII_DC2,
    ASCII_DC3,
    ASCII_DC4,
    ASCII_NAK,
    ASCII_SYN,
    ASCII_ETB,
    ASCII_CAN,
    ASCII_EM,
    ASCII_SUB,
    ASCII_ESC,
    ASCII_FS,
    ASCII_GS,
    ASCII_RS,
    ASCII_US,
    ASCII_DEL,
}

STANDARD_ASCII_CONTROL_CHARS_HEX_CODES = {
    ASCII_HT,
    ASCII_LF,
    ASCII_CR,
}

NON_STANDARD_ASCII_CONTROL_CHARS_HEX_CODES = (
    ASCII_CONTROL_CHARS_HEX_CODES - STANDARD_ASCII_CONTROL_CHARS_HEX_CODES
)
