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
"""Constants for csv files"""
from recidiviz.common.constants.encoding import UTF_8

DOUBLE_QUOTE = '"'
SINGLE_QUOTE = "'"
COMMA = ","
NEWLINE = "\n"
CARRIAGE_RETURN = "\r\n"


VALID_QUOTE_CHARS = {DOUBLE_QUOTE, SINGLE_QUOTE}
VALID_MULTIPLE_LINE_TERMINATORS = {NEWLINE, CARRIAGE_RETURN}

DEFAULT_CSV_LINE_TERMINATOR = NEWLINE
DEFAULT_CSV_ENCODING = UTF_8
DEFAULT_CSV_SEPARATOR = COMMA
DEFAULT_CSV_QUOTE_CHAR = DOUBLE_QUOTE
