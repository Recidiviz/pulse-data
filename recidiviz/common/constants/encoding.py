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
"""Constants for file encodings."""

# python "standard" file encodings. python's codec library has a lookup function that
# returns a "standard" file encoding for file aliases (think like latin-1 -> iso8859-1)
# for a full list, look at stdlib's encodings.aliases or https://docs.python.org/3/library/codecs.html#standard-encodings
UTF_8 = "utf-8"
ISO85591_1 = "iso8859-1"
UTF_16_BE = "utf-16-be"
UTF_16_LE = "utf-16-le"
UTF_32_BE = "utf-32-be"
UTF_32_LE = "utf-32-le"
ASCII = "ascii"

# big query's representation of iso85591-1 is iso-85591-1
BIG_QUERY_UTF_8 = "UTF-8"
BIG_QUERY_ISO85591_1 = "ISO-8859-1"
BIG_QUERY_UTF_16BE = "UTF_16BE"
BIG_QUERY_UTF_16LE = "UTF_16LE"
BIG_QUERY_UTF_32BE = "UTF_32BE"
BIG_QUERY_UTF_32LE = "UTF_32LE"

# per the encodings listed https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#encoding
PYTHON_STANDARD_ENCODINGS_TO_BIG_QUERY_ENCODING = {
    UTF_8: BIG_QUERY_UTF_8,
    ISO85591_1: BIG_QUERY_ISO85591_1,
    UTF_16_BE: BIG_QUERY_UTF_16BE,
    UTF_16_LE: BIG_QUERY_UTF_16LE,
    UTF_32_BE: BIG_QUERY_UTF_32BE,
    UTF_32_LE: BIG_QUERY_UTF_32LE,
}

BIG_QUERY_FIXED_LENGTH_ENCODINGS = {
    BIG_QUERY_ISO85591_1,
    BIG_QUERY_UTF_16BE,
    BIG_QUERY_UTF_16LE,
    BIG_QUERY_UTF_32BE,
    BIG_QUERY_UTF_32LE,
}

BIG_QUERY_VARIABLE_LENGTH_ENCODINGS = {BIG_QUERY_UTF_8}

# TODO(#28239) deprecate these in favor of always comparing using to_python_standard
UTF_8_ENCODING = UTF_8.upper()
ISO_8859_1_ENCODING = "ISO-8859-1"

COMMON_RAW_FILE_ENCODINGS = [UTF_8_ENCODING, ISO_8859_1_ENCODING]
