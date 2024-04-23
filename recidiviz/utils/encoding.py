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
"""Utils for file encodings."""
import codecs


def to_python_standard(encoding: str) -> str:
    """Small wrapper around codecs.lookup to just return the standard name, such as
    latin-1 -> iso8859-1 and windows-1252 -> cp1252. If the encoding is not recognized,
    it will throw a Lookup Error. To see a full list of encodings and their aliases
    managed by the python codec's library, see :
    https://docs.python.org/3.11/library/codecs.html#standard-encodings
    """
    return codecs.lookup(encoding).name
