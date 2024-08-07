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
"""Types for raw table validations."""
from enum import Enum


class RawDataTableImportBlockingValidationType(Enum):
    """Types of import-blocking validations to be run on raw data after it has been loaded to a temporary table
    and before appending to the raw data table"""

    # checks that a column contains at least one nonnull value
    NONNULL_VALUES = "NONNULL_VALUES"
    # checks that datetime columns can be parsed using one of their defined datetime_sql_parsers
    DATETIME_PARSERS = "DATETIME_PARSERS"
    # checks that all values in an enum column match one of the defined known_values
    KNOWN_VALUES = "KNOWN_VALUES"
    # checks that all values in a column with a defined type can be cast to that type,
    # excluding datetime columns which are handled by DATETIME_PARSERS, and string columns which columns are imported as by default
    EXPECTED_TYPE = "EXPECTED_TYPE"
