# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Dimension subclasses shared between systems."""

import enum

from recidiviz.justice_counts.dimensions.base import DimensionBase


class CaseSeverityType(DimensionBase, enum.Enum):
    FELONY = "Felony Caseload"
    MISDEMEANOR = "Misdemeanor Caseload"
    MIXED = "Mixed Caseload"
    OTHER = "Other Caseload"
    UNKNOWN = "Unknown Caseload"

    @classmethod
    def dimension_identifier(cls) -> str:
        return "metric/severity/prosecution_defense/type"
