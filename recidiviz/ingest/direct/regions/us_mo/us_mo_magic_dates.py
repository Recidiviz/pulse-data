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
"""Magic date constants for US_MO"""

MAGIC_DATES = (
    "0",
    "19000000",
    "20000000",
    "66666666",  # Should affiliate with pretrial investigation
    "77777777",
    "88888888",  # Should denote Interstate Compact or Indeterminate sentence
    "99999999",  # Often affiliated with a life sentence
)
