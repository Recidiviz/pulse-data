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
"""Common constants for Utah used across multiple files"""

# legal status codes from the lgl_stat_cd table that indicate that a JII is on supervision
SUPERVISION_LEGAL_STATUS_CODES = [
    "A",  # CLASS A PROBATION
    "F",  # FELONY PROBATION
    "M",  # CLASS B/C PROBATION
    "N",  # PLEA IN ABEYANCE
    "P",  # PAROLE
    "W",  # COMPACT IN PROBATION
    "X",  # COMPACT IN PAROLE
    "O",  # PRE-CONVICT DIVERSN
]

SUPERVISION_LEGAL_STATUS_CODES_SET = set(SUPERVISION_LEGAL_STATUS_CODES)

# legal status codes from the lgl_stat_cd table that indicate that a JII is incarcerated
INCARCERATION_LEGAL_STATUS_CODES = [
    "I",  # INMATE
    "Y",  # COMPACT IN INMATE
    "V",  # PAROLE VIOLATION
]

# location type codes from the loc_typ_cd table that indicate that a JII is in a prison/jail
INCARCERATION_LOCATION_TYPE_CODES = [
    "U",  # UTAH JAIL
    "I",  # INSTITUTIONAL OPS
]
