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
"""Helper SQL fragments that import raw tables for NC
"""

FACILITY_PROGRAM_ID_VALUES = [
    # Substance abuse
    "COMPLETE RECOMMENDED SUBSTANCE ABUSE TREATMENT.",
    "PARTICIPATE AND COMPLETE BLACK MOUNTAIN.",  # residential treatment program
    "PARTICIPATE IN TROSA PROGRAM.",  # Triangle Residential Options for Substance Abusers
    "PARTICIPATE IN DART PROGRAM.",  # Drug Abuse Residential Treatment
    # Mental health
    "COMPLETE RECOMMENDED MENTAL HEALTH TREATMENT.",
    "PARTICIPATE IN PSYCHOLOGICAL COUNSELING.",
    "PARTICIPATE IN PSYCHIATRIC COUNSELING.",
    "PARTICIPATE IN AND COMPLETE MENTAL HEALTH TREATMENT COURT.",
    "PARTICIPATE IN MEDICAL/PSYCHOLOGICAL TREATMENT.",
    # Driving While Intoxicated
    "PARTICIPATE AND COMPLETE THREE SESSIONS PER WEEK DWI.",
]
