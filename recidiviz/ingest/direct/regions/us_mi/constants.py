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
"""This module has reusable strings for various MI specific query fragments and constants referenced in multiple spots in our codebase."""
from datetime import date

# Offense numbers that MI's 2026 classification policy treats as violent. Each is
# matched against the last two digits of a three-digit misconduct offense code, so the
# accomplice, attempt, and conspiracy variants of each offense count too.
CLASSIFICATION_2026_VIOLENT_OFFENSE_NUMBERS: list[str] = [
    "02",  # FELONY
    "03",  # ASSAULT RESULTING IN SERIOUS PHYSICAL INJURY - PRISONER VICTIM
    "04",  # ASSAULT RESULTING IN SERIOUS PHYSICAL INJURY - STAFF VICTIM
    "05",  # ASSAULT RESULTING IN SERIOUS PHYSICAL INJURY - OTHER VICTIM
    "07",  # ASSAULT AND BATTERY - PRISONER VICTIM
    "08",  # ASSAULT AND BATTERY - STAFF VICTIM
    "09",  # ASSAULT AND BATTERY - OTHER VICTIM
    "10",  # HOMICIDE
    "11",  # ASSAULT AND BATTERY - PRISONER VICTIM
    "15",  # ASSAULT AND BATTERY - STAFF VICTIM
    "16",  # ASSAULT AND BATTERY - OTHER VICTIM
    "13",  # SEXUAL ASSAULT
    "51",  # SEXUAL ASSAULT - ABUSIVE SEXUAL CONTACT
    "52",  # SEXUAL ASSAULT - STAFF VICTIM
    "53",  # SEXUAL ASSAULT - OTHER VICTIM
    "22",  # INCITE TO RIOT OR STRIKE
]

# Date when Michigan migrated from OMNI to COMS for supervision data
COMS_MIGRATION_DATE = date(2023, 8, 14)

ACTIVE_SUPERVISION_STATUSES = """
      'Parole',
      'Delayed Sentence',
      'Interstate Compact Parole',
      'Interstate Compact Probation',
      'Probation'
"""
