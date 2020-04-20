# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Constants that correspond to column/field names from the ID DOC data warehouse."""

# Facility constants
INTERSTATE_FACILITY_CODE = 'IS'
FUGITIVE_FACILITY_CODE = 'FI'
JAIL_FACILITY_CODES = (
    'COUNTY JAIL',
    'JAIL BACKLOG'
)

PREVIOUS_FACILITY_TYPE = 'prev_fac_typ'
PREVIOUS_FACILITY_CODE = 'prev_fac_cd'
PREVIOUS_INVESTIGATION = 'prev_investigative'
PREVIOUS_PAROLE_VIOLATOR = 'prev_parole_violator'

CURRENT_FACILITY_TYPE = 'fac_typ'
CURRENT_FACILITY_CODE = 'fac_cd'
CURRENT_INVESTIGATION = 'investigative'
CURRENT_RIDER = 'rider'
CURRENT_PAROLE_VIOLATOR = 'parole_violator'

NEXT_FACILITY_TYPE = 'next_fac_typ'
NEXT_FACILITY_CODE = 'next_fac_cd'

# Violation report constants

# Constants from Violation Report 210
NON_VIOLENT_MISDEMEANOR = 'Non-Violent Misdemeanor'
VIOLENT_MISDEMEANOR = 'Violent Misdemeanor'
NON_VIOLENT_FELONY = 'Non-Violent Felony'
VIOLENT_FELONY = 'Violent Felony'
NON_VIOLENT_SEX_OFFENSE = 'Non-Violent Sex Offense'
VIOLENT_SEX_OFFENSE = 'Violent Sex Offense'

NO_REC_DEFER_TO_COURT_210 = 'No Recommendation - Defer to Court'
NO_REC_DEFER_TO_PAROLE_210 = 'No Recommendation - Defer to Parole Commission'

# Constants from Violation Report 204 (old version)
VIOLENT_CRIME = 'Violent Crime'
NON_VIOLENT_CRIME = 'NON-Violent Crime'
SEX_OFFENSE = 'Sex Offense'
NO_REC_DEFER_TO_COURT_204 = 'NO REC - Defer to Court'
NO_REC_DEFER_TO_PAROLE_204 = 'NO REC - No recommendation, defer to PC decision'

VIOLATION_REPORT_NO_RECOMMENDATION_VALUES = (
    NO_REC_DEFER_TO_COURT_210,
    NO_REC_DEFER_TO_PAROLE_210,
    NO_REC_DEFER_TO_COURT_204,
    NO_REC_DEFER_TO_PAROLE_204,
)

ALL_NEW_CRIME_TYPES = (
    NON_VIOLENT_MISDEMEANOR,
    VIOLENT_MISDEMEANOR,
    NON_VIOLENT_FELONY,
    VIOLENT_FELONY,
    NON_VIOLENT_SEX_OFFENSE,
    VIOLENT_SEX_OFFENSE,
    VIOLENT_CRIME,
    NON_VIOLENT_CRIME,
    SEX_OFFENSE,
)

VIOLENT_CRIME_TYPES = (
    VIOLENT_CRIME,
    VIOLENT_MISDEMEANOR,
    VIOLENT_FELONY,
    VIOLENT_SEX_OFFENSE,
)

SEX_CRIME_TYPES = (
    SEX_OFFENSE,
    VIOLENT_SEX_OFFENSE,
    NON_VIOLENT_SEX_OFFENSE,
)

# Recidiviz generated date used by Supervision/Incarceration periods query
MAX_DATE_STR = '9999-12-31'
