#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""State-specific helpers for transforming ETL data"""
import re

US_TN_SUPERVISION_TYPE_MAPPING = {
    "DIVERSION": "Diversion",
    "TN PROBATIONER": "Probation",
    "TN PAROLEE": "Parole",
    "ISC FROM OTHER JURISDICTION": "ISC",
    "DETERMINATE RLSE PROBATIONER": "Determinate Release Probation",
    "SPCL ALT INCARCERATION UNIT": "SAIU",
    "MISDEMEANOR PROBATIONER": "Misdemeanor Probation",
}


def state_specific_client_address_transformation(state_code: str, address: str) -> str:
    if state_code == "US_TN":
        # incoming strings may have cased state abbreviation wrong
        return re.sub(r"\bTn\b", lambda m: m[0].upper(), address)
    return address


def state_specific_supervision_type_transformation(
    state_code: str, supervision_type: str
) -> str:
    if state_code == "US_TN":
        return US_TN_SUPERVISION_TYPE_MAPPING.get(supervision_type, supervision_type)
    return supervision_type
