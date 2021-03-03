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

"""A static reference cache for converting raw incoming codes into normalized
county codes."""

import logging
from typing import Dict, Optional


COUNTY_CODES: Dict[str, str] = {
    "1": "US_ND_ADAMS",
    "AD": "US_ND_ADAMS",
    "2": "US_ND_BARNES",
    "BA": "US_ND_BARNES",
    "3": "US_ND_BENSON",
    "BE": "US_ND_BENSON",
    "4": "US_ND_BILLINGS",
    "BI": "US_ND_BILLINGS",
    "5": "US_ND_BOTTINEAU",
    "BT": "US_ND_BOTTINEAU",
    "6": "US_ND_BOWMAN",
    "BW": "US_ND_BOWMAN",
    "7": "US_ND_BURKE",
    "BU": "US_ND_BURKE",
    "8": "US_ND_BURLEIGH",
    "BR": "US_ND_BURLEIGH",
    "9": "US_ND_CASS",
    "CA": "US_ND_CASS",
    "10": "US_ND_CAVALIER",
    "CV": "US_ND_CAVALIER",
    "11": "US_ND_DICKEY",
    "DI": "US_ND_DICKEY",
    "12": "US_ND_DIVIDE",
    "DV": "US_ND_DIVIDE",
    "13": "US_ND_DUNN",
    "DU": "US_ND_DUNN",
    "14": "US_ND_EDDY",
    "ED": "US_ND_EDDY",
    "15": "US_ND_EMMONS",
    "EM": "US_ND_EMMONS",
    "16": "US_ND_FOSTER",
    "FO": "US_ND_FOSTER",
    "17": "US_ND_GOLDEN_VALLEY",
    "GV": "US_ND_GOLDEN_VALLEY",
    "18": "US_ND_GRAND_FORKS",
    "GF": "US_ND_GRAND_FORKS",
    "19": "US_ND_GRANT",
    "GR": "US_ND_GRANT",
    "20": "US_ND_GRIGGS",
    "GG": "US_ND_GRIGGS",
    "21": "US_ND_HETTINGER",
    "HE": "US_ND_HETTINGER",
    "22": "US_ND_KIDDER",
    "KI": "US_ND_KIDDER",
    "23": "US_ND_LAMOURE",
    "LM": "US_ND_LAMOURE",
    "24": "US_ND_LOGAN",
    "LO": "US_ND_LOGAN",
    "25": "US_ND_MCHENRY",
    "MH": "US_ND_MCHENRY",
    "26": "US_ND_MCINTOSH",
    "MI": "US_ND_MCINTOSH",
    "27": "US_ND_MCKENZIE",
    "MK": "US_ND_MCKENZIE",
    "28": "US_ND_MCLEAN",
    "ML": "US_ND_MCLEAN",
    "29": "US_ND_MERCER",
    "ME": "US_ND_MERCER",
    "30": "US_ND_MORTON",
    "MO": "US_ND_MORTON",
    "31": "US_ND_MOUNTRAIL",
    "MT": "US_ND_MOUNTRAIL",
    "32": "US_ND_NELSON",
    "NE": "US_ND_NELSON",
    "33": "US_ND_OLIVER",
    "OL": "US_ND_OLIVER",
    "34": "US_ND_PEMBINA",
    "PE": "US_ND_PEMBINA",
    "35": "US_ND_PIERCE",
    "PI": "US_ND_PIERCE",
    "36": "US_ND_RAMSEY",
    "RA": "US_ND_RAMSEY",
    "37": "US_ND_RANSOM",
    "RN": "US_ND_RANSOM",
    "38": "US_ND_RENVILLE",
    "RE": "US_ND_RENVILLE",
    "39": "US_ND_RICHLAND",
    "RI": "US_ND_RICHLAND",
    "40": "US_ND_ROLETTE",
    "RO": "US_ND_ROLETTE",
    "41": "US_ND_SARGENT",
    "SA": "US_ND_SARGENT",
    "42": "US_ND_SHERIDAN",
    "SH": "US_ND_SHERIDAN",
    "43": "US_ND_SIOUX",
    "SI": "US_ND_SIOUX",
    "44": "US_ND_SLOPE",
    "SL": "US_ND_SLOPE",
    "45": "US_ND_STARK",
    "ST": "US_ND_STARK",
    "46": "US_ND_STEELE",
    "SE": "US_ND_STEELE",
    "47": "US_ND_STUTSMAN",
    "SU": "US_ND_STUTSMAN",
    "48": "US_ND_TOWNER",
    "TO": "US_ND_TOWNER",
    "49": "US_ND_TRAILL",
    "TR": "US_ND_TRAILL",
    "50": "US_ND_WALSH",
    "WA": "US_ND_WALSH",
    "51": "US_ND_WARD",
    "WR": "US_ND_WARD",
    "52": "US_ND_WELLS",
    "WE": "US_ND_WELLS",
    "53": "US_ND_WILLIAMS",
    "WI": "US_ND_WILLIAMS",
    "101": "US_AL",
    "102": "US_AK",
    "103": "US_AZ",
    "104": "US_AR",
    "105": "US_CA",
    "106": "US_CO",
    "107": "US_CT",
    "108": "US_DE",
    "109": "US_DC",
    "110": "US_FL",
    "111": "US_GA",
    "112": "US_HI",
    "113": "US_ID",
    "114": "US_IL",
    "115": "US_IN",
    "116": "US_IA",
    "117": "US_KS",
    "118": "US_KY",
    "119": "US_LA",
    "120": "US_ME",
    "MD": "US_MD",
    "121": "US_MD",
    "122": "US_MA",
    "123": "US_MI",
    "124": "US_MN",
    "MN": "US_MN",
    "MINN": "US_MN",
    "125": "US_MS",
    "126": "US_MO",
    "127": "US_MT",
    "128": "US_NE",
    "129": "US_NV",
    "130": "US_NH",
    "131": "US_NJ",
    "132": "US_NM",
    "133": "US_NY",
    "134": "US_NC",
    "135": "US_ND",
    "136": "US_OH",
    "137": "US_OK",
    "138": "US_OR",
    "139": "US_PA",
    "140": "US_PR",
    "141": "US_RH",
    "142": "US_SC",
    "143": "US_SD",
    "SD": "US_SD",
    "144": "US_TN",
    "145": "US_TX",
    "146": "US_UT",
    "147": "US_VT",
    "148": "US_VA",
    "149": "US_VI",
    "150": "US_WA",
    "151": "US_WV",
    "152": "US_WI",
    "153": "US_WY",
    "WY": "US_WY",
    "154": "INTERNATIONAL",
    "155": "ERROR",
    "156": "PAROLE",
    "OS": "OUT_OF_STATE",
    "FD": "FEDERAL",
}


def normalized_county_code(
    county_code: Optional[str], county_codes_map: Dict[str, str]
) -> Optional[str]:
    if not county_code:
        return None

    pre_normalized_code = _normalize_int_code(county_code.upper())
    normalized_code = county_codes_map.get(pre_normalized_code)
    if not normalized_code:
        logging.warning(
            "Found new county code not in reference cache: [%s]", county_code
        )
        return pre_normalized_code

    return normalized_code


def _normalize_int_code(county_code: str) -> str:
    """Normalize integer-based county codes as a first step.

    Some of the county codes we receive are integers. Of those, some have
    0-padding and some do not. To ensure consistency, we check for integer-based
    codes and convert those to ints and back to string to drop padding,
    if present.
    """
    try:
        as_int = int(county_code)
        return str(as_int)
    except ValueError:
        # Not an integer
        return county_code
