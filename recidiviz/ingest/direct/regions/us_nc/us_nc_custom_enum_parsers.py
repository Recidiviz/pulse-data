# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_NC. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_nc_custom_enum_parsers.<function name>
"""
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
    StateSpecializedPurposeForIncarceration,
)


def get_incarceration_type_from_facility(raw_text: str) -> StateIncarcerationType:
    """
    Parses the facility name to impute the type of incarceration.
    """
    if raw_text is None:
        return StateIncarcerationType.INTERNAL_UNKNOWN

    if raw_text in (
        "GEORGIA",
        "TENNESSEE",
        "MASSACHUSETTS",
        "CALIFORNIA",
        "ILLINOIS",
        "FLORIDA",
        "PENNSYLVANIA",
        "VIRGINIA",
        "KANSAS",
        "TEXAS",
        "MARYLAND",
        "ALABAMA",
        "MAINE",
        "ARKANSAS",
        "OKLAHOMA",
        "NEVADA",
        "MICHIGAN",
        "IDAHO",
        "MISSOURI",
        "MINNESOTA",
        "INDIANA",
        "OHIO",
        "WISCONSIN",
        "KENTUCKY",
        "LOUISIANA",
        "WASHINGTON",
        "DELAWARE",
        "ARIZONA",
        "MONTANA",
        "CONNECTICUT",
        "MISSISSIPPI",
        "COLORADO",
        "OREGON",
        "VERMONT",
        "NEBRASKA",
        "IOWA",
        "UTAH",
        "WYOMING",
        "ALASKA",
        "HAWAII",
        "MICRONESIA",
        "BAHAMAS",
        "CANADA",
        "GERMANY",
        "MEXICO",
        "NORTH DAKOTA",
        "UNITED STATES",
        "OUT-OF-STATE PAROLE",
        "NEW MEXICO",
        "SOUTH DAKOTA",
        "RHODE ISLAND",
        "NEW HAMPSHIRE",
        "NEW JERSEY",
        "WEST VIRGINIA",
        "NEW YORK",
        "SOUTH CAROLINA",
        "DISTRICT OF COLUMBIA",
        "US PUERTO RICO",
        "UNITED KINGDOM",
    ):
        return StateIncarcerationType.OUT_OF_STATE

    if "COUNTY" in raw_text:
        return StateIncarcerationType.COUNTY_JAIL

    if raw_text in (
        "UNKNOWN AT CONVERSION",
        "??????????????????????????????",
        "LOCATION UNKNOWN",
    ):
        return StateIncarcerationType.EXTERNAL_UNKNOWN

    if raw_text in (
        "FOOTHILLS CI              FOOT",
        "GRANVILLE CI              GRNV",
        "PIEDMONT CI               PIED",
        "CRAVEN CI                 CRAV",
        "NCCI WOMEN                NCCW",
        "CENTRAL PRISON            CENT",
        "ANSON CI                  ANSN",
        "ALBEMARLE CI              ALBE",
        "BERTIE CI                 BERT",
        "SAMPSON CI                SAMP",
        "TABOR CI                  TABR",
        "RANDOLPH CC               RAND",
        "PAMLICO CI                PCI",
        "DAVIDSON CC               DAVD",
        "JOHNSTON CI               JOHN",
        "COLUMBUS CI               COLU",
        "NEUSE CI                  NEUS",
        "GREENE CI                 GREE",
        "WILKES CC                 WILK",
        "WESTERN CCW               WEST",
        "HYDE CI                   HYDE",
        "LUMBERTON CI              LUMB",
        "RICHMOND CI               RICH",
        "EASTERN CI                EAST",
        "CALDWELL CC               CALD",
        "NASH CI                   NSHI",
        "FRANKLIN CC               FRAN",
        "MAURY C.I.                MAUR",
        "CRAGGY CC                 CRAG",
        "SCOTLAND CI               SCOT",
        "PASQUOTANK CI             PASQ",
        "LINCOLN CC                LINC",
        "CASWELL CC                CASW",
        "HARNETT CI                HARN",
        "WARREN CI                 WARR",
        "FORSYTH CC                FORS",
        "NEW HANOVER CC            NHAN",
        "PENDER CI                 PEND",
        "SANFORD CC                SANF",
        "RUTHERFORD CC             RUTH",
        "CARTERET CC               CART",
        "SOUTHERN CI               SCI",
        "GASTON CC                 GAST",
        "ALEXANDER C.I.            ALEX",
        "MARION CI                 MARI",
        "ORANGE CC                 ORAN",
        "CATAWBA CC                CATA",
    ):
        return StateIncarcerationType.STATE_PRISON

    return StateIncarcerationType.INTERNAL_UNKNOWN


def get_custody_level(raw_text: str) -> StateIncarcerationPeriodCustodyLevel:
    """Parses the custody level string from the raw data."""
    if "MEDIUM" in raw_text:
        return StateIncarcerationPeriodCustodyLevel.MEDIUM
    if "MINIMUM" in raw_text:
        return StateIncarcerationPeriodCustodyLevel.MINIMUM
    if "CLOSE" in raw_text:
        return StateIncarcerationPeriodCustodyLevel.CLOSE
    if "UNASSIGNEDUNA" in raw_text:
        return StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN
    if "MAXIMUM" in raw_text:
        return StateIncarcerationPeriodCustodyLevel.MAXIMUM
    return StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN


def get_sp_purpose(
    start_reason: str, end_reason: str
) -> StateSpecializedPurposeForIncarceration:
    """When "PSD" (Pre-Sentence Diagnostic) is included in a start or end reason for
    a period of incarceration, the specialized purpose is mapped to TEMPORARY_CUSTODY.
    Otherwise mapped to INTERNAL_UNKNOWN.
    """
    if start_reason is None or end_reason is None:
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    if "PSD" in start_reason or "PSD" in end_reason:
        return StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
    return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
