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
"""Custom enum parsers functions for US_UT. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ut_custom_enum_parsers.<function name>
"""

from recidiviz.common.constants.state.state_charge import (
    StateChargeV2ClassificationType,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentencingAuthority
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.ingest.direct.regions.us_ut.ingest_views.common_code_constants import (
    SUPERVISION_LEGAL_STATUS_CODES_SET,
)

_INCARCERATED_SUPERVISION_LOCATIONS = [
    "ATF CUSTODY",
    "CUCF ASPEN",
    "CUCF BIRCH",
    "CUCF CEDAR",
    "CUCF DOGWOOD",
    "CUCF ELM",
    "CUCF FIR",
    "CUCF GALE",
    "FEDERAL CUSTODY",
    "ICE CUSTODY",
    "OTHER COUNTRY CUSTODY",
    "USCF B1 1-3 FRONT",
    "USCF B4 1-3 FRONT",
    "USCF B4 4-5 MIDDLE",
    "USCF D LOW SIDE",
    "USCF F HIGH SIDE",
    "USCF F LOW SIDE",
    "USCF G LOW SIDE",
    "USP DRAPER",
    "USP USCF",
]


def parse_employment_status(
    raw_text: str,
) -> StateEmploymentPeriodEmploymentStatus:
    """Determines the status of an employment period. If a person's job title does not
    signify that they are unemployed, we assume their appearance in the emplymt raw data
    table means they are employed in some capacity."""
    hours_per_week, job_title, comment, employer_name = raw_text.split("@@")
    if (
        "STUDENT" in job_title.upper()
        or "STUDENT" in comment.upper()
        or "STUDENT" in employer_name.upper()
    ):
        return StateEmploymentPeriodEmploymentStatus.STUDENT
    if (
        "SOCIAL SECURITY" in employer_name.upper()
        or "PENSION" in employer_name.upper()
        or "WORKERS' COMPENSATION" in employer_name.upper()
        or "VETERANS' ADMIN BENEFITS" in employer_name.upper()
        or "PRIMARY CARE GIVER/HOMEMAKER" in employer_name.upper()
    ):
        return StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE
    if "UNABLE TO WORK" in employer_name.upper():
        return StateEmploymentPeriodEmploymentStatus.UNABLE_TO_WORK
    if (
        "UNEMPL" in job_title.upper()
        or job_title.upper() == "NONE"
        or "UNEMPL" in comment.upper()
    ):
        return StateEmploymentPeriodEmploymentStatus.UNEMPLOYED
    if hours_per_week != "NONE":
        if int(float(hours_per_week)) >= 40:
            return StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME
    return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT


def parse_charge_classification_type(raw_text: str) -> StateChargeV2ClassificationType:
    if raw_text:
        if "MISDEMEANOR" in raw_text:
            return StateChargeV2ClassificationType.MISDEMEANOR
        if "FELONY" in raw_text:
            return StateChargeV2ClassificationType.FELONY
        return StateChargeV2ClassificationType.INTERNAL_UNKNOWN
    return StateChargeV2ClassificationType.INTERNAL_UNKNOWN


def parse_sentencing_authority(raw_text: str) -> StateSentencingAuthority:
    if raw_text:
        # Justice Courts are established by counties and municipalities and have the
        # authority to deal with class B and C misdemeanors, violations of ordinances,
        # small claims, and infractions committed within their territorial jurisdiction.
        if "JUSTICE" in raw_text:
            return StateSentencingAuthority.COUNTY
        if "FEDERAL" in raw_text or raw_text == "US SUPREME COURT":
            return StateSentencingAuthority.FEDERAL
        if "COMPACT" in raw_text or "OTHER STATE" in raw_text:
            return StateSentencingAuthority.OTHER_STATE
        # The catch-all STATE sentencing authority here includes:
        # District Courts - the state trial court of general jurisdiction.
        # Juvenile Courts - of equal status with the District Court.
        # Circuit Courts - can be at a state or federal level, but are all associated
        # with a region in Utah in this data, so we assume they are state circuits.
        return StateSentencingAuthority.STATE
    return StateSentencingAuthority.INTERNAL_UNKNOWN


def parse_supervision_type(raw_text: str) -> StateSupervisionPeriodSupervisionType:
    if raw_text:
        lgl_stat_desc, start_reason, body_loc_desc = raw_text.split("|")
        if (
            start_reason in ("ABSCOND", "WALKAWAY", "ESCAPE", "AWOL", "FUGITIVE")
            or "FUGITIVE" in body_loc_desc
            or "-FUG-" in body_loc_desc
            or body_loc_desc in ("A.W.O.L.", "ABSCOND", "WALKAWAY")
        ):
            return StateSupervisionPeriodSupervisionType.ABSCONSION
        if "PROBATION" in lgl_stat_desc:
            return StateSupervisionPeriodSupervisionType.PROBATION
        if "PAROLE" in lgl_stat_desc:
            return StateSupervisionPeriodSupervisionType.PAROLE
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN


def parse_incarceration_type(raw_text: str) -> StateIncarcerationType:
    if raw_text:
        if "CO JAIL" in raw_text:
            return StateIncarcerationType.COUNTY_JAIL
        return StateIncarcerationType.STATE_PRISON
    return StateIncarcerationType.INTERNAL_UNKNOWN


def parse_custodial_authority(raw_text: str) -> StateCustodialAuthority:
    if raw_text:
        location_description, legal_status_code = raw_text.split("@@")
        if legal_status_code in SUPERVISION_LEGAL_STATUS_CODES_SET:
            return StateCustodialAuthority.SUPERVISION_AUTHORITY
        if "CO JAIL" in location_description:
            return StateCustodialAuthority.COUNTY
        # Custody Compact & other denotation of custody in another state.
        if "CUSCOM" in location_description or "ZINCARCERATED" in location_description:
            return StateCustodialAuthority.OTHER_STATE
        if location_description in ("FEDERAL CUSTODY", "US MARSHALS"):
            return StateCustodialAuthority.FEDERAL
        return StateCustodialAuthority.STATE_PRISON
    return StateCustodialAuthority.INTERNAL_UNKNOWN


def parse_specialized_pfi(raw_text: str) -> StateSpecializedPurposeForIncarceration:
    if raw_text:
        start_reason, end_reason, legal_status = raw_text.split("@@")
        if "72 HOUR HOLD" in start_reason or "SANCTION" in start_reason:
            return StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        # If a person was arrested while on probation and then immediately released,
        # we can assume that is a period of temporary custody.
        if (
            start_reason == "ARRESTED"
            and end_reason == "JAIL RELEASE"
            and (legal_status == "PLEA IN ABEYANCE" or "PROBATION" in legal_status)
        ):
            return StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
        return StateSpecializedPurposeForIncarceration.GENERAL
    return StateSpecializedPurposeForIncarceration.GENERAL


def parse_supervision_level(raw_text: str) -> StateSupervisionLevel:
    """Parse supervision level first by checking if a person is incarcerated, and then
    by using the raw supervision level value from raw data."""
    if raw_text:
        level = raw_text.split("-")[0]
        location = raw_text.split("-")[2]
        if (
            "JAIL" in location
            or "ZINCARCERATED" in location
            or "INCARCERATED" in level
            or location in _INCARCERATED_SUPERVISION_LOCATIONS
        ):
            return StateSupervisionLevel.IN_CUSTODY
        if level == "COMPACT OUT":
            return StateSupervisionLevel.INTERSTATE_COMPACT
        if "FUGITIVE" in level:
            return StateSupervisionLevel.WARRANT
        if "MAXIMUM" in level:
            return StateSupervisionLevel.MAXIMUM
        if "MEDIUM" in level or "MODERATE" in level:
            return StateSupervisionLevel.MEDIUM
        if level == "MIN/MED MISDEMEANOR":
            return StateSupervisionLevel.LOW_MEDIUM
        if "MINIMUM" in level or level == "LOW":
            return StateSupervisionLevel.MINIMUM
        if level in ("TELEPHONIC", "ADMINISTRATION"):
            return StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY
        if level == "COLLECTION ONLY":
            return StateSupervisionLevel.UNSUPERVISED
        if "INTENSIVE" in level or "ISP" in level or level == "HIGH":
            return StateSupervisionLevel.HIGH
        if "RESIDENTIAL" in level:
            return StateSupervisionLevel.RESIDENTIAL_PROGRAM
        if level == "UNKNOWN":
            return StateSupervisionLevel.EXTERNAL_UNKNOWN
        return StateSupervisionLevel.INTERNAL_UNKNOWN
    return StateSupervisionLevel.PRESENT_WITHOUT_INFO


def parse_supervision_violation_type_from_description(
    raw_text: str,
) -> StateSupervisionViolationType:
    """
    Determines the type of violations based on the description of the violation provided.
    Violations that stem from controlled substance use or possession are not labeled as
    new crimes in any way in the raw data, and there are not criminal charges documented
    in other raw data that relate to those violations; substance-related violations are
    therefore mapped to TECHNICAL.
    """
    if raw_text:
        violation_desc = raw_text.lower()
        if (
            "crime" in violation_desc
            or "arrest" in violation_desc
            or "convict" in violation_desc
            or "new charge" in violation_desc
            or "new criminal offense" in violation_desc
        ):
            if (
                "non crime" not in violation_desc
                # These extra conditions ensure that violations that involve *both* a new
                # crime and a technical violation are parsed as a new crime. It also ensures
                # that violations that only involve associating with people with a criminal
                # record are not parsed as new crimes.
                and not (
                    "by having associated" in violation_desc
                    and (
                        "new criminal offense" not in violation_desc
                        and "by having committed" not in violation_desc
                    )
                )
            ):
                return StateSupervisionViolationType.LAW
        if "abscond" in violation_desc:
            return StateSupervisionViolationType.ABSCONDED
        return StateSupervisionViolationType.TECHNICAL
    return StateSupervisionViolationType.INTERNAL_UNKNOWN
