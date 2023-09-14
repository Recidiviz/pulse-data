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
"""Custom enum parsers functions for US_OR. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_or_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus


def parse_housing_unit_type(  # TODO(#21278): Update when schema change request for additional types of solitary is in.
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitType]:
    """
    Maps |housing_unit|, to its corresponding StateIncarcerationPeriodHousingUnitType.
    # make task and link to edit these when more housing unit types

    DSU stays are meant to be for a specific amount of time (SEG is also a DSU unit)
    ASU and AHU are also specified amount of times.
    Will remap SEG to other solitary confinement when additional enums available.

    IMU and BHU are more program completion based, and can result in long stays.
    MHI and ICH are mental health special housing units and length of stay is not specified.
    SHU is utilized at CCCF for DSU sanctioned, BHU programming and IMU programming female AICs.
    """

    if raw_text in (
        "%DS%",
        "%SEG%",
    ):
        return StateIncarcerationPeriodHousingUnitType.DISCIPLINARY_SOLITARY_CONFINEMENT

    if raw_text == "ASU":
        return (
            StateIncarcerationPeriodHousingUnitType.ADMINISTRATIVE_SOLITARY_CONFINEMENT
        )

    if raw_text == "AHU":
        return StateIncarcerationPeriodHousingUnitType.PROTECTIVE_CUSTODY

    if raw_text in ("%IMU%", "%SHU%", "%SMU%"):
        return StateIncarcerationPeriodHousingUnitType.OTHER_SOLITARY_CONFINEMENT

    if raw_text in (
        "%BHU%",
        "%ICH%",
    ):
        return (
            StateIncarcerationPeriodHousingUnitType.MENTAL_HEALTH_SOLITARY_CONFINEMENT
        )

    if raw_text in ("MHI", "%INFIRMARY%"):
        return StateIncarcerationPeriodHousingUnitType.HOSPITAL

    if not raw_text:
        return None
    return StateIncarcerationPeriodHousingUnitType.GENERAL


def parse_housing_unit_category(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitCategory]:
    """
    Maps |housing_unit|, to its corresponding StateIncarcerationPeriodHousingUnitCategory,
    identifying which housing units are solitary confinement units and which are general.
    """

    if raw_text in (
        "%DS%",
        "%ASU%",
        "%AHU%",
        "%SEG%",
        "%SEG%",
        "%IMU%",
        "%BHU%",
        "%MHI%",
        "%ICH%",
        "%SHU%",
        "%SMU%",
    ):
        return StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT

    if not raw_text:
        return None

    return StateIncarcerationPeriodHousingUnitCategory.GENERAL


def parse_sentence_status(raw_text: str) -> Optional[StateSentenceStatus]:
    """
    OR does not have a sentence status but they have TERMINATION_DATE AND TERMINATION CODE
    """
    term_code, term_date = raw_text.split("@@")

    if term_code == "NONE" and term_date == "NONE":
        return StateSentenceStatus.SERVING

    if term_code in ("VACA", "VARE", "ACQT", "DISM"):
        # VACA-Sentences Vacated, VARE-Sentences Vacated and Remanded to Court,
        # ACQT-ACQUITTAL: Convictions acquitted after re-trial/re-sentencing, DISM-Dismissed
        return StateSentenceStatus.VACATED

    if (
        term_code in ("COMP", "EXEC", "DIED", "DISC", "DSCH", "EXPI", "POST", "TERM")
        or term_code == "NONE"
        and term_date != "NONE"
    ):
        # Completed, executed, died, discharged, discharged (sp), expired, institution sentence expired,
        # OR sentence expired - International Transfer
        return StateSentenceStatus.COMPLETED

    if term_code == "AUTO":
        return StateSentenceStatus.EXTERNAL_UNKNOWN

    if term_code == "SUSP":
        return StateSentenceStatus.SUSPENDED

    if term_code in ("APPE", "SCOM", "XERR"):
        # Appeal Won, Sentence Commuted, Admitted to prison in error
        return StateSentenceStatus.COMMUTED

    if term_code in ("RSNT", "CASR", "HEAR", "PROB"):
        # Resentenced, Case Revoked to Court Responsibility, Resentenced under 137.712,
        # Institution sentence amended to supervision
        return StateSentenceStatus.AMENDED

    return StateSentenceStatus.INTERNAL_UNKNOWN
