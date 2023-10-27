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
"""Custom enum parsers functions for US_AR. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ar_custom_enum_parsers.<function name>
"""

from typing import Optional

from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)


def parse_ethnic_group(
    raw_text: str,
) -> Optional[StateEthnicity]:
    if raw_text in [
        "03",  # Cuban
        "09",  # Hispanic or Latino
        "10",  # South American
        "11",  # Central America
        "23",  # Mexican American
        "24",  # Mexican National
        "27",  # Puerto Rican
        "33",  # Spain (note: by most definitions, Hispanic but not Latino)
        "35",  # Peru
        "36",  # Panama
        "37",  # Boliva
        "40",  # Mariel-Cuban
    ]:
        return StateEthnicity.HISPANIC

    if raw_text in ["98", "99"]:
        return StateEthnicity.EXTERNAL_UNKNOWN

    return StateEthnicity.NOT_HISPANIC if raw_text else None


def parse_non_parole_period_start_reason(
    raw_text: str,
) -> StateSupervisionPeriodAdmissionReason:
    """Custom parser for concatenated non-parole start reason codes."""
    # TODO(#21687): Separate parsers are currently needed to correctly interpret G01
    # (Intake new case) for parole and non-parole periods. This could be handled more cleanly
    # by pulling event reason into the supervision period view, since that data distinguishes
    # between court sentences/incarceration releases for G01 events.
    codes = raw_text.split("-")

    if "G01" in codes:
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    if "S41" in codes:
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION
    if "G05" in codes:
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
    if "L05" in codes:
        return StateSupervisionPeriodAdmissionReason.ABSCONSION
    if any(code in ["S48", "S31"] for code in codes):
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION
    if "G03" in codes:
        return StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION
    if any(
        code
        in [
            "G02",
            "G07",
            "G08",
            "S02",
            "S04",
            "S05",
            "S11",
            "S12",
            "S27",
            "S28",
            "S35",
            "S36",
            "S37",
            "S38",
            "S40",
        ]
        for code in codes
    ):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN


def parse_parole_period_start_reason(
    raw_text: str,
) -> StateSupervisionPeriodAdmissionReason:
    """Custom parser for concatenated parole start reason codes."""
    codes = raw_text.split("-")

    if "G01" in codes:
        return StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION

    return parse_non_parole_period_start_reason(raw_text)


def parse_supervision_period_end_reason(
    raw_text: str,
) -> StateSupervisionPeriodTerminationReason:
    """Custom parser for concatenated end reason codes."""
    codes = raw_text.split("-")
    if any(code in ["L05", "S25"] for code in codes):
        if "G05" in codes:
            # Same-day absconsions and returns are treated as unknown.
            return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
        return StateSupervisionPeriodTerminationReason.ABSCONSION
    if "G05" in codes:
        return StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION
    if any(code in ["L21", "S30", "S47"] for code in codes):
        return StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION
    if "L10" in codes:
        return StateSupervisionPeriodTerminationReason.DEATH
    if any(code in ["L12", "L13"] for code in codes):
        return StateSupervisionPeriodTerminationReason.DISCHARGE
    if "L11" in codes:
        return StateSupervisionPeriodTerminationReason.EXPIRATION
    if "L09" in codes:
        return StateSupervisionPeriodTerminationReason.PARDONED
    if any(code in ["L06", "L07", "L08"] for code in codes):
        return StateSupervisionPeriodTerminationReason.REVOCATION
    if "L03" in codes:
        return StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION
    if any(
        code
        in [
            "G02",
            "G07",
            "G08",
            "S02",
            "S04",
            "S05",
            "S11",
            "S12",
            "S13",
            "S27",
            "S28",
            "S31",
            "S35",
            "S36",
            "S37",
            "S38",
            "S40",
            "S41",
        ]
        for code in codes
    ):
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    if "S90" in codes:
        return StateSupervisionPeriodTerminationReason.VACATED
    return StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
