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
"""Custom enum parsers functions for US_IX. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ix_custom_enum_parsers.<function name>
"""

from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)

# Mappings list for contact location

ALTERNATIVE_EMPLOYMENT_LOCATION = ["ALTERNATE WORK SITE"]

JAIL_LOCATION = [
    "JAIL",
]

COURT_LOCATION = [
    "COURT",
    "DRUG COURT",
]

FIELD_LOCATION = [
    "FIELD",
    "COMMUNITY SERVICE SITE",
]

LAW_ENFORCEMENT_LOCATION = [
    "LAW ENFORCEMENT AGENCY",
]

OFFICE_LOCATION = [
    "OFFICE",
    "INTERSTATE OFFICE",
]

PAROLE_COMMISSION_LOCATION = [
    "PAROLE COMMISSION",
]

RESIDENCE_LOCATION = [
    "RESIDENCE",
    "OTHER RESIDENCE",
]

EMPLOYMENT_LOCATION = [
    "EMPLOYER",
]

TREATMENT_PROVIDER_LOCATION = [
    "TREATMENT PROVIDER",
]


def contact_location_from_contact_locations_list(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Determines which supervision contact location to map to based on a list of concatenated supervision contact locations"""

    locations = raw_text.split(",")

    if any(location in RESIDENCE_LOCATION for location in locations):
        return StateSupervisionContactLocation.RESIDENCE

    if any(location in TREATMENT_PROVIDER_LOCATION for location in locations):
        return StateSupervisionContactLocation.TREATMENT_PROVIDER

    if any(location in OFFICE_LOCATION for location in locations):
        return StateSupervisionContactLocation.SUPERVISION_OFFICE

    if any(location in FIELD_LOCATION for location in locations):
        return StateSupervisionContactLocation.FIELD

    if any(location in PAROLE_COMMISSION_LOCATION for location in locations):
        return StateSupervisionContactLocation.PAROLE_COMMISSION

    if any(location in EMPLOYMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT

    if any(location in COURT_LOCATION for location in locations):
        return StateSupervisionContactLocation.COURT

    if any(location in LAW_ENFORCEMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY

    if any(location in ALTERNATIVE_EMPLOYMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT

    if any(location in JAIL_LOCATION for location in locations):
        return StateSupervisionContactLocation.JAIL

    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


def contact_method_from_contact_methods_list(
    raw_text: str,
) -> StateSupervisionContactMethod:
    """Determines which supervision contact method to map to based on a list of concatenated supervision contact methods"""

    methods = raw_text.split(",")

    if "TELEPHONE" in methods:
        return StateSupervisionContactMethod.TELEPHONE

    if "WRITTEN_MESSAGE" in methods:
        return StateSupervisionContactMethod.WRITTEN_MESSAGE

    if "VIRTUAL" in methods:
        return StateSupervisionContactMethod.VIRTUAL

    if "IN_PERSON" in methods:
        return StateSupervisionContactMethod.IN_PERSON

    return StateSupervisionContactMethod.INTERNAL_UNKNOWN


def bw_supervision_type(raw_text: str) -> StateSupervisionPeriodSupervisionType:
    """Maps supervision type to BENCH_WARRANT for cases where we've already identified via the supervising officer as being bench warrant;
    used instead of a literal enum so that raw text can be preserved."""
    if raw_text:
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT
    raise ValueError("This parser should never be called on missing raw text.")


def lsu_supervision_level(raw_text: str) -> StateSupervisionLevel:
    """Maps supervision level to LIMITED for cases where we've already determined supervision site to be district 0 or LSU;
    used instead of a literal enum so that raw text can be preserved."""
    if raw_text:
        return StateSupervisionLevel.LIMITED
    raise ValueError("This parser should never be called on missing raw text.")
