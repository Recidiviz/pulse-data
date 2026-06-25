# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""The canonical set of organization types a `known_organizations` entry may be
classified as, and their descriptions.

This enum is the single source of truth for both the set of valid organization
types and the human-readable description of each — the latter used as curated
prompt context when organizations of a given type are rendered.
"""
from enum import Enum


class OrganizationType(Enum):
    """A category an organization, program, or facility can be classified as.

    Values are the strings used in the reference-data YAML files (e.g. a
    `known_organizations` entry's `organization_type`).
    """

    COMMUNITY_CORRECTIONS = "community_corrections"
    NONRESIDENTIAL_PROGRAM = "nonresidential_program"
    RESIDENTIAL_TREATMENT = "residential_treatment"
    SOBER_LIVING = "sober_living"
    HALFWAY_HOUSE = "halfway_house"
    SHELTER = "shelter"
    HOTEL_MOTEL = "hotel_motel"
    EMPLOYER = "employer"
    STAFFING_AGENCY = "staffing_agency"

    @property
    def description(self) -> str:
        """Returns a concise human-readable phrase for this type, used as curated
        prompt context when organizations of this type are rendered.
        """
        if self is OrganizationType.COMMUNITY_CORRECTIONS:
            return "Custody or community-corrections facilities people are confined to."
        if self is OrganizationType.NONRESIDENTIAL_PROGRAM:
            return "Programs people attend but do not live at."
        if self is OrganizationType.RESIDENTIAL_TREATMENT:
            return "Treatment facilities that may be residential or outpatient."
        if self is OrganizationType.SOBER_LIVING:
            return "Sober living homes."
        if self is OrganizationType.HALFWAY_HOUSE:
            return "Transitional / halfway housing."
        if self is OrganizationType.SHELTER:
            return "Emergency or homeless shelters."
        if self is OrganizationType.HOTEL_MOTEL:
            return "Hotels, motels, inns, and lodging houses."
        if self is OrganizationType.EMPLOYER:
            return "Employers."
        if self is OrganizationType.STAFFING_AGENCY:
            return "Temp / staffing agencies that place people in jobs."
        raise ValueError(f"Unexpected OrganizationType: [{self}]")
