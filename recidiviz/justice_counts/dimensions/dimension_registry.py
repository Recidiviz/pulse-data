# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Registry containing all official Justice Counts dimensions."""

from recidiviz.justice_counts.dimensions import (
    corrections,
    courts,
    jails_and_prisons,
    law_enforcement,
    location,
    person,
    prosecution,
    supervision,
)

# All official Justice Counts dimensions should be "checked in" here
DIMENSIONS = [
    person.Age,
    person.Race,
    person.RaceAndEthnicity,
    person.Gender,
    person.GenderRestricted,
    person.Ethnicity,
    location.Agency,
    location.Facility,
    location.CountyFIPS,
    location.Country,
    location.County,
    location.State,
    location.Country,
    law_enforcement.CallType,
    law_enforcement.OffenseType,
    law_enforcement.ForceType,
    law_enforcement.LawEnforcementStaffType,
    corrections.PopulationType,
    corrections.ReleaseType,
    corrections.ReleaseType,
    corrections.AdmissionType,
    corrections.SupervisionType,
    corrections.SupervisionViolationType,
    jails_and_prisons.CorrectionalFacilityForceType,
    jails_and_prisons.JailPopulationType,
    jails_and_prisons.ReadmissionType,
    jails_and_prisons.CorrectionalFacilityStaffType,
    jails_and_prisons.JailReleaseType,
    jails_and_prisons.PrisonPopulationType,
    jails_and_prisons.PrisonReleaseType,
    prosecution.CaseSeverityType,
    prosecution.DispositionType,
    prosecution.ProsecutionAndDefenseStaffType,
    courts.CourtsCaseSeverityType,
    courts.CourtCaseType,
    courts.CourtReleaseType,
    courts.CourtStaffType,
    courts.SentenceType,
    supervision.NewOffenseType,
    supervision.SupervisionCaseType,
    supervision.SupervisionIndividualType,
    supervision.SupervisionStaffType,
    supervision.SupervisionTerminationType,
    supervision.SupervisionViolationType,
]


DIMENSION_IDENTIFIER_TO_DIMENSION = {
    dimension.dimension_identifier(): dimension for dimension in DIMENSIONS
}
