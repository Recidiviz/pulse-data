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
    law_enforcement,
    location,
    person,
)

# All official Justice Counts dimensions should be "checked in" here
DIMENSIONS = [
    person.Age,
    person.Race,
    person.RaceAndEthnicity,
    person.Gender,
    person.GenderRestricted,
    person.Ethnicity,
    person.StaffType,
    location.Agency,
    location.Facility,
    location.CountyFIPS,
    location.Country,
    location.County,
    location.State,
    location.Country,
    law_enforcement.SheriffBudgetType,
    law_enforcement.CallType,
    law_enforcement.OffenseType,
    law_enforcement.ForceType,
    corrections.PopulationType,
    corrections.ReleaseType,
    corrections.ReleaseType,
    corrections.AdmissionType,
    corrections.SupervisionType,
    corrections.SupervisionViolationType,
]


DIMENSION_IDENTIFIER_TO_DIMENSION = {
    dimension.dimension_identifier(): dimension for dimension in DIMENSIONS
}
