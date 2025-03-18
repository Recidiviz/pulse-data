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
    common,
    corrections,
    courts,
    defense,
    jails,
    law_enforcement,
    location,
    offense,
    person,
    prisons,
    prosecution,
    superagency,
    supervision,
)

# All official Justice Counts dimensions should be "checked in" here
DIMENSIONS = [
    common.ExpenseType,
    common.CaseSeverityType,
    common.DispositionType,
    offense.OffenseType,
    person.BiologicalSex,
    person.CensusRace,
    person.Race,
    person.RaceAndEthnicity,
    person.Gender,
    person.GenderRestricted,
    defense.StaffType,
    defense.CaseAppointedSeverityType,
    location.Agency,
    location.Facility,
    location.CountyFIPS,
    location.Country,
    location.County,
    location.State,
    location.Country,
    law_enforcement.CallType,
    law_enforcement.ForceType,
    law_enforcement.ComplaintType,
    law_enforcement.StaffType,
    law_enforcement.FundingType,
    corrections.PopulationType,
    corrections.ReleaseType,
    corrections.ReleaseType,
    corrections.AdmissionType,
    corrections.SupervisionType,
    corrections.SupervisionViolationType,
    prisons.ReadmissionType,
    prisons.StaffType,
    prisons.ReleaseType,
    prisons.GrievancesUpheldType,
    prisons.ExpenseType,
    prisons.FundingType,
    jails.PopulationType,
    jails.StaffType,
    jails.PreAdjudicationReleaseType,
    jails.PostAdjudicationReleaseType,
    jails.GrievancesUpheldType,
    jails.FundingType,
    jails.ExpenseType,
    jails.BehavioralHealthNeedType,
    prosecution.DivertedCaseSeverityType,
    prosecution.ReferredCaseSeverityType,
    prosecution.CaseDeclinedSeverityType,
    prosecution.StaffType,
    prosecution.FundingType,
    prosecution.ProsecutedCaseSeverityType,
    defense.FundingType,
    defense.ExpenseType,
    courts.CaseSeverityType,
    courts.ReleaseType,
    courts.StaffType,
    courts.SentenceType,
    courts.FundingType,
    supervision.DailyPopulationType,
    supervision.StaffType,
    supervision.DischargeType,
    supervision.ViolationType,
    supervision.FundingType,
    supervision.RevocationType,
    superagency.FundingType,
    superagency.ExpenseType,
    superagency.StaffType,
]


DIMENSION_IDENTIFIER_TO_DIMENSION = {
    dimension.dimension_identifier(): dimension for dimension in DIMENSIONS
}
