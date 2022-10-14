# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Metricfile objects used for prison metrics."""

from recidiviz.justice_counts.dimensions.jails_and_prisons import (
    CorrectionalFacilityForceType,
    PrisonsOffenseType,
    PrisonsReadmissionType,
    PrisonsReleaseType,
    PrisonsStaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import prisons

PRISON_METRIC_FILES = [
    MetricFile(
        canonical_filename="annual_budget",
        definition=prisons.annual_budget,
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=prisons.total_staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=prisons.total_staff,
        disaggregation=PrisonsStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="readmissions",
        definition=prisons.readmissions,
    ),
    MetricFile(
        canonical_filename="readmissions_by_type",
        definition=prisons.readmissions,
        disaggregation=PrisonsReadmissionType,
        disaggregation_column_name="readmission_type",
    ),
    MetricFile(
        canonical_filename="admissions",
        definition=prisons.admissions,
    ),
    MetricFile(
        canonical_filename="admissions_by_type",
        definition=prisons.admissions,
        disaggregation=PrisonsOffenseType,
        disaggregation_column_name="admission_type",
    ),
    MetricFile(
        canonical_filename="population",
        definition=prisons.average_daily_population,
    ),
    MetricFile(
        canonical_filename="population_by_type",
        definition=prisons.average_daily_population,
        disaggregation=PrisonsOffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="population_by_race",
        definition=prisons.average_daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="population_by_gender",
        definition=prisons.average_daily_population,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        canonical_filename="releases",
        definition=prisons.releases,
    ),
    MetricFile(
        canonical_filename="releases_by_type",
        definition=prisons.releases,
        disaggregation=PrisonsReleaseType,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        canonical_filename="use_of_force",
        definition=prisons.staff_use_of_force_incidents,
    ),
    MetricFile(
        canonical_filename="use_of_force_by_type",
        definition=prisons.staff_use_of_force_incidents,
        disaggregation=CorrectionalFacilityForceType,
        disaggregation_column_name="force_type",
    ),
    MetricFile(
        canonical_filename="grievances_upheld",
        definition=prisons.grievances_upheld,
    ),
]
