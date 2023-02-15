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

from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.dimensions.prisons import (
    ExpenseType,
    FundingType,
    GrievancesUpheldType,
    ReadmissionType,
    ReleaseType,
    StaffType,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import prisons

PRISON_METRIC_FILES = [
    MetricFile(
        canonical_filename="funding",
        definition=prisons.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=prisons.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="expenses",
        definition=prisons.expenses,
    ),
    MetricFile(
        canonical_filename="expense_by_type",
        definition=prisons.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expense_type",
    ),
    MetricFile(
        canonical_filename="staff",
        definition=prisons.staff,
    ),
    MetricFile(
        canonical_filename="staff_by_type",
        definition=prisons.staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="readmissions",
        definition=prisons.readmissions,
    ),
    MetricFile(
        canonical_filename="readmissions_by_type",
        definition=prisons.readmissions,
        disaggregation=ReadmissionType,
        disaggregation_column_name="readmission_type",
    ),
    MetricFile(
        canonical_filename="admissions",
        definition=prisons.admissions,
    ),
    MetricFile(
        canonical_filename="admissions_by_type",
        definition=prisons.admissions,
        disaggregation=OffenseType,
        disaggregation_column_name="admission_type",
    ),
    MetricFile(
        canonical_filename="population",
        definition=prisons.daily_population,
    ),
    MetricFile(
        canonical_filename="population_by_type",
        definition=prisons.daily_population,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="population_by_race",
        definition=prisons.daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="population_by_biological_sex",
        definition=prisons.daily_population,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="releases",
        definition=prisons.releases,
    ),
    MetricFile(
        canonical_filename="releases_by_type",
        definition=prisons.releases,
        disaggregation=ReleaseType,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        canonical_filename="use_of_force",
        definition=prisons.staff_use_of_force_incidents,
    ),
    MetricFile(
        canonical_filename="grievances_upheld",
        definition=prisons.grievances_upheld,
    ),
    MetricFile(
        canonical_filename="grievances_upheld_by_type",
        definition=prisons.grievances_upheld,
        disaggregation=GrievancesUpheldType,
        disaggregation_column_name="grievances_type",
    ),
]
