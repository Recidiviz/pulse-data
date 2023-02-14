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
"""Metricfile objects used for Jails metrics."""

from recidiviz.justice_counts.dimensions.jails import (
    ExpenseType,
    FundingType,
    ReleaseType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import jails

JAILS_METRIC_FILES = [
    MetricFile(
        canonical_filename="funding",
        definition=jails.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=jails.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="expenses",
        definition=jails.expenses,
    ),
    MetricFile(
        canonical_filename="expense_by_type",
        definition=jails.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expense_type",
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=jails.total_staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=jails.total_staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="readmissions",
        definition=jails.readmissions,
    ),
    MetricFile(
        canonical_filename="pre_adj_admissions",
        definition=jails.pre_adjudication_admissions,
    ),
    MetricFile(
        canonical_filename="pre_adj_admissions_by_type",
        definition=jails.pre_adjudication_admissions,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="post_adj_admissions",
        definition=jails.post_adjudication_admissions,
    ),
    MetricFile(
        canonical_filename="post_adj_admissions_by_type",
        definition=jails.post_adjudication_admissions,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="pre_adj_population",
        definition=jails.pre_adjudication_daily_population,
    ),
    MetricFile(
        canonical_filename="pre_adj_population_by_type",
        definition=jails.pre_adjudication_daily_population,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="pre_adj_population_by_race",
        definition=jails.pre_adjudication_daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="pre_adj_population_by_sex",
        definition=jails.pre_adjudication_daily_population,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="post_adj_population",
        definition=jails.post_adjudication_daily_population,
    ),
    MetricFile(
        canonical_filename="post_adj_population_by_type",
        definition=jails.post_adjudication_daily_population,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="post_adj_population_by_race",
        definition=jails.post_adjudication_daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="post_adj_population_by_sex",
        definition=jails.post_adjudication_daily_population,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="releases",
        definition=jails.releases,
    ),
    MetricFile(
        canonical_filename="releases_by_type",
        definition=jails.releases,
        disaggregation=ReleaseType,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        canonical_filename="use_of_force",
        definition=jails.staff_use_of_force_incidents,
    ),
    MetricFile(
        canonical_filename="grievances_upheld",
        definition=jails.grievances_upheld,
    ),
]
