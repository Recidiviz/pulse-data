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
"""Metricfile objects used for supervision metrics."""

from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.dimensions.supervision import (
    DailyPopulationType,
    DischargeType,
    ExpenseType,
    FundingType,
    NewCaseType,
    RevocationType,
    StaffType,
    ViolationType,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import supervision

SUPERVISION_METRIC_FILES = [
    MetricFile(
        canonical_filename="funding",
        definition=supervision.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=supervision.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="expenses",
        definition=supervision.expenses,
    ),
    MetricFile(
        canonical_filename="expenses_by_type",
        definition=supervision.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expenses_type",
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=supervision.total_staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=supervision.total_staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="violations",
        definition=supervision.violations,
    ),
    MetricFile(
        canonical_filename="violations_by_type",
        definition=supervision.violations,
        disaggregation=ViolationType,
        disaggregation_column_name="violation_type",
    ),
    MetricFile(
        canonical_filename="new_cases",
        definition=supervision.new_cases,
    ),
    MetricFile(
        canonical_filename="new_cases_by_type",
        definition=supervision.new_cases,
        disaggregation=NewCaseType,
        disaggregation_column_name="case_type",
    ),
    MetricFile(
        canonical_filename="population",
        definition=supervision.daily_population,
    ),
    MetricFile(
        canonical_filename="population_by_type",
        definition=supervision.daily_population,
        disaggregation=DailyPopulationType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        canonical_filename="population_by_biological_sex",
        definition=supervision.daily_population,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="population_by_race",
        definition=supervision.daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="discharges",
        definition=supervision.discharges,
    ),
    MetricFile(
        canonical_filename="discharges_by_type",
        definition=supervision.discharges,
        disaggregation=DischargeType,
        disaggregation_column_name="discharges_type",
    ),
    MetricFile(
        canonical_filename="reconvictions",
        definition=supervision.reconvictions,
    ),
    MetricFile(
        canonical_filename="open_cases",
        definition=supervision.caseload_numerator,
    ),
    MetricFile(
        canonical_filename="staff_with_caseload",
        definition=supervision.caseload_denominator,
    ),
    MetricFile(
        canonical_filename="revocations",
        definition=supervision.revocations,
    ),
    MetricFile(
        canonical_filename="revocations_by_type",
        definition=supervision.revocations,
        disaggregation=RevocationType,
        disaggregation_column_name="revocations_type",
    ),
]
