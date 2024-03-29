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
"""Metricfile objects used for Defense metrics."""

from recidiviz.justice_counts.dimensions.common import CaseSeverityType, DispositionType
from recidiviz.justice_counts.dimensions.defense import (
    CaseAppointedSeverityType,
    ExpenseType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import defense

DEFENSE_METRIC_FILES = [
    MetricFile(
        canonical_filename="funding",
        definition=defense.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=defense.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="expenses",
        definition=defense.expenses,
    ),
    MetricFile(
        canonical_filename="expenses_by_type",
        definition=defense.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expense_type",
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=defense.staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=defense.staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="cases_appointed",
        definition=defense.cases_appointed_counsel,
    ),
    MetricFile(
        canonical_filename="cases_appointed_by_severity",
        definition=defense.cases_appointed_counsel,
        disaggregation=CaseAppointedSeverityType,
        disaggregation_column_name="severity_type",
    ),
    MetricFile(
        canonical_filename="open_cases",
        definition=defense.caseload_numerator,
    ),
    MetricFile(
        canonical_filename="open_cases_by_type",
        definition=defense.caseload_numerator,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="severity_type",
    ),
    MetricFile(
        canonical_filename="staff_with_caseload",
        definition=defense.caseload_denominator,
    ),
    MetricFile(
        canonical_filename="staff_with_caseload_by_type",
        definition=defense.caseload_denominator,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="severity_type",
    ),
    MetricFile(
        canonical_filename="cases_disposed",
        definition=defense.cases_disposed,
    ),
    MetricFile(
        canonical_filename="cases_disposed_by_type",
        definition=defense.cases_disposed,
        disaggregation=DispositionType,
        disaggregation_column_name="disposition_type",
    ),
    MetricFile(
        canonical_filename="cases_disposed_by_race",
        definition=defense.cases_disposed,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="cases_disposed_by_sex",
        definition=defense.cases_disposed,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="complaints_sustained",
        definition=defense.client_complaints_sustained,
    ),
]
