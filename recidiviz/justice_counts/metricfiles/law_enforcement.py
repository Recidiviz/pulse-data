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
"""Metricfile objects used for law enforcement metrics."""

from recidiviz.justice_counts.dimensions.common import ExpenseType
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ComplaintType,
    ForceType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import law_enforcement

LAW_ENFORCEMENT_METRIC_FILES = [
    MetricFile(
        canonical_filename="expenses",
        definition=law_enforcement.expenses,
    ),
    MetricFile(
        canonical_filename="expenses_by_type",
        definition=law_enforcement.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expense_type",
    ),
    MetricFile(
        canonical_filename="funding",
        definition=law_enforcement.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=law_enforcement.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="staff",
        definition=law_enforcement.staff,
    ),
    MetricFile(
        canonical_filename="staff_by_type",
        definition=law_enforcement.staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="staff_by_race",
        definition=law_enforcement.staff,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="staff_by_biological_sex",
        definition=law_enforcement.staff,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="calls_for_service",
        definition=law_enforcement.calls_for_service,
    ),
    MetricFile(
        canonical_filename="calls_for_service_by_type",
        definition=law_enforcement.calls_for_service,
        disaggregation=CallType,
        disaggregation_column_name="call_type",
    ),
    MetricFile(
        canonical_filename="reported_crime",
        definition=law_enforcement.reported_crime,
    ),
    MetricFile(
        canonical_filename="reported_crime_by_type",
        definition=law_enforcement.reported_crime,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="arrests",
        definition=law_enforcement.arrests,
    ),
    MetricFile(
        canonical_filename="arrests_by_type",
        definition=law_enforcement.arrests,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="arrests_by_race",
        definition=law_enforcement.arrests,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="arrests_by_biological_sex",
        definition=law_enforcement.arrests,
        disaggregation=BiologicalSex,
        disaggregation_column_name="biological_sex",
    ),
    MetricFile(
        canonical_filename="use_of_force",
        definition=law_enforcement.use_of_force_incidents,
    ),
    MetricFile(
        canonical_filename="use_of_force_by_type",
        definition=law_enforcement.use_of_force_incidents,
        disaggregation=ForceType,
        disaggregation_column_name="force_type",
    ),
    MetricFile(
        canonical_filename="civilian_complaints",
        definition=law_enforcement.civilian_complaints_sustained,
    ),
    MetricFile(
        canonical_filename="civilian_complaints_by_type",
        definition=law_enforcement.civilian_complaints_sustained,
        disaggregation=ComplaintType,
        disaggregation_column_name="complaint_type",
    ),
]
