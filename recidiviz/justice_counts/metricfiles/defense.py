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

from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    DispositionType,
    ProsecutionAndDefenseStaffType,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import defense

DEFENSE_METRIC_FILES = [
    MetricFile(
        canonical_filename="annual_budget",
        definition=defense.annual_budget,
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=defense.total_staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=defense.total_staff,
        disaggregation=ProsecutionAndDefenseStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="cases_appointed",
        definition=defense.cases_appointed_counsel,
    ),
    MetricFile(
        canonical_filename="cases_appointed_by_severity",
        definition=defense.cases_appointed_counsel,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="severity_type",
    ),
    MetricFile(
        canonical_filename="caseloads",
        definition=defense.caseloads,
    ),
    MetricFile(
        canonical_filename="caseloads_by_severity",
        definition=defense.caseloads,
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
        canonical_filename="cases_disposed_by_gender",
        definition=defense.cases_disposed,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        canonical_filename="complaints_sustained",
        definition=defense.complaints,
    ),
]
