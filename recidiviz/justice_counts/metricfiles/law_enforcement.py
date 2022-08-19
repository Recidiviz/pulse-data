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

from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ForceType,
    OffenseType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import law_enforcement

LAW_ENFORCEMENT_METRIC_FILES = [
    MetricFile(
        canonical_filename="annual_budget",
        definition=law_enforcement.annual_budget,
    ),
    MetricFile(
        canonical_filename="police_officers",
        definition=law_enforcement.police_officers,
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
        definition=law_enforcement.total_arrests,
    ),
    MetricFile(
        canonical_filename="arrests_by_type",
        definition=law_enforcement.total_arrests,
        disaggregation=OffenseType,
        disaggregation_column_name="offense_type",
    ),
    MetricFile(
        canonical_filename="arrests_by_race",
        allowed_filenames=[
            "arrests_by_race/ethnicity",
        ],
        definition=law_enforcement.total_arrests,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="arrests_by_gender",
        definition=law_enforcement.total_arrests,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        canonical_filename="use_of_force",
        definition=law_enforcement.officer_use_of_force_incidents,
    ),
    MetricFile(
        canonical_filename="use_of_force_by_type",
        definition=law_enforcement.officer_use_of_force_incidents,
        disaggregation=ForceType,
        disaggregation_column_name="force_type",
    ),
    MetricFile(
        canonical_filename="civilian_complaints_sustained",
        definition=law_enforcement.civilian_complaints_sustained,
    ),
]
