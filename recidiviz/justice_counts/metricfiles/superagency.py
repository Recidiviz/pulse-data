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
"""Metricfile objects used for Superagency metrics."""

from recidiviz.justice_counts.dimensions.superagency import (
    ExpenseType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import superagency

SUPERAGENCY_METRIC_FILES = [
    MetricFile(
        canonical_filename="funding",
        definition=superagency.funding,
    ),
    MetricFile(
        canonical_filename="funding_by_type",
        definition=superagency.funding,
        disaggregation=FundingType,
        disaggregation_column_name="funding_type",
    ),
    MetricFile(
        canonical_filename="expenses",
        definition=superagency.expenses,
    ),
    MetricFile(
        canonical_filename="expenses_by_type",
        definition=superagency.expenses,
        disaggregation=ExpenseType,
        disaggregation_column_name="expense_type",
    ),
    MetricFile(
        canonical_filename="staff",
        definition=superagency.staff,
    ),
    MetricFile(
        canonical_filename="staff_by_type",
        definition=superagency.staff,
        disaggregation=StaffType,
        disaggregation_column_name="staff_type",
    ),
]
