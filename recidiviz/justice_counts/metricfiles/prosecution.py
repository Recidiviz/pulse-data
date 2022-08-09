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
"""Metricfile objects used for prosecution metrics."""

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
from recidiviz.justice_counts.metrics import prosecution

PROSECUTION_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=prosecution.annual_budget,
    ),
    MetricFile(
        filenames=["caseloads"],
        definition=prosecution.caseloads,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_disposed"],
        definition=prosecution.cases_disposed,
        disaggregation=DispositionType,
        disaggregation_column_name="disposition_type",
    ),
    MetricFile(
        filenames=["cases_referred"],
        definition=prosecution.cases_referred,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_rejected"],
        definition=prosecution.cases_rejected,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_rejected_by_gender", "cases_rejected_gender"],
        definition=prosecution.cases_rejected,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["cases_rejected_by_raceethnicity", "cases_rejected_race"],
        definition=prosecution.cases_rejected,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=prosecution.total_staff,
        disaggregation=ProsecutionAndDefenseStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["violations_filed"],
        definition=prosecution.violations,
    ),
]
