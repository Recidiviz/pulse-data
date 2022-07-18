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
"""Helpers for bulk upload functionality."""

from typing import Optional, Type

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    DispositionType,
    ProsecutionAndDefenseStaffType,
)
from recidiviz.justice_counts.metrics import prosecution
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.persistence.database.schema.justice_counts import schema


@attr.define()
class MetricFile:
    """Describes the structure of a CSV file for a particular Justice Counts metric.
    If the metric has <= 1 disaggregation, there will be one corresponding file.
    If the metric has multiple disaggregations (e.g. gender and race) there will be
    one CSV for each disaggregation.
    """

    # The name of the CSV file (minus the .csv extension).
    filename: str
    # The definition of the corresponding Justice Counts metric.
    definition: MetricDefinition

    # The dimension by which this metric is disaggregated in this file,
    # e.g. RaceAndEthnicity.
    # (Note that each file can only contain a single disaggregation.)
    disaggregation: Optional[Type[DimensionBase]] = None
    # The name of the column that includes the dimension categories,
    # e.g. `race/ethnicity`.
    disaggregation_column_name: Optional[str] = None


PROSECUTION_METRIC_FILES = [
    MetricFile(
        filename="annual_budget",
        definition=prosecution.annual_budget,
    ),
    MetricFile(
        filename="caseloads",
        definition=prosecution.caseloads,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filename="cases_disposed",
        definition=prosecution.cases_disposed,
        disaggregation=DispositionType,
        disaggregation_column_name="disposition_type",
    ),
    MetricFile(
        filename="cases_referred",
        definition=prosecution.cases_referred,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filename="cases_rejected",
        definition=prosecution.cases_rejected,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filename="cases_rejected_by_gender",
        definition=prosecution.cases_rejected,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        filename="cases_rejected_by_raceethnicity",
        definition=prosecution.cases_rejected,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        filename="total_staff",
        definition=prosecution.total_staff,
        disaggregation=ProsecutionAndDefenseStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filename="violations_filed",
        definition=prosecution.violations,
    ),
]

# The `test_metricfile_list` unit test ensures that this dictionary includes
# all metrics registered for each system.
SYSTEM_TO_FILENAME_TO_METRICFILE = {
    schema.System.PROSECUTION: {
        metricfile.filename: metricfile for metricfile in PROSECUTION_METRIC_FILES
    },
}
