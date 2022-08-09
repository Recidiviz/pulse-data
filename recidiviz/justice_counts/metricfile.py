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
"""Defines the MetricFile class."""

from typing import List, Optional, Type

import attr

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition


@attr.define()
class MetricFile:
    """Describes the structure of a CSV file for a particular Justice Counts metric.
    If the metric has <= 1 disaggregation, there will be one corresponding file.
    If the metric has multiple disaggregations (e.g. gender and race) there will be
    one CSV for each disaggregation.
    """

    # Allowed names of the CSV file (minus the .csv extension).
    # We use a list of allowed names because fuzzy matching doesn't
    # work well at distinguishing them
    filenames: List[str]
    # The definition of the corresponding Justice Counts metric.
    definition: MetricDefinition

    # The dimension by which this metric is disaggregated in this file,
    # e.g. RaceAndEthnicity.
    # (Note that each file can only contain a single disaggregation.)
    disaggregation: Optional[Type[DimensionBase]] = None
    # The name of the column that includes the dimension categories,
    # e.g. `race/ethnicity`.
    disaggregation_column_name: Optional[str] = None

    # Indicates whether this file contains a non-primary aggregation,
    # like gender or race. In this case, the aggregate values don't
    # need to be reported, because they already have been reported
    # on the primary aggregation. If they are reported, they should
    # match the primary aggregation's values.
    supplementary_disaggregation: bool = False
