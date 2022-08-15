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
    """Describes the structure of a CSV file for a particular Justice Counts metric,
    according to the technical specification. This format is used for bulk upload/ingest
    and for generating the public feeds.

    If the metric has no disaggregations, there will be one corresponding file.
    If the metric has disaggregations (e.g. gender and race) there will be
    one CSV for the aggregate value and one CSV for each disaggregation.
    """

    # Filename used for the technical specifiction.
    canonical_filename: str

    # The definition of the corresponding Justice Counts metric.
    definition: MetricDefinition

    # The dimension by which this metric is disaggregated in this file,
    # e.g. RaceAndEthnicity.
    # (Note that each file can only contain a single disaggregation.)
    disaggregation: Optional[Type[DimensionBase]] = None
    # The name of the column that includes the dimension categories,
    # e.g. `race/ethnicity`.
    disaggregation_column_name: Optional[str] = None

    # List of filenames that are accepted during bulk upload.
    allowed_filenames: List[str] = attr.field(factory=list)

    def __attrs_post_init__(self) -> None:
        self.allowed_filenames = list(
            set(self.allowed_filenames) | {self.canonical_filename}
        )
