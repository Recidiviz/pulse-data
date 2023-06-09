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
""" Contains Dictionary that maps Metric, Breakdown pairs to their appropriate Bulk
Upload sheet name
"""

from typing import Dict

from recidiviz.justice_counts.metricfiles.metricfile_registry import metric_files

metric_definition_key_to_aggregate_metricfile = {
    metricfile.definition.key: metricfile
    for metricfile in metric_files
    if metricfile.disaggregation is None
}

# this mapping contains:
#   - key: (aggregate metricfile.canonical_filename, breakdown metricfile.disaggregation_column_name)
#   - value: breakdown metricfile.canonical_filename
METRIC_BREAKDOWN_PAIR_TO_SHEET_NAME: Dict[tuple[str, str], str] = {}
for metricfile in metric_files:
    if metricfile.disaggregation is None:
        continue
    aggregate_metricfile = metric_definition_key_to_aggregate_metricfile[
        metricfile.definition.key
    ]
    METRIC_BREAKDOWN_PAIR_TO_SHEET_NAME[
        (
            aggregate_metricfile.canonical_filename,
            metricfile.disaggregation_column_name,  # type: ignore[index]
        )
    ] = metricfile.canonical_filename
