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
"""Implements tests for Justice Counts Control Panel bulk upload helpers."""


from unittest import TestCase

from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_FILENAME_TO_METRICFILE,
)
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM


class TestJusticeCountsBulkUploadHelpers(TestCase):
    """Implements tests for the Justice Counts Control Panel bulk upload helpers."""

    def test_metricfile_list(self) -> None:
        # Ensure that all of a system's metrics are present in the SYSTEM_TO_FILENAME_TO_METRICFILE dictionary.
        for system, filename_to_metricfile in SYSTEM_TO_FILENAME_TO_METRICFILE.items():
            metric_keys = {
                metricfile.definition.key
                for metricfile in filename_to_metricfile.values()
            }
            registered_metric_keys = {
                m.key for m in METRICS_BY_SYSTEM[system] if not m.disabled
            }
            if metric_keys != registered_metric_keys:
                raise ValueError(
                    f"{system} has the following registered metric keys: {registered_metric_keys} "
                    f"and the following metric keys in SYSTEM_TO_FILENAME_TO_METRICFILE: {metric_keys}."
                )
