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
"""Tests for registry containing all official Justice Counts metrics."""

import unittest
from inspect import getmembers

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.justice_counts import metrics
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.justice_counts.metrics.metric_registry import (
    METRICS as registered_metrics,
)


class TestJusticeCountsMetricRegistry(unittest.TestCase):
    def test_all_metrics_in_metrics_registry(self) -> None:
        metrics_modules = ModuleCollectorMixin.get_submodules(
            metrics, submodule_name_prefix_filter=None
        )

        metrics_dict = {}
        for module in metrics_modules:
            for _, obj in getmembers(module):
                if isinstance(obj, MetricDefinition):
                    metrics_dict[obj.key] = obj

        registered_metrics_dict = {obj.key: obj for obj in registered_metrics}
        self.assertEqual(metrics_dict, registered_metrics_dict)
