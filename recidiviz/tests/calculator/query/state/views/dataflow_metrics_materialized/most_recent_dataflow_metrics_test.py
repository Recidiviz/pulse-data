# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for most_recent_dataflow_metrics.py"""
# pylint: disable=protected-access
import unittest

from mock import patch
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    METRIC_TABLES_JOIN_OVERRIDES,
    _make_most_recent_metric_view_builder,
    METRICS_VIEWS_TO_MATERIALIZE,
    DEFAULT_JOIN_INDICES,
)


def make_using_clause(join_indices: str) -> str:
    return f"USING ({join_indices})"


class MostRecentDataflowMetricsTest(unittest.TestCase):
    """Tests for the generated most_recent_* views"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_replace_join_clause(self) -> None:
        """Test that special-cased tables use the appropriate JOIN indices"""
        metric_names = METRIC_TABLES_JOIN_OVERRIDES.keys()
        for metric_name in metric_names:
            builder = _make_most_recent_metric_view_builder(metric_name)
            query = builder.build()
            using_clause = make_using_clause(METRIC_TABLES_JOIN_OVERRIDES[metric_name])
            self.assertIn(using_clause, query.__repr__())

    def test_default_join_clause(self) -> None:
        """Test that non-special-cased tables use the default JOIN indices"""
        using_clause = make_using_clause(DEFAULT_JOIN_INDICES)
        for metric_name in [
            name
            for name in METRICS_VIEWS_TO_MATERIALIZE
            if name not in METRIC_TABLES_JOIN_OVERRIDES
        ]:
            query = _make_most_recent_metric_view_builder(metric_name).build()
            self.assertIn(using_clause, query.__repr__())
