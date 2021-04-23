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
"""Tests for dataset_overrides.py."""

import unittest
from typing import Set, Sequence

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
    BigQueryAddress,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.tools.utils.dataset_overrides import (
    dataset_overrides_for_deployed_view_datasets,
    dataset_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE


class TestDatasetOverrides(unittest.TestCase):
    """Tests for dataset_overrides.py."""

    @staticmethod
    def _all_datasets(builders: Sequence[BigQueryViewBuilder]) -> Set[str]:
        datasets = set()
        for builder in builders:
            datasets.add(builder.dataset_id)
            if builder.materialized_address_override:
                datasets.add(builder.materialized_address_override.dataset_id)
        return datasets

    def test_dataset_overrides_for_view_builders(self) -> None:
        view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_2",
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
                materialized_address_override=BigQueryAddress(
                    dataset_id="materialized_dataset", table_id="table_materialized"
                ),
            ),
        ]

        prefix = "my_prefix"
        overrides = dataset_overrides_for_view_builders(prefix, view_builders)

        expected_overrides = {
            "dataset_1": "my_prefix_dataset_1",
            "dataset_2": "my_prefix_dataset_2",
            "materialized_dataset": "my_prefix_materialized_dataset",
        }

        self.assertEqual(expected_overrides, overrides)

    def test_dataset_overrides_for_deployed_view_datasets(self) -> None:
        prefix = "my_prefix"
        overrides = dataset_overrides_for_deployed_view_datasets(prefix)

        datasets: Set[str] = set()
        for builders in DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE.values():
            datasets = datasets.union(self._all_datasets(builders))

        datasets.remove(DATAFLOW_METRICS_MATERIALIZED_DATASET)
        self.assertEqual(datasets, set(overrides.keys()))
        for override in overrides.values():
            self.assertTrue(override.startswith(prefix))

    def test_dataset_overrides_for_deployed_view_datasets_dataflow_override(
        self,
    ) -> None:
        prefix = "my_prefix"
        dataflow_dataset_override = "test_dataflow"
        overrides = dataset_overrides_for_deployed_view_datasets(
            prefix, dataflow_dataset_override
        )

        datasets: Set[str] = set()
        for builders in DEPLOYED_VIEW_BUILDERS_BY_NAMESPACE.values():
            datasets = datasets.union(self._all_datasets(builders))

        datasets.add(DATAFLOW_METRICS_DATASET)

        self.assertEqual(datasets, set(overrides.keys()))
