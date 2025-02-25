# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for view_config.py."""

import unittest

import mock

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.calculator.query.state import dataset_config, view_config
from recidiviz.calculator.query.state.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE,
)
from recidiviz.calculator.query.state.views.reference.reference_views import (
    REFERENCE_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.shared_metric.shared_metric_views import (
    SHARED_METRIC_VIEW_BUILDERS,
)
from recidiviz.metrics.export import export_config


# TODO(#18189): Refactor reference_views dataset so we can write tests here that
#  enforce that the `normalized_state` / `state` datasets are only used where
#  appropriate.
class ViewExportConfigTest(unittest.TestCase):
    """Tests for the export variables in view_config.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @mock.patch("google.cloud.bigquery.Client")
    def test_VIEW_COLLECTION_EXPORT_CONFIGS_types(
        self, _mock_client: mock.MagicMock
    ) -> None:
        """Make sure that all view_builders in the view_builders_to_export attribute of
        VIEW_COLLECTION_EXPORT_CONFIGS are of type BigQueryViewBuilder, and that running view_builder.build()
        produces a BigQueryView."""
        for (
            dataset_export_config
        ) in export_config.VIEW_COLLECTION_EXPORT_INDEX.values():
            for view_builder in dataset_export_config.view_builders_to_export:
                self.assertIsInstance(view_builder, BigQueryViewBuilder)
                view = view_builder.build()
                self.assertIsInstance(view, BigQueryView)

    @staticmethod
    def test_building_all_views() -> None:
        """Tests that all view_builders in VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE successfully pass validations and build."""
        for view_builder in view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE:
            _ = view_builder.build()

    def test_no_dataflow_descendants_in_reference_views(self) -> None:
        """Enforces that none of the REFERENCE_VIEWS pull from Dataflow metrics."""

        all_views_dag_walker = BigQueryViewDagWalker(
            [view_builder.build() for view_builder in VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE]
        )
        all_views_dag_walker.populate_ancestor_sub_dags()

        for view_builder in REFERENCE_VIEW_BUILDERS:
            sub_dag = all_views_dag_walker.node_for_view(
                view_builder.build()
            ).ancestors_sub_dag
            for view in sub_dag.views:
                self.assertNotEqual(
                    dataset_config.DATAFLOW_METRICS_DATASET,
                    view.address.dataset_id,
                    f"View {view_builder.dataset_id}.{view_builder.view_id} relies on "
                    f"Dataflow metrics. Shared views that pull from Dataflow metrics "
                    f"should be in the shared_metric_views dataset, not the "
                    f"reference_views dataset.",
                )

    def test_all_dataflow_descendants_in_shared_metric_views(self) -> None:
        """Enforces that all the SHARED_METRIC_VIEWS pull from Dataflow metrics."""

        all_views_dag_walker = BigQueryViewDagWalker(
            [view_builder.build() for view_builder in VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE]
        )
        all_views_dag_walker.populate_ancestor_sub_dags()

        for view_builder in SHARED_METRIC_VIEW_BUILDERS:
            dataflow_parent = False
            sub_dag = all_views_dag_walker.node_for_view(
                view_builder.build()
            ).ancestors_sub_dag
            for view in sub_dag.views:
                node = all_views_dag_walker.node_for_view(view)
                # check source tables which exist beyond ancestors
                for key in node.parent_node_addresses | node.source_addresses:
                    if key.dataset_id == dataset_config.DATAFLOW_METRICS_DATASET:
                        dataflow_parent = True
            self.assertTrue(
                dataflow_parent,
                f"Found view {view_builder.dataset_id}.{view_builder.view_id} "
                f"that does not pull from Dataflow metrics. If this is a reference "
                f"view used by other views, then it should be in the reference_views "
                f"dataset instead.",
            )
