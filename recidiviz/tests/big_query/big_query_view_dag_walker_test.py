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
"""Tests for classes in big_query_view_dag_walker.py"""

import datetime
import random
import re
import threading
import time
import unittest
from typing import Dict
from unittest.mock import patch

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    BigQueryViewDagNode,
    DagKey,
)
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_VIEW_BUILDERS,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


LATEST_VIEW_DATASET_REGEX = re.compile(r"(us_[a-z]{2})_raw_data_up_to_date_views")
MOCK_VIEW_PROCESS_TIME_SECONDS = 0.01


class TestBigQueryViewDagWalker(unittest.TestCase):
    """Tests for BigQueryViewDagWalker"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            self.all_views = [
                view_builder.build() for view_builder in DEPLOYED_VIEW_BUILDERS
            ]

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_dag_init(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        self.assertEqual(len(self.all_views), len(walker.nodes_by_key))

        for key, node in walker.nodes_by_key.items():
            if not node.parent_keys:
                self.assertIsValidEmptyParentsView(node)

            if node.is_root:
                for parent_key in node.parent_keys:
                    # Root views should only have source data tables as parents
                    self.assertIsValidSourceDataTable(key, parent_key)

    def test_dag_touches_all_views(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, DagKey]
        ) -> DagKey:
            time.sleep(MOCK_VIEW_PROCESS_TIME_SECONDS / 10)
            return DagKey.for_view(view)

        result = walker.process_dag(process_simple)

        expected_view_keys = set(walker.nodes_by_key)
        self.assertEqual(expected_view_keys, set(result.values()))

        walked_view_keys_from_process_results = set(result.values())
        self.assertEqual(expected_view_keys, walked_view_keys_from_process_results)

    def test_dag_process_time(self) -> None:
        num_views = len(self.all_views)
        walker = BigQueryViewDagWalker(self.all_views)

        serial_processing_time_seconds = num_views * MOCK_VIEW_PROCESS_TIME_SECONDS
        serial_processing_time = datetime.timedelta(
            seconds=serial_processing_time_seconds
        )

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, DagKey]
        ) -> DagKey:
            time.sleep(MOCK_VIEW_PROCESS_TIME_SECONDS)
            return DagKey.for_view(view)

        start = datetime.datetime.now()
        result = walker.process_dag(process_simple)
        end = datetime.datetime.now()

        self.assertEqual(num_views, len(result))

        processing_time = end - start

        # We expect to see significant speedup over the processing time if we ran the process function for each view
        # in series.
        self.assertLess(processing_time * 5, serial_processing_time)

    def test_dag_does_not_process_until_parents_processed(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        mutex = threading.Lock()
        all_processed = set()

        def process_check_parents(
            view: BigQueryView, parent_results: Dict[BigQueryView, None]
        ) -> None:
            with mutex:
                node = walker.node_for_view(view)
                if not node.is_root:
                    for parent_key in node.parent_keys:
                        if parent_key not in all_processed:
                            # The only parents that won't have been fully processed are source data tables
                            try:
                                self.assertIsValidSourceDataTable(
                                    child_view_key=node.dag_key,
                                    source_table_key=parent_key,
                                )
                            except ValueError as e:
                                raise ValueError(
                                    f"Found parent view [{parent_key}] that was not processed before "
                                    f"child [{node.dag_key}] started processing."
                                ) from e
                        else:
                            self.assertIn(
                                walker.view_for_key(parent_key), parent_results
                            )

            time.sleep(
                random.uniform(
                    MOCK_VIEW_PROCESS_TIME_SECONDS, MOCK_VIEW_PROCESS_TIME_SECONDS * 2
                )
            )
            with mutex:
                all_processed.add(node.dag_key)

        result = walker.process_dag(process_check_parents)
        self.assertEqual(len(self.all_views), len(result))

    def test_dag_returns_parent_results(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        def process_check_parents(
            _view: BigQueryView, parent_results: Dict[BigQueryView, int]
        ) -> int:
            if not parent_results:
                return 1
            return max(parent_results.values()) + 1

        result = walker.process_dag(process_check_parents)
        self.assertEqual(len(self.all_views), len(result))

        max_depth = 0
        max_depth_view = None
        for view, depth in result.items():
            if depth > max_depth:
                max_depth = depth
                max_depth_view = view
        if not max_depth_view:
            self.fail("Found no max_depth_view")
        max_depth_node = walker.node_for_view(max_depth_view)
        self.assertEqual(set(), max_depth_node.child_keys)

    def test_dag_exception_handling(self) -> None:
        """Test that exceptions during processing propagate properly."""

        class TestDagWalkException(ValueError):
            pass

        walker = BigQueryViewDagWalker(self.all_views)

        def process_throws(
            _view: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            raise TestDagWalkException()

        with self.assertRaises(TestDagWalkException):
            _ = walker.process_dag(process_throws)

        def process_throws_after_root(
            view: BigQueryView, _parent_results: Dict[BigQueryView, DagKey]
        ) -> DagKey:
            node = walker.node_for_view(view)
            if not node.is_root:
                raise TestDagWalkException()
            return node.dag_key

        with self.assertRaises(TestDagWalkException):
            _ = walker.process_dag(process_throws_after_root)

    def test_views_use_materialized_if_present(self) -> None:
        """Checks that each view is using the materialized version of a parent view, if
        one exists."""
        walker = BigQueryViewDagWalker(self.all_views)

        def process_check_using_materialized(
            view: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            node = walker.node_for_view(view)
            for parent_table_address in node.parent_tables:
                if parent_table_address in walker.materialized_addresss:
                    # We are using materialized version of a table
                    continue
                parent_key = DagKey(view_address=parent_table_address)
                if parent_key not in walker.nodes_by_key:
                    # We assume this is a source data table (checked in other tests)
                    continue
                parent_view: BigQueryView = walker.view_for_key(parent_key)
                self.assertIsNone(
                    parent_view.materialized_address,
                    f"Found view [{node.dag_key}] referencing un-materialized version "
                    f"of view [{parent_key}] when materialized table "
                    f"[{parent_view.materialized_address}] exists.",
                )

        result = walker.process_dag(process_check_using_materialized)
        self.assertEqual(len(self.all_views), len(result))

    def test_dag_with_cycle_at_root(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
        )

        with self.assertRaises(ValueError) as e:
            _ = BigQueryViewDagWalker([view_1, view_2])

        self.assertEqual(
            str(e.exception), "No roots detected. Input views contain a cycle."
        )

    def test_dag_with_cycle_at_root_with_other_valid_dag(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
        )

        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )

        with self.assertRaises(ValueError) as e:
            _ = BigQueryViewDagWalker([view_1, view_2, view_3])

        self.assertEqual(
            str(e.exception),
            "Detected cycle in graph reachable from ('dataset_1', 'table_1'): "
            "[('dataset_2', 'table_2'), ('dataset_1', 'table_1')]",
        )

    def test_dag_with_cycle_after_root(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_3.table_3`
            USING (col)""",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        )
        with self.assertRaises(ValueError) as e:
            _ = BigQueryViewDagWalker([view_1, view_2, view_3])
        self.assertEqual(
            str(e.exception),
            "Detected cycle in graph reachable from ('dataset_1', 'table_1'): "
            "[('dataset_2', 'table_2'), ('dataset_3', 'table_3')]",
        )

    def test_dag_no_cycle(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        )
        _ = BigQueryViewDagWalker([view_1, view_2, view_3])

    def test_find_full_parentage(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        )
        view_4 = BigQueryView(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_5 = BigQueryView(
            dataset_id="dataset_5",
            view_id="table_5",
            description="table_5 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        dag_walker = BigQueryViewDagWalker([view_1, view_2, view_3, view_4, view_5])

        # root node start
        start_node = dag_walker.node_for_view(view_1)
        parentage_set = dag_walker.find_full_parentage(
            curr_node=start_node,
            view_source_table_datasets={"source_dataset"},
        )
        self.assertEqual(
            {
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id="source_dataset", table_id="source_table"
                    )
                )
            },
            parentage_set,
        )

        # start in middle
        start_node = dag_walker.node_for_view(view_3)
        parentage_set = dag_walker.find_full_parentage(
            curr_node=start_node,
            view_source_table_datasets={"source_dataset"},
        )
        expected_parent_nodes = {
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="source_dataset", table_id="source_table"
                )
            ),
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="source_dataset", table_id="source_table_2"
                )
            ),
            DagKey.for_view(view_1),
            DagKey.for_view(view_2),
        }
        self.assertEqual(expected_parent_nodes, parentage_set)

        # single start node
        start_node = dag_walker.node_for_view(view_4)
        parentage_set = dag_walker.find_full_parentage(
            curr_node=start_node,
            view_source_table_datasets={"source_dataset"},
        )
        expected_parent_nodes = {
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="source_dataset", table_id="source_table"
                )
            ),
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="source_dataset", table_id="source_table_2"
                )
            ),
            DagKey.for_view(view_1),
            DagKey.for_view(view_2),
            DagKey.for_view(view_3),
        }
        self.assertEqual(expected_parent_nodes, parentage_set)

        # multiple start nodes
        start_nodes = [start_node, dag_walker.node_for_view(view_5)]

        parentage_set = set()
        for node in start_nodes:
            parentage_set = parentage_set.union(
                dag_walker.find_full_parentage(
                    curr_node=node,
                    view_source_table_datasets={"source_dataset"},
                )
            )

        self.assertEqual(expected_parent_nodes, parentage_set)

    def test_find_full_parentage_complex_dependencies(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                           SELECT * FROM `{project_id}.dataset_1.table_1`
                           JOIN `{project_id}.dataset_2.table_2`
                           USING (col)""",
        )
        view_4 = BigQueryView(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
                           SELECT * FROM `{project_id}.dataset_2.table_2`
                           JOIN `{project_id}.dataset_3.table_3`
                           USING (col)""",
        )

        dag_walker = BigQueryViewDagWalker([view_1, view_2, view_3, view_4])
        start_node = dag_walker.node_for_view(view_4)

        parentage_set = dag_walker.find_full_parentage(
            curr_node=start_node,
            view_source_table_datasets={"source_dataset"},
        )
        expected_parent_nodes = {
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="source_dataset", table_id="source_table"
                )
            ),
            DagKey.for_view(view_1),
            DagKey.for_view(view_2),
            DagKey.for_view(view_3),
        }
        self.assertEqual(expected_parent_nodes, parentage_set)

    def test_get_dfs_tree_str_for_table(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        )
        view_4 = BigQueryView(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_5 = BigQueryView(
            dataset_id="dataset_5",
            view_id="table_5",
            description="table_5 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        dag_walker = BigQueryViewDagWalker([view_1, view_2, view_3, view_4, view_5])

        # Top level view
        tree = dag_walker.get_dfs_tree_str_for_table(
            dataset_id=view_5.dataset_id, table_id=view_5.table_id
        )
        expected_tree = """dataset_5.table_5
|--dataset_3.table_3
|----dataset_2.table_2
|------source_dataset.source_table_2
|----dataset_1.table_1
|------source_dataset.source_table
"""
        self.assertEqual(expected_tree, tree)

        # Middle of tree
        tree = dag_walker.get_dfs_tree_str_for_table(
            dataset_id=view_3.dataset_id, table_id=view_3.table_id
        )
        expected_tree = """dataset_3.table_3
|--dataset_2.table_2
|----source_dataset.source_table_2
|--dataset_1.table_1
|----source_dataset.source_table
"""
        self.assertEqual(expected_tree, tree)

        # Skip datasets
        tree = dag_walker.get_dfs_tree_str_for_table(
            dataset_id=view_3.dataset_id,
            table_id=view_3.table_id,
            datasets_to_skip={"dataset_1"},
        )
        expected_tree = """dataset_3.table_3
|--dataset_2.table_2
|----source_dataset.source_table_2
|--source_dataset.source_table
"""
        self.assertEqual(expected_tree, tree)

        # Custom formatted
        def _custom_formatter(dag_key: DagKey) -> str:
            return f"custom_formatted_{dag_key.dataset_id}_{dag_key.table_id}"

        tree = dag_walker.get_dfs_tree_str_for_table(
            dataset_id=view_3.dataset_id,
            table_id=view_3.table_id,
            custom_node_formatter=_custom_formatter,
        )

        expected_tree = """custom_formatted_dataset_3_table_3
|--custom_formatted_dataset_2_table_2
|----custom_formatted_source_dataset_source_table_2
|--custom_formatted_dataset_1_table_1
|----custom_formatted_source_dataset_source_table
"""
        self.assertEqual(expected_tree, tree)

        # Source table
        tree = dag_walker.get_dfs_tree_str_for_table(
            dataset_id="source_dataset",
            table_id="source_table",
        )
        expected_tree = """source_dataset.source_table
"""
        self.assertEqual(expected_tree, tree)

    def test_dag_materialized_view_clobbers_other(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id=view_1.dataset_id, table_id=view_1.view_id
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        with self.assertRaises(ValueError) as e:
            _ = BigQueryViewDagWalker([view_1, view_2])
        self.assertTrue(
            str(e.exception).startswith(
                "Found materialized view address for view [('dataset_2', 'table_2')] that "
                "matches the view address of another view [('dataset_1', 'table_1')]."
            )
        )

    def test_dag_two_views_same_materialized_address(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset", table_id="other_table"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset", table_id="other_table"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        with self.assertRaises(ValueError) as e:
            _ = BigQueryViewDagWalker([view_1, view_2])
        self.assertTrue(
            str(e.exception).startswith(
                "Found materialized view address for view [('dataset_2', 'table_2')] "
                "that matches materialized_address of another view: "
                "[('dataset_1', 'table_1')]."
            )
        )

    def test_dag_parents_materialized(self) -> None:
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                SELECT * FROM `{project_id}.dataset_1.table_1`
                JOIN `{project_id}.dataset_2.table_2_materialized`
                USING (col)""",
        )
        walker = BigQueryViewDagWalker([view_1, view_2, view_3])

        def process_simple(
            view: BigQueryView, parent_results: Dict[BigQueryView, DagKey]
        ) -> str:
            if view == view_3:
                # View 3 should have two parents
                self.assertEqual(
                    {view_1: view_1.view_id, view_2: view_2.view_id}, parent_results
                )

            return view.view_id

        result = walker.process_dag(process_simple)
        self.assertEqual(
            {view_1: view_1.view_id, view_2: view_2.view_id, view_3: view_3.view_id},
            result,
        )

    def test_dag_parents_materialized_non_default(self) -> None:
        self.maxDiff = None
        view_1 = BigQueryView(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_1", table_id="other_table_1"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_2 = BigQueryView(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_2", table_id="other_table_2"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_3 = BigQueryView(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                SELECT * FROM `{project_id}.dataset_1.table_1`
                JOIN `{project_id}.other_dataset_2.other_table_2`
                USING (col)""",
        )
        walker = BigQueryViewDagWalker([view_1, view_2, view_3])

        def process_simple(
            view: BigQueryView, parent_results: Dict[BigQueryView, DagKey]
        ) -> str:
            if view == view_3:
                # View 3 should have two parents
                self.assertEqual(
                    {view_1: view_1.view_id, view_2: view_2.view_id}, parent_results
                )

            return view.view_id

        result = walker.process_dag(process_simple)
        self.assertEqual(
            {view_1: view_1.view_id, view_2: view_2.view_id, view_3: view_3.view_id},
            result,
        )

    def assertIsValidEmptyParentsView(self, node: BigQueryViewDagNode) -> None:
        """Fails the test if a view that has no parents is an expected view with no
        parents. Failures could be indicative of poorly formed view queries.
        """
        known_empty_parent_view_addresss = {
            # These views unnest data from a static list
            BigQueryAddress(
                dataset_id="census_views", table_id="charge_class_severity_ranks"
            ),
            BigQueryAddress(
                dataset_id="analyst_data",
                table_id="admission_start_reason_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="analyst_data",
                table_id="release_termination_reason_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="analyst_data", table_id="violation_type_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="analyst_data", table_id="supervision_level_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="analyst_data", table_id="compartment_level_2_dedup_priority"
            ),
            # Generate data using pure date functions
            BigQueryAddress(
                dataset_id="reference_views", table_id="covid_report_weeks"
            ),
            BigQueryAddress(
                dataset_id="population_projection_data", table_id="simulation_run_dates"
            ),
        }
        if node.dag_key.view_address in known_empty_parent_view_addresss:
            return

        if "FROM EXTERNAL_QUERY" in node.view.view_query:
            return

        self.fail(node.dag_key)

    @staticmethod
    def assertIsValidSourceDataTable(
        child_view_key: DagKey, source_table_key: DagKey
    ) -> None:
        if source_table_key.dataset_id in VIEW_SOURCE_TABLE_DATASETS:
            return

        raise ValueError(
            f"Found parent [{source_table_key}] of BQ graph root node [{child_view_key}] that is not in an "
            f"expected raw inputs dataset."
        )

    @staticmethod
    def assertIsValidRawTableViewDependency(
        child_view_key: DagKey, raw_data_view_key: DagKey
    ) -> None:
        """Asserts that the dependency (i.e. edge in the graph) that is being walked is a valid one to walk."""
        up_to_date_view_match = re.match(
            LATEST_VIEW_DATASET_REGEX, raw_data_view_key.dataset_id
        )
        if not up_to_date_view_match:
            raise ValueError(
                f"Expected dataset matching *_raw_data_up_to_date_views, found: "
                f"[{raw_data_view_key.dataset_id}]"
            )
        region_code = up_to_date_view_match.group(1)
        region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=region_code
        )
        raw_data_latest_view_match = re.match(
            r"([A-Za-z_\d]+)_latest", raw_data_view_key.table_id
        )
        if not raw_data_latest_view_match:
            raise ValueError(
                f"Unexpected table_id [{raw_data_view_key.table_id}] for parent view "
                f"[{raw_data_view_key}] referenced by [{child_view_key}]"
            )
        raw_file_tag = raw_data_latest_view_match.group(1)
        if raw_file_tag not in region_raw_file_config.raw_file_configs:
            raise ValueError(
                f"No raw file config exists for raw data view [{raw_data_view_key}] "
                f"referenced by view [{child_view_key}]."
            )

        config = region_raw_file_config.raw_file_configs[raw_file_tag]
        if not config.primary_key_cols:
            raise ValueError(
                f"Raw file [{raw_file_tag}] has no primary key cols defined - the _latest view for "
                f"this table will not be created until these are defined. Cannot reference "
                f"[{raw_data_view_key}] in another BQ view."
            )


class TestBigQueryViewDagNode(unittest.TestCase):
    """Tests for BigQueryViewDagNode"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_parse_simple_view(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.some_table`",
        )
        node = BigQueryViewDagNode(view)
        self.assertIsNone(view.materialized_address)
        node.set_materialized_addresss({})
        self.assertEqual(node.is_root, False)
        self.assertEqual(
            node.dag_key,
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id="my_dataset", table_id="my_view_id"
                )
            ),
        )
        self.assertEqual(
            node.parent_keys,
            {
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id="some_dataset", table_id="some_table"
                    )
                )
            },
        )
        self.assertEqual(node.child_keys, set())

        node.is_root = True
        child_key = DagKey(
            view_address=BigQueryAddress(
                dataset_id="other_dataset", table_id="other_table"
            )
        )
        node.add_child_key(child_key)

        self.assertEqual(node.is_root, True)
        self.assertEqual(node.child_keys, {child_key})

    def test_parse_view_materialized_parent(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.some_table_materialized`",
        )
        parent_view = BigQueryView(
            dataset_id="some_dataset",
            view_id="some_table",
            description="my parent view description",
            view_query_template="SELECT * FROM UNNEST([])",
            should_materialize=True,
        )
        node = BigQueryViewDagNode(view)
        if not parent_view.materialized_address:
            raise ValueError("Null materialized_address for view [{parent_view}]")
        node.set_materialized_addresss(
            {parent_view.materialized_address: DagKey.for_view(parent_view)}
        )
        self.assertEqual(
            node.parent_keys,
            {
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id="some_dataset", table_id="some_table"
                    )
                )
            },
        )

    def test_parse_view_multiple_parents(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="""SELECT * FROM `{project_id}.some_dataset.some_table`
            LEFT OUTER JOIN `{project_id}.some_dataset.other_table`
            USING (some_col);
            """,
        )
        node = BigQueryViewDagNode(view)
        node.set_materialized_addresss({})
        self.assertEqual(
            node.parent_keys,
            {
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id="some_dataset", table_id="some_table"
                    )
                ),
                DagKey(
                    view_address=BigQueryAddress(
                        dataset_id="some_dataset", table_id="other_table"
                    )
                ),
            },
        )
