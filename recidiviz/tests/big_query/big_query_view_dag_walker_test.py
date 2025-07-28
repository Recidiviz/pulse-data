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

import abc
import datetime
import re
import threading
import time
import unittest
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagNode,
    BigQueryViewDagWalker,
    ProcessDagPerfConfig,
    TraversalDirection,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_views import deployed_view_builders

LATEST_VIEW_DATASET_REGEX = re.compile(r"(us_[a-z]{2})_raw_data_up_to_date_views")
MOCK_VIEW_PROCESS_TIME_SECONDS = 0.01


# View builders for views forming a DAG shaped like an X:
#  1     2
#   \   /
#     3
#   /   \
#  4     5
X_SHAPED_DAG_VIEW_BUILDERS_LIST = [
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_1",
        view_id="table_1",
        description="table_1 description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_2",
        view_id="table_2",
        description="table_2 description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_3",
        view_id="table_3",
        description="table_3 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_4",
        view_id="table_4",
        description="table_4 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_5",
        view_id="table_5",
        description="table_5 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
    ),
]

# View builders for views forming a DAG with a diamond in it:
#  1     2
#   \   /
#     3
#   /   \
#  4     5
#   \   /
#     6
DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST = [
    *X_SHAPED_DAG_VIEW_BUILDERS_LIST.copy(),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_6",
        view_id="table_6",
        description="table_6 description",
        view_query_template="""
            SELECT * FROM `{project_id}.dataset_4.table_4`
            JOIN `{project_id}.dataset_5.table_5`
            USING (col)""",
    ),
]


class TestBigQueryViewDagWalkerBase(unittest.TestCase):
    """Tests for BigQueryViewDagWalker"""

    __test__ = False
    project_id_patcher: Any
    all_views: List[BigQueryView]
    x_shaped_dag_views_list: List[BigQueryView]
    diamond_shaped_dag_views_list: List[BigQueryView]

    @classmethod
    def setUpClass(cls) -> None:
        cls.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        cls.project_id_patcher.start().return_value = "recidiviz-456"

        cls.all_views = [
            view_builder.build() for view_builder in deployed_view_builders()
        ]

        cls.x_shaped_dag_views_list = [
            b.build() for b in X_SHAPED_DAG_VIEW_BUILDERS_LIST
        ]

        cls.diamond_shaped_dag_views_list = [
            b.build() for b in DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST
        ]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.project_id_patcher.stop()

    @property
    @abc.abstractmethod
    def synchronous(self) -> bool:
        raise NotImplementedError

    def test_view_for_address(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        view = self.diamond_shaped_dag_views_list[1]
        self.assertEqual(view, all_views_dag_walker.view_for_address(view.address))

    def test_views_for_addresses(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        view_1 = self.diamond_shaped_dag_views_list[1]
        view_2 = self.diamond_shaped_dag_views_list[2]
        self.assertEqual(
            [view_1, view_2],
            all_views_dag_walker.views_for_addresses([view_1.address, view_2.address]),
        )

    def test_dag_touches_all_views(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> BigQueryAddress:
            return view.address

        result = walker.process_dag(process_simple, synchronous=self.synchronous)

        expected_view_addresses = set(walker.nodes_by_address)
        self.assertEqual(expected_view_addresses, set(result.view_results.values()))

        walked_view_addresses_from_process_results = set(result.view_results.values())
        self.assertEqual(
            expected_view_addresses, walked_view_addresses_from_process_results
        )

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
                    for parent_address in node.parent_node_addresses:
                        if parent_address not in all_processed:
                            # The only parents that won't have been fully processed are
                            # source data tables
                            try:
                                self.assertIsValidSourceDataTable(
                                    child_view_address=node.view.address,
                                    source_table_address=parent_address,
                                )
                            except ValueError as e:
                                raise ValueError(
                                    f"Found parent view [{parent_address.to_str()}] "
                                    f"that was not processed before child "
                                    f"[{node.view.address}] started processing."
                                ) from e
                        else:
                            self.assertIn(
                                walker.view_for_address(parent_address),
                                parent_results,
                            )

            with mutex:
                all_processed.add(node.view.address)

        result = walker.process_dag(process_check_parents, synchronous=self.synchronous)
        self.assertEqual(len(self.all_views), len(result.view_results))

    def test_dag_returns_parent_results(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        def process_check_parents(
            _view: BigQueryView, parent_results: Dict[BigQueryView, int]
        ) -> int:
            if not parent_results:
                return 1
            return max(parent_results.values()) + 1

        result = walker.process_dag(process_check_parents, synchronous=self.synchronous)
        self.assertEqual(len(self.all_views), len(result.view_results))

        max_depth = 0
        max_depth_view = None
        for view, depth in result.view_results.items():
            if depth > max_depth:
                max_depth = depth
                max_depth_view = view
        if not max_depth_view:
            self.fail("Found no max_depth_view")
        max_depth_node = walker.node_for_view(max_depth_view)
        self.assertEqual(set(), max_depth_node.child_node_addresses)

    def test_dag_processing_can_be_reversed(self) -> None:
        #  We would like to be able to traverse our DAG both top
        #  down and bottom up. In the case of the diamond:
        #
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #   \   /
        #     6
        #
        # The expectation is that node '3' is processed after
        # both nodes '1' and '2' and before nodes '4' and '5'.
        # However, when reversed:
        #
        #     6
        #   /   \
        #  4     5
        #   \   /
        #     3
        #   /   \
        #  1     2
        #
        # The expectation is node '3' will be processed after
        # both nodes '4' and '5' but before '1' and '2'.

        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        (
            view_1,
            view_2,
            view_3,
            view_4,
            view_5,
            view_6,
        ) = self.diamond_shaped_dag_views_list

        def processing_ordered_string(
            _: BigQueryView, parent_results: Dict[BigQueryView, str]
        ) -> List[str]:
            return sorted([f"{p.dataset_id}.{p.table_id}" for p in parent_results])

        forward_results = walker.process_dag(
            processing_ordered_string, synchronous=self.synchronous
        )
        assert forward_results.view_results == {
            view_1: [],
            view_2: [],
            view_3: ["dataset_1.table_1", "dataset_2.table_2"],
            view_4: ["dataset_3.table_3"],
            view_5: ["dataset_3.table_3"],
            view_6: ["dataset_4.table_4", "dataset_5.table_5"],
        }

        backwards_results = walker.process_dag(
            processing_ordered_string,
            synchronous=self.synchronous,
            traversal_direction=TraversalDirection.LEAVES_TO_ROOTS,
        )
        assert backwards_results.view_results == {
            view_6: [],
            view_5: ["dataset_6.table_6"],
            view_4: ["dataset_6.table_6"],
            view_3: ["dataset_4.table_4", "dataset_5.table_5"],
            view_2: ["dataset_3.table_3"],
            view_1: ["dataset_3.table_3"],
        }

        assert (
            forward_results.get_distinct_paths_to_leaf_nodes()
            == backwards_results.get_distinct_paths_to_leaf_nodes()
            == 4
        )

    def test_dag_exception_handling(self) -> None:
        """Test that exceptions during processing propagate properly."""

        class TestDagWalkException(ValueError):
            pass

        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        def process_throws(
            _view: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            raise TestDagWalkException()

        with self.assertRaises(TestDagWalkException):
            walker.process_dag(process_throws, synchronous=self.synchronous)

        def process_throws_after_root(
            view: BigQueryView, _parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> BigQueryAddress:
            node = walker.node_for_view(view)
            if not node.is_root:
                raise TestDagWalkException()
            return node.view.address

        with self.assertRaises(TestDagWalkException):
            walker.process_dag(process_throws_after_root, synchronous=self.synchronous)

    def test_dag_with_cycle_at_root_with_other_valid_dag(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
        ).build()

        view_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()

        with self.assertRaisesRegex(
            ValueError,
            r"^Detected cycle in graph reachable from dataset_1.table_1: "
            r"\['dataset_2.table_2']$",
        ):
            _ = BigQueryViewDagWalker([view_1, view_2, view_3])

    def test_dag_with_cycle_after_root(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_3.table_3`
            USING (col)""",
        ).build()
        view_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        ).build()
        with self.assertRaisesRegex(
            ValueError,
            r"^Detected cycle in graph reachable from dataset_1.table_1: "
            r"\['dataset_2.table_2', 'dataset_3.table_3'\]$",
        ):
            _ = BigQueryViewDagWalker([view_1, view_2, view_3])

    def test_dag_no_cycle(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        ).build()
        view_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        ).build()
        _ = BigQueryViewDagWalker([view_1, view_2, view_3])

    def test_populate_ancestors_sub_dag(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        all_views_dag_walker.populate_ancestor_sub_dags()

        all_sub_dag_views = {
            key: sorted(v.address for v in node.ancestors_sub_dag.views)
            for key, node in all_views_dag_walker.nodes_by_address.items()
        }

        # Expected ancestor DAG views by view given this diamond structure:
        #    1     2
        #     \   /
        #       3
        #     /   \
        #    4     5
        #     \   /
        #       6
        expected_results = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"): [
                # All ancestor DAGs include the target view
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
            ],
            BigQueryAddress(dataset_id="dataset_2", table_id="table_2"): [
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2")
            ],
            BigQueryAddress(dataset_id="dataset_3", table_id="table_3"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
            ],
            BigQueryAddress(dataset_id="dataset_4", table_id="table_4"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
            ],
            BigQueryAddress(dataset_id="dataset_5", table_id="table_5"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
            ],
            BigQueryAddress(dataset_id="dataset_6", table_id="table_6"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
        }
        self.assertEqual(expected_results, all_sub_dag_views)

    def test_populate_descendants_sub_dag(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        all_views_dag_walker.populate_descendant_sub_dags()

        all_sub_dag_views = {
            key: sorted(v.address for v in node.descendants_sub_dag.views)
            for key, node in all_views_dag_walker.nodes_by_address.items()
        }

        # Expected descendant DAG views by view given this diamond structure:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #   \   /
        #     6
        expected_results = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
            BigQueryAddress(dataset_id="dataset_2", table_id="table_2"): [
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
            BigQueryAddress(dataset_id="dataset_3", table_id="table_3"): [
                BigQueryAddress(dataset_id="dataset_3", table_id="table_3"),
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
            BigQueryAddress(dataset_id="dataset_4", table_id="table_4"): [
                BigQueryAddress(dataset_id="dataset_4", table_id="table_4"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
            BigQueryAddress(dataset_id="dataset_5", table_id="table_5"): [
                BigQueryAddress(dataset_id="dataset_5", table_id="table_5"),
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6"),
            ],
            BigQueryAddress(dataset_id="dataset_6", table_id="table_6"): [
                # All descendant DAGs include the target view
                BigQueryAddress(dataset_id="dataset_6", table_id="table_6")
            ],
        }

        self.assertEqual(expected_results, all_sub_dag_views)

    def test_populate_sub_dag_empty(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker([])
        all_views_dag_walker.populate_descendant_sub_dags()
        all_views_dag_walker.populate_ancestor_sub_dags()

    def test_populate_descendant_sub_dag_one_view(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(
            self.diamond_shaped_dag_views_list[0:1]
        )

        all_views_dag_walker.populate_descendant_sub_dags()

        all_sub_dag_views = {
            key: sorted(v.address for v in node.descendants_sub_dag.views)
            for key, node in all_views_dag_walker.nodes_by_address.items()
        }

        expected_results = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
            ]
        }
        self.assertEqual(expected_results, all_sub_dag_views)

    def test_populate_ancestor_sub_dag_one_view(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(
            self.diamond_shaped_dag_views_list[0:1]
        )

        all_views_dag_walker.populate_ancestor_sub_dags()

        all_sub_dag_views = {
            key: sorted(v.address for v in node.ancestors_sub_dag.views)
            for key, node in all_views_dag_walker.nodes_by_address.items()
        }

        expected_results = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"): [
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
            ]
        }
        self.assertEqual(expected_results, all_sub_dag_views)

    def test_get_sub_dag_root_node(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            [self.x_shaped_dag_views_list[0]],
        )

        expected_views = [
            v
            for v in self.x_shaped_dag_views_list
            # This source table does not depend on "table_1"
            if v.view_id != "table_2"
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(
            [self.x_shaped_dag_views_list[0]]
        )

        # Only should include this view
        expected_views = [self.x_shaped_dag_views_list[0]]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_get_sub_dag_middle_node(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # Get descendants sub-dag
        descendants_sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            [self.x_shaped_dag_views_list[2]],
        )

        expected_views = [
            self.x_shaped_dag_views_list[2],
            self.x_shaped_dag_views_list[3],
            self.x_shaped_dag_views_list[4],
        ]

        self.assertCountEqual(expected_views, descendants_sub_dag.views)

        # Get ancestors sub-dag
        ancestors_sub_dag = all_views_dag_walker.get_ancestors_sub_dag(
            [self.x_shaped_dag_views_list[2]]
        )

        expected_views = [
            self.x_shaped_dag_views_list[0],
            self.x_shaped_dag_views_list[1],
            self.x_shaped_dag_views_list[2],
        ]

        self.assertCountEqual(expected_views, ancestors_sub_dag.views)

        # Get both directions sub-dag
        both_directions_dag = BigQueryViewDagWalker.union_dags(
            descendants_sub_dag, ancestors_sub_dag
        )

        expected_views = self.x_shaped_dag_views_list

        self.assertCountEqual(expected_views, both_directions_dag.views)

    def test_get_sub_dag_leaf_node(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            [self.x_shaped_dag_views_list[4]],
        )

        # Only should include this view
        expected_views = [self.x_shaped_dag_views_list[4]]

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(
            [self.x_shaped_dag_views_list[4]]
        )

        expected_views = [
            v
            for v in self.x_shaped_dag_views_list
            # This view does not depend on other leaf view "table_4"
            if v.view_id != "table_4"
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_get_sub_dag_multiple_input_views(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        input_views = [
            self.x_shaped_dag_views_list[0],
            self.x_shaped_dag_views_list[1],
            self.x_shaped_dag_views_list[4],
        ]

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            input_views,
        )

        # Only should include this view
        expected_views = self.x_shaped_dag_views_list

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(input_views)

        expected_views = [
            self.x_shaped_dag_views_list[0],
            self.x_shaped_dag_views_list[1],
            self.x_shaped_dag_views_list[2],
            self.x_shaped_dag_views_list[4],
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_get_sub_dag_multiple_input_views2(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        input_views = [
            self.x_shaped_dag_views_list[2],
            self.x_shaped_dag_views_list[4],
        ]

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            input_views,
        )

        # Only should include this view
        expected_views = [
            self.x_shaped_dag_views_list[2],
            self.x_shaped_dag_views_list[3],
            self.x_shaped_dag_views_list[4],
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(input_views)

        expected_views = [
            self.x_shaped_dag_views_list[0],
            self.x_shaped_dag_views_list[1],
            self.x_shaped_dag_views_list[2],
            self.x_shaped_dag_views_list[4],
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_get_sub_dag_empty_input_views(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag([])

        self.assertCountEqual([], sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag([])

        self.assertCountEqual([], sub_dag.views)

    def test_get_sub_dag_single_node_input(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list[0:1])

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(
            self.x_shaped_dag_views_list[0:1],
        )

        expected_views = self.x_shaped_dag_views_list[0:1]

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(
            self.x_shaped_dag_views_list[0:1]
        )

        # Only should include this view
        expected_views = self.x_shaped_dag_views_list[0:1]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_get_sub_dag_input_views_not_all_in_input_dag(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list[0:2])

        with self.assertRaisesRegex(
            ValueError, "^Found input views not in source DAG:"
        ):
            # Add different input views to build source dag
            _ = all_views_dag_walker.get_descendants_sub_dag(
                self.x_shaped_dag_views_list[1:5],
            )

    def test_sub_dag_with_cycle(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        input_views = [
            self.diamond_shaped_dag_views_list[1],
            self.diamond_shaped_dag_views_list[4],
        ]

        # Get descendants sub-dag
        sub_dag = all_views_dag_walker.get_descendants_sub_dag(input_views)

        expected_views = [
            self.diamond_shaped_dag_views_list[1],
            self.diamond_shaped_dag_views_list[2],
            self.diamond_shaped_dag_views_list[3],
            self.diamond_shaped_dag_views_list[4],
            self.diamond_shaped_dag_views_list[5],
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

        # Get ancestors sub-dag
        sub_dag = all_views_dag_walker.get_ancestors_sub_dag(
            input_views,
        )

        expected_views = [
            self.diamond_shaped_dag_views_list[0],
            self.diamond_shaped_dag_views_list[1],
            self.diamond_shaped_dag_views_list[2],
            self.diamond_shaped_dag_views_list[4],
        ]

        self.assertCountEqual(expected_views, sub_dag.views)

    def test_sub_dag_include_ancestors(self) -> None:
        dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        sub_dag_walker = dag_walker.get_sub_dag(
            views=[self.x_shaped_dag_views_list[3]],
            include_ancestors=True,
            include_descendants=False,
        )

        self.assertCountEqual(
            [
                self.x_shaped_dag_views_list[0],
                self.x_shaped_dag_views_list[1],
                self.x_shaped_dag_views_list[2],
                self.x_shaped_dag_views_list[3],
            ],
            sub_dag_walker.views,
        )

    def test_sub_dag_include_descendants(self) -> None:
        dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        sub_dag_walker = dag_walker.get_sub_dag(
            views=[self.x_shaped_dag_views_list[2]],
            include_ancestors=False,
            include_descendants=True,
        )

        self.assertCountEqual(
            [
                self.x_shaped_dag_views_list[2],
                self.x_shaped_dag_views_list[3],
                self.x_shaped_dag_views_list[4],
            ],
            sub_dag_walker.views,
        )

    def test_dag_materialized_view_clobbers_other(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id=view_1.dataset_id, table_id=view_1.view_id
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        ).build()
        with self.assertRaisesRegex(
            ValueError,
            r"^Found materialized view address for view \[dataset_2.table_2\] that "
            r"matches the view address of another view \[dataset_1.table_1\].",
        ):
            _ = BigQueryViewDagWalker([view_1, view_2])

    def test_dag_two_views_same_materialized_address(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset", table_id="other_table"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset", table_id="other_table"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        ).build()
        with self.assertRaisesRegex(
            ValueError,
            r"Found materialized view address for view \[dataset_2.table_2\] "
            r"that matches materialized_address of another view: \[dataset_1.table_1\].",
        ):
            _ = BigQueryViewDagWalker([view_1, view_2])

    def test_dag_parents_materialized(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        ).build()
        view_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                SELECT * FROM `{project_id}.dataset_1.table_1`
                JOIN `{project_id}.dataset_2.table_2_materialized`
                USING (col)""",
        ).build()
        walker = BigQueryViewDagWalker([view_1, view_2, view_3])

        def process_simple(
            view: BigQueryView, parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> str:
            if view == view_3:
                # View 3 should have two parents
                self.assertEqual(
                    {view_1: view_1.view_id, view_2: view_2.view_id}, parent_results
                )

            return view.view_id

        result = walker.process_dag(process_simple, synchronous=self.synchronous)
        self.assertEqual(
            {view_1: view_1.view_id, view_2: view_2.view_id, view_3: view_3.view_id},
            result.view_results,
        )

    def test_dag_parents_materialized_non_default(self) -> None:
        self.maxDiff = None
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_1", table_id="other_table_1"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_2", table_id="other_table_2"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        ).build()
        view_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
                SELECT * FROM `{project_id}.dataset_1.table_1`
                JOIN `{project_id}.other_dataset_2.other_table_2`
                USING (col)""",
        ).build()
        walker = BigQueryViewDagWalker([view_1, view_2, view_3])

        def process_simple(
            view: BigQueryView, parent_results: Dict[BigQueryView, str]
        ) -> str:
            if view == view_3:
                # View 3 should have two parents
                self.assertEqual(
                    {view_1: view_1.view_id, view_2: view_2.view_id}, parent_results
                )

            return view.view_id

        result = walker.process_dag(process_simple, synchronous=self.synchronous)
        self.assertEqual(
            {view_1: view_1.view_id, view_2: view_2.view_id, view_3: view_3.view_id},
            result.view_results,
        )

        self.assertEqual(
            {view_1: 0, view_2: 0, view_3: 1},
            {
                view: metadata.graph_depth
                for view, metadata in result.view_processing_stats.items()
            },
        )

    def test_get_all_node_addresses_between_start_and_end_collections(self) -> None:
        # DAG Walker for views with the following structure
        #  source table 1    source table 2
        #             \       /
        #              1     2
        #               \   /
        #                 3
        #               /   \
        #              4     5
        all_views_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        source_table_1_address = BigQueryAddress(
            dataset_id="source_dataset", table_id="source_table"
        )
        source_table_2_address = BigQueryAddress(
            dataset_id="source_dataset", table_id="source_table_2"
        )
        view_1_address = self.x_shaped_dag_views_list[0].address
        view_2_address = self.x_shaped_dag_views_list[1].address
        view_3_address = self.x_shaped_dag_views_list[2].address
        view_4_address = self.x_shaped_dag_views_list[3].address
        view_5_address = self.x_shaped_dag_views_list[4].address

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            start_node_addresses={view_2_address},
            end_node_addresses={view_5_address},
        )

        self.assertEqual({view_2_address, view_3_address, view_5_address}, addresses)

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            start_node_addresses={view_2_address},
            end_node_addresses={view_2_address},
        )

        self.assertEqual({view_2_address}, addresses)

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses={source_table_1_address},
            start_node_addresses={view_2_address},
            end_node_addresses={view_3_address},
        )

        self.assertEqual({view_1_address, view_2_address, view_3_address}, addresses)

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            # View 4 is not a descendant of any of the end addresses, but it should
            # still be included.
            start_node_addresses={view_4_address, view_5_address},
            end_node_addresses={view_5_address},
        )

        self.assertEqual({view_4_address, view_5_address}, addresses)

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses={source_table_1_address, source_table_2_address},
            start_node_addresses=set(),
            end_node_addresses={view_4_address, view_5_address},
        )

        self.assertEqual(
            {
                view_1_address,
                view_2_address,
                view_3_address,
                view_4_address,
                view_5_address,
            },
            addresses,
        )

        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            start_node_addresses={view_4_address},
            # End address that is upstream of all start addresses should still be
            # included.
            end_node_addresses={view_1_address},
        )

        self.assertEqual(
            {
                view_1_address,
                view_4_address,
            },
            addresses,
        )

        # If only source table addresses are specified, everything view that is a direct
        # child of a source table should be loaded.
        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses={source_table_1_address},
            start_node_addresses=set(),
            end_node_addresses=set(),
        )

        self.assertEqual({view_1_address}, addresses)

        # Empty case
        addresses = all_views_dag_walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            start_node_addresses=set(),
            end_node_addresses=set(),
        )

        self.assertEqual(set(), addresses)

    def test_dag_perf_config(self) -> None:
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            if view.view_id == "table_6":
                time.sleep(0.1)
            else:
                time.sleep(0.01)

        with self.assertRaisesRegex(
            ValueError,
            r"Processing for \[BigQueryAddress\(dataset_id='dataset_6', "
            r"table_id='table_6'\)\] took \[0.1[0-9]?\] seconds. Expected node to "
            r"process in less than \[0.05\] seconds.",
        ):
            walker.process_dag(
                process_simple,
                synchronous=self.synchronous,
                perf_config=ProcessDagPerfConfig(
                    node_max_processing_time_seconds=0.05,
                    node_allowed_process_time_overrides={},
                ),
            )

        # Now add an exemption for the slow view and see it processes
        result = walker.process_dag(
            process_simple,
            synchronous=self.synchronous,
            perf_config=ProcessDagPerfConfig(
                node_max_processing_time_seconds=0.05,
                node_allowed_process_time_overrides={
                    BigQueryAddress(dataset_id="dataset_6", table_id="table_6"): 0.2
                },
            ),
        )

        self.assertEqual(set(walker.views), set(result.view_results))

        # Now process with a NULL perf config to see that no perf thresholds are
        # enforced.
        result = walker.process_dag(
            process_simple, synchronous=self.synchronous, perf_config=None
        )

        self.assertEqual(set(walker.views), set(result.view_results))

    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=True))
    def test_dag_perf_config_in_gcp_no_crash(self) -> None:
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, None]
        ) -> None:
            if view.view_id == "table_6":
                time.sleep(0.1)
            else:
                time.sleep(0.01)

        with patch("logging.Logger.error") as mock_logger:
            result = walker.process_dag(
                process_simple,
                synchronous=self.synchronous,
                perf_config=ProcessDagPerfConfig(
                    node_max_processing_time_seconds=0.05,
                    node_allowed_process_time_overrides={},
                ),
            )
            # Don't crash in GCP but do still emit an error log
            logged_message = mock_logger.call_args[0][0]
            # Allow small variations in timing
            self.assertRegex(
                logged_message,
                r"\[BigQueryViewDagWalker Node Failure\] Processing for \[BigQueryAddress\(dataset_id='dataset_6', table_id='table_6'\)\] took \[0\.1(\d+)?\] seconds. Expected node to process in less than \[0\.05\] seconds.",
            )
        self.assertEqual(set(walker.views), set(result.view_results))

        with patch("logging.Logger.error") as mock_logger:
            # Now add an exemption for the slow view and see it processes
            result = walker.process_dag(
                process_simple,
                synchronous=self.synchronous,
                perf_config=ProcessDagPerfConfig(
                    node_max_processing_time_seconds=0.05,
                    node_allowed_process_time_overrides={
                        BigQueryAddress(dataset_id="dataset_6", table_id="table_6"): 0.2
                    },
                ),
            )
            mock_logger.assert_not_called()

        self.assertEqual(set(walker.views), set(result.view_results))

    def test_log_processing_stats(self) -> None:
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> BigQueryAddress:
            time.sleep(MOCK_VIEW_PROCESS_TIME_SECONDS)
            return view.address

        result = walker.process_dag(process_simple, synchronous=self.synchronous)

        # Logging stats shouldn't crash
        result.log_processing_stats(n_slowest=3)

    def assertIsValidEmptyParentsView(self, node: BigQueryViewDagNode) -> None:
        """Fails the test if a view that has no parents is an expected view with no
        parents. Failures could be indicative of poorly formed view queries.
        """
        known_empty_parent_view_addresss = {
            # These views unnest data from a static list
            BigQueryAddress(
                dataset_id="census_managed_views",
                table_id="charge_class_severity_ranks",
            ),
            BigQueryAddress(
                dataset_id="reference_views",
                table_id="state_info",
            ),
            BigQueryAddress(
                dataset_id="sessions",
                table_id="admission_start_reason_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="sessions",
                table_id="custody_level_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="sessions",
                table_id="release_termination_reason_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="sessions", table_id="supervision_level_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="sessions", table_id="assessment_level_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="sessions", table_id="compartment_level_1_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="sessions", table_id="compartment_level_2_dedup_priority"
            ),
            BigQueryAddress(
                dataset_id="sessions",
                table_id="state_staff_role_subtype_dedup_priority",
            ),
            BigQueryAddress(
                dataset_id="sessions", table_id="assessment_lsir_scoring_key"
            ),
            BigQueryAddress(
                dataset_id="analyst_data",
                table_id="us_pa_raw_treatment_classification_codes",
            ),
            BigQueryAddress(
                dataset_id="reference_views",
                table_id="task_to_completion_event",
            ),
            BigQueryAddress(
                dataset_id="reference_views",
                table_id="completion_event_type_metadata",
            ),
            # Generate data using pure date functions
            BigQueryAddress(
                dataset_id="reference_views", table_id="covid_report_weeks"
            ),
            BigQueryAddress(
                dataset_id="population_projection_data", table_id="simulation_run_dates"
            ),
            BigQueryAddress(dataset_id="sessions", table_id="cohort_month_index"),
            BigQueryAddress(
                dataset_id="aggregated_metrics", table_id="metric_time_periods"
            ),
            BigQueryAddress(
                dataset_id="reference_views", table_id="workflows_opportunity_configs"
            ),
        }
        if node.view.address in known_empty_parent_view_addresss:
            return

        if "FROM EXTERNAL_QUERY" in node.view.view_query:
            return

        self.fail(node.view.address)

    @staticmethod
    def assertIsValidSourceDataTable(
        child_view_address: BigQueryAddress, source_table_address: BigQueryAddress
    ) -> None:
        if source_table_address.dataset_id in get_source_table_datasets(
            metadata.project_id()
        ):
            return

        raise ValueError(
            f"Found parent [{source_table_address.to_str()}] of BQ graph root node "
            f"[{child_view_address.to_str()}] that is not in an expected raw inputs "
            f"dataset."
        )

    @staticmethod
    def assertIsValidRawTableViewDependency(
        child_view_key: BigQueryAddress, raw_data_view_key: BigQueryAddress
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

    def test_edges(self) -> None:
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        assert walker.get_number_of_edges() == 6

        walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)
        assert walker.get_number_of_edges() == 4

    def test_distinct_paths(self) -> None:
        # in a DAG like
        #  1     2       11
        #   \   /
        #     3
        #   /   \
        #  4     5
        #  |\   / |\
        #  |  6   | \
        #  |/   \ |  \
        #  7      8   \
        #  |        \  |
        #  9         10
        #
        # we want to make sure we are counting 11 as a "single" path to it (from itself)
        views_for_path = [
            *DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST.copy(),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_7",
                view_id="table_7",
                description="table_7 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_4.table_4`
            JOIN `{project_id}.dataset_6.table_6`
            USING (col)""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_8",
                view_id="table_8",
                description="table_8 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_5.table_5`
            JOIN `{project_id}.dataset_6.table_6`
            USING (col)""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_9",
                view_id="table_9",
                description="table_9 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_7.table_7`""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_10",
                view_id="table_10",
                description="table_10 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_5.table_5`
            JOIN `{project_id}.dataset_8.table_8`
            USING (col)""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_11",
                view_id="table_11",
                description="table_11 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_not_in_graph.table`""",
            ),
        ]

        walker = BigQueryViewDagWalker([v.build() for v in views_for_path])
        assert (
            walker.process_dag(
                lambda _, __: None, synchronous=self.synchronous
            ).get_distinct_paths_to_leaf_nodes()
            == 15
        )


class SynchronousBigQueryViewDagWalkerTest(TestBigQueryViewDagWalkerBase):
    """DAG walker tests run with synchronous processing"""

    __test__ = True

    @property
    def synchronous(self) -> bool:
        return True

    def test_ancestors_dfs_tree_str(self) -> None:
        view_builder_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_builder_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_builder_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        )
        view_builder_4 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_builder_5 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_5",
            view_id="table_5",
            description="table_5 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        all_view_builders = [
            view_builder_1,
            view_builder_2,
            view_builder_3,
            view_builder_4,
            view_builder_5,
        ]
        dag_walker = BigQueryViewDagWalker([b.build() for b in all_view_builders])

        # Top level view
        dfs_tree = dag_walker.ancestors_dfs_tree_str(view_builder_5.build())
        expected_tree = """dataset_5.table_5
|--dataset_3.table_3
|----dataset_1.table_1
|------source_dataset.source_table
|----dataset_2.table_2
|------source_dataset.source_table_2
"""
        self.assertEqual(expected_tree, dfs_tree)

        # Middle of tree
        dfs_tree = dag_walker.ancestors_dfs_tree_str(view_builder_3.build())
        expected_tree = """dataset_3.table_3
|--dataset_1.table_1
|----source_dataset.source_table
|--dataset_2.table_2
|----source_dataset.source_table_2
"""
        self.assertEqual(expected_tree, dfs_tree)

        # Custom formatted
        def _custom_formatter(address: BigQueryAddress, is_truncated: bool) -> str:
            formatted = f"custom_formatted_{address.dataset_id}_{address.table_id}"
            if is_truncated:
                formatted += " (....)"
            return formatted

        dfs_tree = dag_walker.ancestors_dfs_tree_str(
            view_builder_3.build(),
            custom_node_formatter=_custom_formatter,
        )

        expected_tree = """custom_formatted_dataset_3_table_3
|--custom_formatted_dataset_1_table_1
|----custom_formatted_source_dataset_source_table
|--custom_formatted_dataset_2_table_2
|----custom_formatted_source_dataset_source_table_2
"""
        self.assertEqual(expected_tree, dfs_tree)

        # Skip datasets
        dag_walker = BigQueryViewDagWalker([b.build() for b in all_view_builders])
        dfs_tree = dag_walker.ancestors_dfs_tree_str(
            view_builder_3.build(), datasets_to_skip={"dataset_1"}
        )
        expected_tree = """dataset_3.table_3
|----source_dataset.source_table
|--dataset_2.table_2
|----source_dataset.source_table_2
"""
        self.assertEqual(expected_tree, dfs_tree)

    def test_ancestors_dfs_tree_str_complex(self) -> None:
        views = [
            *self.diamond_shaped_dag_views_list,
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_7",
                view_id="table_7",
                description="table_7 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_6.table_6`",
            ).build(),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_8",
                view_id="table_8",
                description="table_8 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_6.table_6`",
            ).build(),
        ]
        dag_walker = BigQueryViewDagWalker(views)

        # Top level view
        ancestors_tree = dag_walker.ancestors_dfs_tree_str(view=views[-1])

        expected = """dataset_8.table_8
|--dataset_6.table_6
|----dataset_4.table_4
|------dataset_3.table_3
|--------dataset_1.table_1
|----------source_dataset.source_table
|--------dataset_2.table_2
|----------source_dataset.source_table_2
|----dataset_5.table_5
|------dataset_3.table_3 (...)
"""
        self.assertEqual(expected, ancestors_tree)

        # Custom formatted
        def _custom_formatter(address: BigQueryAddress, is_truncated: bool) -> str:
            formatted = f"custom_formatted_{address.dataset_id}_{address.table_id}"
            if is_truncated:
                formatted += " (....)"
            return formatted + " <br/>"

        ancestors_tree = dag_walker.ancestors_dfs_tree_str(
            view=views[-1], custom_node_formatter=_custom_formatter
        )
        expected = """custom_formatted_dataset_8_table_8 <br/>
|--custom_formatted_dataset_6_table_6 <br/>
|----custom_formatted_dataset_4_table_4 <br/>
|------custom_formatted_dataset_3_table_3 <br/>
|--------custom_formatted_dataset_1_table_1 <br/>
|----------custom_formatted_source_dataset_source_table <br/>
|--------custom_formatted_dataset_2_table_2 <br/>
|----------custom_formatted_source_dataset_source_table_2 <br/>
|----custom_formatted_dataset_5_table_5 <br/>
|------custom_formatted_dataset_3_table_3 (....) <br/>
"""
        self.assertEqual(expected, ancestors_tree)

    def test_descendants_dfs_tree_str(self) -> None:
        view_builder_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )
        view_builder_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
        )
        view_builder_3 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_3",
            view_id="table_3",
            description="table_3 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
        )
        view_builder_4 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_4",
            view_id="table_4",
            description="table_4 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_builder_5 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_5",
            view_id="table_5",
            description="table_5 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
        )
        view_builder_6 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_6",
            view_id="table_6",
            description="table_6 description",
            view_query_template="""
            SELECT * FROM `{project_id}.dataset_5.table_5`""",
        )
        all_builders = [
            view_builder_1,
            view_builder_2,
            view_builder_3,
            view_builder_4,
            view_builder_5,
            view_builder_6,
        ]
        all_views = [b.build() for b in all_builders]
        dag_walker = BigQueryViewDagWalker(all_views)

        # Top level view
        descendant_tree = dag_walker.descendants_dfs_tree_str(
            view=view_builder_2.build()
        )
        expected_tree = """dataset_2.table_2
|--dataset_3.table_3
|----dataset_4.table_4
|----dataset_5.table_5
|------dataset_6.table_6
"""
        self.assertEqual(expected_tree, descendant_tree)

        # Descendants from middle of tree
        descendant_tree = dag_walker.descendants_dfs_tree_str(
            view=view_builder_3.build()
        )
        expected_tree = """dataset_3.table_3
|--dataset_4.table_4
|--dataset_5.table_5
|----dataset_6.table_6
"""
        self.assertEqual(expected_tree, descendant_tree)

    def test_descendants_dfs_tree_str_complex(self) -> None:
        views = [
            *self.diamond_shaped_dag_views_list,
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_7",
                view_id="table_7",
                description="table_7 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_6.table_6`",
            ).build(),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_8",
                view_id="table_8",
                description="table_8 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_6.table_6`",
            ).build(),
        ]
        dag_walker = BigQueryViewDagWalker(views)

        # Top level view
        descendant_tree = dag_walker.descendants_dfs_tree_str(view=views[0])
        expected = """dataset_1.table_1
|--dataset_3.table_3
|----dataset_4.table_4
|------dataset_6.table_6
|--------dataset_7.table_7
|--------dataset_8.table_8
|----dataset_5.table_5
|------dataset_6.table_6 (...)
"""
        self.assertEqual(expected, descendant_tree)

        # Custom formatted
        def _custom_formatter(address: BigQueryAddress, is_truncated: bool) -> str:
            formatted = f"custom_formatted_{address.dataset_id}_{address.table_id}"
            if is_truncated:
                formatted += " (....)"
            return formatted + " <br/>"

        descendant_tree = dag_walker.descendants_dfs_tree_str(
            view=views[0], custom_node_formatter=_custom_formatter
        )
        expected = """custom_formatted_dataset_1_table_1 <br/>
|--custom_formatted_dataset_3_table_3 <br/>
|----custom_formatted_dataset_4_table_4 <br/>
|------custom_formatted_dataset_6_table_6 <br/>
|--------custom_formatted_dataset_7_table_7 <br/>
|--------custom_formatted_dataset_8_table_8 <br/>
|----custom_formatted_dataset_5_table_5 <br/>
|------custom_formatted_dataset_6_table_6 (....) <br/>
"""
        self.assertEqual(expected, descendant_tree)

    def test_descendants_dfs_tree_str_many_paths_to_leaf(self) -> None:
        r"""
        Tests structure like:
               1
            /  |  \
           2   3   4
            \  |  /
               5
        """
        view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_2",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_3",
                view_id="table_3",
                description="table_3 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_4",
                view_id="table_4",
                description="table_4 description",
                view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_5",
                view_id="table_5",
                description="table_5 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_2.table_2`
                    UNION ALL
                    SELECT * FROM `{project_id}.dataset_3.table_3`
                    UNION ALL
                    SELECT * FROM `{project_id}.dataset_4.table_4`
                    """,
            ),
        ]
        views = [vb.build() for vb in view_builders]
        dag_walker = BigQueryViewDagWalker(views)

        # Top level view
        descendant_tree = dag_walker.descendants_dfs_tree_str(view=views[0])
        expected = """dataset_1.table_1
|--dataset_2.table_2
|----dataset_5.table_5
|--dataset_3.table_3
|----dataset_5.table_5
|--dataset_4.table_4
|----dataset_5.table_5
"""
        self.assertEqual(expected, descendant_tree)


class AsynchronousBigQueryViewDagWalkerTest(TestBigQueryViewDagWalkerBase):
    """
    Run TestBigQueryViewDagWalkerBase unit tests for the
    aysnchronous case, as well as all non-polymorphic unit tests
    """

    __test__ = True

    @property
    def synchronous(self) -> bool:
        return False

    # Non-polymorphic tests
    def test_dag_process_time(self) -> None:
        num_views = len(self.all_views)
        walker = BigQueryViewDagWalker(self.all_views)

        serial_processing_time_seconds = num_views * MOCK_VIEW_PROCESS_TIME_SECONDS
        serial_processing_time = datetime.timedelta(
            seconds=serial_processing_time_seconds
        )

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> BigQueryAddress:
            time.sleep(MOCK_VIEW_PROCESS_TIME_SECONDS)
            return view.address

        start = datetime.datetime.now()
        result = walker.process_dag(process_simple, synchronous=self.synchronous)
        end = datetime.datetime.now()

        self.assertEqual(num_views, len(result.view_results))

        processing_time = end - start

        # We expect to see significant speedup over the processing time if we ran the
        # process function for each view in series.
        self.assertLess(processing_time * 3, serial_processing_time)

    def test_dag_init(self) -> None:
        walker = BigQueryViewDagWalker(self.all_views)

        self.assertEqual(len(self.all_views), len(walker.nodes_by_address))

        for key, node in walker.nodes_by_address.items():
            if not node.parent_tables:
                self.assertIsValidEmptyParentsView(node)

            if node.is_root:
                # Root views should only have source data tables as parents
                self.assertEqual(set(), node.parent_node_addresses)
                for parent_address in node.parent_tables:
                    self.assertIsValidSourceDataTable(key, parent_address)

    def test_related_ancestor_addresses(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        all_views_dag_walker.populate_ancestor_sub_dags()
        view_1, view_2, view_3, *_ = self.diamond_shaped_dag_views_list
        assert all_views_dag_walker.related_ancestor_addresses(
            address=view_3.address
        ) == set(
            [
                view_1.address,
                view_2.address,
                view_3.address,
                BigQueryAddress(dataset_id="source_dataset", table_id="source_table"),
                BigQueryAddress(dataset_id="source_dataset", table_id="source_table_2"),
            ]
        )

    def test_dag_with_cycle_at_root(self) -> None:
        view_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_2.table_2`",
        ).build()
        view_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_2",
            view_id="table_2",
            description="table_2 description",
            view_query_template="SELECT * FROM `{project_id}.dataset_1.table_1`",
        ).build()

        with self.assertRaisesRegex(
            ValueError, "^No roots detected. Input views contain a cycle.$"
        ):
            _ = BigQueryViewDagWalker([view_1, view_2])

    def test_union_dags(self) -> None:
        x_shaped_dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # This DAG is a superset of the X-shaped DAG
        diamond_shaped_dag_walker = BigQueryViewDagWalker(
            self.diamond_shaped_dag_views_list
        )
        unioned_dag = BigQueryViewDagWalker.union_dags(
            x_shaped_dag_walker, diamond_shaped_dag_walker
        )

        self.assertCountEqual(self.diamond_shaped_dag_views_list, unioned_dag.views)

    def test_union_dags_empty(self) -> None:
        unioned_dag = BigQueryViewDagWalker.union_dags(
            BigQueryViewDagWalker([]), BigQueryViewDagWalker([])
        )

        self.assertCountEqual([], unioned_dag.views)

    def test_union_dags_no_dags(self) -> None:
        """Unioning together a list of DAGs with no DAGs in it returns an empty DAG."""
        dags: List[BigQueryViewDagWalker] = []
        unioned_dag = BigQueryViewDagWalker.union_dags(*dags)

        self.assertCountEqual([], unioned_dag.views)

    def test_union_dags_more_than_two_dags(self) -> None:
        dag_1 = BigQueryViewDagWalker(self.x_shaped_dag_views_list)

        # This DAG is a superset of the X-shaped DAG
        dag_2 = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        dag_3_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="another_dataset",
                view_id="another_table",
                description="another_table description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
        ]

        dag_3_views = [b.build() for b in dag_3_builders]
        dag_3 = BigQueryViewDagWalker([b.build() for b in dag_3_builders])

        unioned_dag = BigQueryViewDagWalker.union_dags(dag_1, dag_2, dag_3)

        self.assertCountEqual(
            self.diamond_shaped_dag_views_list + dag_3_views, unioned_dag.views
        )

    def test_union_dags_same_view_different_object(self) -> None:
        view_builder_1 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_1", table_id="other_table_1"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )

        dag_1 = BigQueryViewDagWalker([view_builder_1.build()])

        view_builder_2 = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="table_1",
            description="table_1 description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="other_dataset_1", table_id="other_table_1"
            ),
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        )

        dag_2 = BigQueryViewDagWalker([view_builder_2.build()])
        unioned_dag = BigQueryViewDagWalker.union_dags(
            dag_1,
            dag_2,
        )

        self.assertCountEqual([view_builder_1.build()], unioned_dag.views)

    def test_referenced_source_tables(self) -> None:
        view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="""SELECT * FROM `{project_id}.some_dataset.some_table`
            LEFT OUTER JOIN `{project_id}.some_dataset.other_table`
            USING (some_col);
            """,
        ).build()
        child_view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="child_view",
            description="my view description",
            view_query_template="""SELECT * FROM `{project_id}.my_dataset.my_view_id`""",
        ).build()
        walker = BigQueryViewDagWalker([view, child_view])
        self.assertEqual(
            walker.get_referenced_source_tables(),
            {
                BigQueryAddress(dataset_id="some_dataset", table_id="some_table"),
                BigQueryAddress(dataset_id="some_dataset", table_id="other_table"),
            },
        )


class TestBigQueryViewDagNode(unittest.TestCase):
    """Tests for BigQueryViewDagNode"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_parse_simple_view(self) -> None:
        view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.some_table`",
        ).build()
        node = BigQueryViewDagNode(view)
        self.assertIsNone(view.materialized_address)
        self.assertEqual(node.is_root, False)
        self.assertEqual(
            node.view.address,
            BigQueryAddress(dataset_id="my_dataset", table_id="my_view_id"),
        )
        self.assertEqual(
            node.parent_tables,
            {BigQueryAddress(dataset_id="some_dataset", table_id="some_table")},
        )
        self.assertEqual(node.child_node_addresses, set())

        node.is_root = True
        child_address = BigQueryAddress(
            dataset_id="other_dataset", table_id="other_table"
        )
        node.add_child_node_address(child_address)

        self.assertEqual(node.is_root, True)
        self.assertEqual(node.child_node_addresses, {child_address})

    def test_parse_view_materialized_parent(self) -> None:
        view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.some_table_materialized`",
        ).build()
        parent_view = SimpleBigQueryViewBuilder(
            dataset_id="some_dataset",
            view_id="some_table",
            description="my parent view description",
            view_query_template="SELECT * FROM UNNEST([])",
            should_materialize=True,
        ).build()
        dag_walker = BigQueryViewDagWalker([view, parent_view])
        node = dag_walker.node_for_view(view)
        self.assertEqual(
            node.parent_node_addresses,
            {BigQueryAddress(dataset_id="some_dataset", table_id="some_table")},
        )

    def test_parse_view_multiple_parents(self) -> None:
        view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="""SELECT * FROM `{project_id}.some_dataset.some_table`
            LEFT OUTER JOIN `{project_id}.some_dataset.other_table`
            USING (some_col);
            """,
        ).build()
        node = BigQueryViewDagNode(view)
        self.assertEqual(
            node.parent_tables,
            {
                BigQueryAddress(dataset_id="some_dataset", table_id="some_table"),
                BigQueryAddress(dataset_id="some_dataset", table_id="other_table"),
            },
        )
