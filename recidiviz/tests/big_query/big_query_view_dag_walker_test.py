# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
from typing import Any, Dict, List, Set
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagNode,
    BigQueryViewDagWalker,
    BigQueryViewDagWalkerProcessingError,
    BigQueryViewDagWalkerProcessingFailureMode,
    ProcessDagPerfConfig,
    TraversalDirection,
)
from recidiviz.datasets.static_data.views.dataset_config import (
    STATIC_REFERENCE_DATA_VIEWS_DATASET_ID,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.tests.utils.test_utils import assert_group_contains_regex
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_views import deployed_view_builders

LATEST_VIEW_DATASET_REGEX = re.compile(r"(us_[a-z]{2})_raw_data_up_to_date_views")
MOCK_VIEW_PROCESS_TIME_SECONDS = 0.02


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
            BigQueryAddress(
                dataset_id="reference_views", table_id="compliance_tasks_configs"
            ),
            BigQueryAddress(
                dataset_id="task_eligibility_completion_events_us_nc",
                table_id="granted_supervision_sentence_reduction",
            ),
        }
        if node.view.address in known_empty_parent_view_addresss:
            return

        if node.view.dataset_id in {
            # These view queries are all generated via static data from a CSV and will
            # never have parent tables
            STATIC_REFERENCE_DATA_VIEWS_DATASET_ID
        }:
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

    def test_failure_mode(self) -> None:
        # in a DAG like
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #  |     |
        #  6     7
        #   \   / \
        #     8     9
        views_for_path = [
            *X_SHAPED_DAG_VIEW_BUILDERS_LIST.copy(),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_6",
                view_id="table_6",
                description="table_6 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_4.table_4`""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_7",
                view_id="table_7",
                description="table_7 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_5.table_5`""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_8",
                view_id="table_8",
                description="table_8 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_7.table_7`
            JOIN `{project_id}.dataset_8.table_8`
            USING (col)""",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_9",
                view_id="table_9",
                description="table_9 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_7.table_7`""",
            ),
        ]

        walker = BigQueryViewDagWalker([v.build() for v in views_for_path])

        processed = set()

        def _process_node_4_fails(
            view: BigQueryView, _parent_results: dict[BigQueryView, None]
        ) -> None:
            nonlocal processed
            processed.add(view.view_id)
            if view.view_id == "table_4":
                raise ValueError("Failed to do 4")

        with self.assertRaisesRegex(
            BigQueryViewDagWalkerProcessingError,
            r"Error processing \[dataset_4\.table_4\]",
        ):
            walker.process_dag(
                _process_node_4_fails,
                # set to synchronous to make node level processing order deterministic --
                # asynchronous=True will return the set of all nodes that are processed
                # (all nodes at any deque time) which means we may process level 4 nodes
                # whose parents are all complete before we are all finished with level 3
                # nodes
                synchronous=True,
                failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
            )

        # because we don't process nodes of the same level deterministically, sometimes
        # we process 5 before we process 4 -- both are technically correct so we
        # subtract 5 from processed to account for this
        assert processed - {"table_5"} == {
            "table_1",
            "table_2",
            "table_3",
            "table_4",
            # "table_5",  will process, only sometimes
            # "table_6",  will wont process as is on the next level from 4
            # "table_7",  will wont process as is on the next level from 4
            # "table_8",  will wont process as is on the next level from 4
            # "table_9",  will wont process as is on the next level from 4
        }

        processed = set()

        with assert_group_contains_regex(
            r"DAG processing failed with the following errors\:",
            [
                (
                    BigQueryViewDagWalkerProcessingError,
                    r"Error processing \[dataset_4\.table_4\]",
                )
            ],
        ):
            walker.process_dag(
                _process_node_4_fails,
                synchronous=self.synchronous,
                failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
            )

        # we process 5, 7  and 9 after 4 fails, but not 6 or 8 because they are descendants
        # of 4
        assert processed == {
            "table_1",
            "table_2",
            "table_3",
            "table_4",
            "table_5",
            # "table_6",  we don't process 6 because it is a descendant of 4
            "table_7",
            # "table_8",  we don't process 8 because it is a descendant of 6
            "table_9",
        }

        def _process_node_4_and_9_fails(
            view: BigQueryView, _parent_results: dict[BigQueryView, None]
        ) -> None:
            nonlocal processed
            processed.add(view.view_id)
            if view.view_id in ("table_4", "table_9"):
                raise ValueError("Failed to process")

        processed = set()

        with assert_group_contains_regex(
            r"DAG processing failed with the following errors\:",
            [
                (
                    BigQueryViewDagWalkerProcessingError,
                    r"Error processing \[dataset_4\.table_4\]",
                ),
                (
                    BigQueryViewDagWalkerProcessingError,
                    r"Error processing \[dataset_9\.table_9\]",
                ),
            ],
        ):
            walker.process_dag(
                _process_node_4_and_9_fails,
                synchronous=self.synchronous,
                failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
            )

        # we process 5, 7 and 9 after 4 fails, but not 6 because it is a descendant of
        # of 4
        assert processed == {
            "table_1",
            "table_2",
            "table_3",
            "table_4",
            "table_5",
            # "table_6",  we don't process 6 because it is a descendant of 4
            "table_7",
            # "table_8",  we don't process 8 because it is a descendant of 6
            "table_9",
        }


class SynchronousBigQueryViewDagWalkerTest(TestBigQueryViewDagWalkerBase):
    """DAG walker tests run with synchronous processing"""

    __test__ = True

    def setUp(self) -> None:
        if hasattr(super(), "setUp"):
            super().setUp()

        # Create test views for get_edges() tests with proper dependencies:
        # view_one is a root view (no dependencies)
        self.view_one = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="view_one",
            description="First test view - root view",
            view_query_template="SELECT 1 as col",
        ).build()

        # view_two depends on view_one
        self.view_two = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="view_two",
            description="Second test view that depends on view_one",
            view_query_template="SELECT * FROM `{project_id}.view_dataset.view_one`",
        ).build()

        # view_three also depends on view_one
        self.view_three = SimpleBigQueryViewBuilder(
            dataset_id="view_dataset",
            view_id="view_three",
            description="Third test view that depends on view_one",
            view_query_template="SELECT * FROM `{project_id}.view_dataset.view_one`",
        ).build()

    @property
    def synchronous(self) -> bool:
        return True

    def test_get_edges(self) -> None:
        """Test basic get_edges functionality."""
        dag_walker = BigQueryViewDagWalker(
            [self.view_one, self.view_two, self.view_three]
        )
        edges = dag_walker.get_edges()

        # Convert to set of address tuples for easier comparison
        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        expected_edges = {
            (self.view_one.address, self.view_two.address),  # view_one -> view_two
            (self.view_one.address, self.view_three.address),  # view_one -> view_three
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 2)

    def test_get_edges_with_multiple_parents(self) -> None:
        """Test get_edges with views that have multiple parent dependencies."""
        # Create a view that depends on both view_one and view_two
        view_with_multiple_parents = SimpleBigQueryViewBuilder(
            dataset_id="multiple_parents_dataset",
            view_id="view_with_multiple_parents",
            description="A view with multiple parent dependencies",
            view_query_template="""
            SELECT * FROM `{project_id}.view_dataset.view_one`
            UNION ALL
            SELECT * FROM `{project_id}.view_dataset.view_two`
            """,
        ).build()

        dag_walker = BigQueryViewDagWalker(
            [self.view_one, self.view_two, view_with_multiple_parents]
        )
        edges = dag_walker.get_edges()

        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        expected_edges = {
            (
                self.view_one.address,
                self.view_two.address,
            ),  # view_one -> view_two (from our test setup)
            (
                self.view_one.address,
                view_with_multiple_parents.address,
            ),  # view_one -> view_with_multiple_parents
            (
                self.view_two.address,
                view_with_multiple_parents.address,
            ),  # view_two -> view_with_multiple_parents
        }

        self.assertEqual(edge_addresses, expected_edges)

    def test_get_edges_complex_dag(self) -> None:
        """Test get_edges with a complex DAG structure."""
        dag_walker = BigQueryViewDagWalker(self.all_views)
        edges = dag_walker.get_edges()

        # Verify we have a substantial number of edges (complex DAG should have many)
        self.assertGreater(len(edges), 100)

        # All edges should be between valid views in the DAG
        all_addresses = {view.address for view in self.all_views}
        for parent, child in edges:
            self.assertIn(parent.address, all_addresses)
            self.assertIn(child.address, all_addresses)

    def test_get_edges_empty_dag(self) -> None:
        """Test get_edges with a DAG that has no dependencies."""
        isolated_view = SimpleBigQueryViewBuilder(
            dataset_id="isolated_dataset",
            view_id="isolated_view",
            description="A view with no dependencies",
            view_query_template="SELECT 1 as col1, 'isolated' as col2",
        ).build()

        dag_walker = BigQueryViewDagWalker([isolated_view])
        edges = dag_walker.get_edges()

        # Should have no edges since there are no dependencies
        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

    def test_get_edges_single_view_no_dependencies(self) -> None:
        """Test get_edges with a single view that has no dependencies."""
        dag_walker = BigQueryViewDagWalker([self.view_one])
        edges = dag_walker.get_edges()

        # view_one has no dependencies in this isolated case
        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

    def test_get_edges_x_shaped_dag(self) -> None:
        r"""Test get_edges with X-shaped DAG structure:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        """
        dag_walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)
        edges = dag_walker.get_edges()

        # Convert to set of address tuples for easier comparison
        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        # Expected edges for X-shaped DAG:
        # table_1 -> table_3, table_2 -> table_3, table_3 -> table_4, table_3 -> table_5
        expected_edges = {
            (
                self.x_shaped_dag_views_list[0].address,
                self.x_shaped_dag_views_list[2].address,
            ),  # table_1 -> table_3
            (
                self.x_shaped_dag_views_list[1].address,
                self.x_shaped_dag_views_list[2].address,
            ),  # table_2 -> table_3
            (
                self.x_shaped_dag_views_list[2].address,
                self.x_shaped_dag_views_list[3].address,
            ),  # table_3 -> table_4
            (
                self.x_shaped_dag_views_list[2].address,
                self.x_shaped_dag_views_list[4].address,
            ),  # table_3 -> table_5
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 4)

    def test_get_edges_diamond_shaped_dag(self) -> None:
        r"""Test get_edges with diamond-shaped DAG structure:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #   \   /
        #     6
        """
        dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        edges = dag_walker.get_edges()

        # Convert to set of address tuples for easier comparison
        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        # Expected edges for diamond-shaped DAG (includes all X-shaped edges plus diamond bottom):
        expected_edges = {
            (
                self.diamond_shaped_dag_views_list[0].address,
                self.diamond_shaped_dag_views_list[2].address,
            ),  # table_1 -> table_3
            (
                self.diamond_shaped_dag_views_list[1].address,
                self.diamond_shaped_dag_views_list[2].address,
            ),  # table_2 -> table_3
            (
                self.diamond_shaped_dag_views_list[2].address,
                self.diamond_shaped_dag_views_list[3].address,
            ),  # table_3 -> table_4
            (
                self.diamond_shaped_dag_views_list[2].address,
                self.diamond_shaped_dag_views_list[4].address,
            ),  # table_3 -> table_5
            (
                self.diamond_shaped_dag_views_list[3].address,
                self.diamond_shaped_dag_views_list[5].address,
            ),  # table_4 -> table_6
            (
                self.diamond_shaped_dag_views_list[4].address,
                self.diamond_shaped_dag_views_list[5].address,
            ),  # table_5 -> table_6
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 6)

        # Verify the diamond structure specifically - table_6 should have exactly 2 parents
        table_6_parents = [
            parent
            for parent, child in edges
            if child.address == self.diamond_shaped_dag_views_list[5].address
        ]
        self.assertEqual(len(table_6_parents), 2)
        parent_addresses = {parent.address for parent in table_6_parents}
        expected_parent_addresses = {
            self.diamond_shaped_dag_views_list[3].address,  # table_4
            self.diamond_shaped_dag_views_list[4].address,  # table_5
        }
        self.assertEqual(parent_addresses, expected_parent_addresses)

    def test_get_edges_materialized_views(self) -> None:
        """Test get_edges with materialized views and their relationships."""
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
            # References materialized view via the _materialized suffix
            view_query_template="""
                SELECT * FROM `{project_id}.dataset_1.table_1`
                JOIN `{project_id}.dataset_2.table_2_materialized`
                USING (col)""",
        ).build()

        dag_walker = BigQueryViewDagWalker([view_1, view_2, view_3])
        edges = dag_walker.get_edges()

        # Convert to set of address tuples for easier comparison
        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        expected_edges = {
            (view_1.address, view_3.address),  # view_1 -> view_3
            (
                view_2.address,
                view_3.address,
            ),  # view_2 -> view_3 (through materialized reference)
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 2)

    def test_get_edges_materialized_with_custom_address(self) -> None:
        """Test get_edges with materialized views that have custom addresses."""
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

        dag_walker = BigQueryViewDagWalker([view_1, view_2, view_3])
        edges = dag_walker.get_edges()

        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        expected_edges = {
            (view_1.address, view_3.address),  # view_1 -> view_3
            (
                view_2.address,
                view_3.address,
            ),  # view_2 -> view_3 (through custom materialized address)
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 2)

    def test_get_edges_complex_multi_level_dag(self) -> None:
        """Test get_edges with a complex multi-level DAG structure that mimics real-world scenarios."""
        # Build a more complex DAG representing a realistic data pipeline:
        # Raw -> Ingest -> Normalized -> Validation
        raw_view = SimpleBigQueryViewBuilder(
            dataset_id="us_ca_raw_data_up_to_date_views",
            view_id="movements_latest",
            description="Latest movements from raw data",
            view_query_template="SELECT * FROM `{project_id}.us_ca_raw_data.movements`",
        ).build()

        ingest_view = SimpleBigQueryViewBuilder(
            dataset_id="us_ca_ingest_view_results",
            view_id="incarceration_periods",
            description="Processed incarceration periods",
            view_query_template="SELECT * FROM `{project_id}.us_ca_raw_data_up_to_date_views.movements_latest`",
        ).build()

        normalized_view = SimpleBigQueryViewBuilder(
            dataset_id="us_ca_normalized_state",
            view_id="state_incarceration_period",
            description="Normalized incarceration periods",
            view_query_template="SELECT * FROM `{project_id}.us_ca_ingest_view_results.incarceration_periods`",
        ).build()

        validation_view = SimpleBigQueryViewBuilder(
            dataset_id="validation_views",
            view_id="incarceration_period_validation",
            description="Validation checks for incarceration periods",
            view_query_template="SELECT * FROM `{project_id}.us_ca_normalized_state.state_incarceration_period`",
        ).build()

        # Create another branch from normalized
        metric_view = SimpleBigQueryViewBuilder(
            dataset_id="aggregated_metrics",
            view_id="incarceration_metrics",
            description="Aggregated metrics from incarceration data",
            view_query_template="SELECT * FROM `{project_id}.us_ca_normalized_state.state_incarceration_period`",
        ).build()

        dag_walker = BigQueryViewDagWalker(
            [raw_view, ingest_view, normalized_view, validation_view, metric_view]
        )
        edges = dag_walker.get_edges()

        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        expected_edges = {
            (raw_view.address, ingest_view.address),
            (ingest_view.address, normalized_view.address),
            (normalized_view.address, validation_view.address),
            (normalized_view.address, metric_view.address),
        }

        self.assertEqual(edge_addresses, expected_edges)
        self.assertEqual(len(edges), 4)

    def test_get_edges_all_deployed_views(self) -> None:
        """Test get_edges with all deployed views - integration test."""
        dag_walker = BigQueryViewDagWalker(self.all_views)
        edges = dag_walker.get_edges()

        # Should have edges for the complex DAG structure
        edge_addresses = {(parent.address, child.address) for parent, child in edges}

        # Verify we have a substantial number of edges (complex DAG should have many)
        self.assertGreater(len(edges), 100)

        # All edges should be between valid views in the DAG
        all_addresses = {view.address for view in self.all_views}
        for parent, child in edges:
            self.assertIn(
                parent.address,
                all_addresses,
                f"Parent {parent.address} not found in deployed views",
            )
            self.assertIn(
                child.address,
                all_addresses,
                f"Child {child.address} not found in deployed views",
            )

        # Verify edges are unique (no duplicates)
        self.assertEqual(len(edges), len(edge_addresses))

        # Verify consistency with get_number_of_edges()
        self.assertEqual(len(edges), dag_walker.get_number_of_edges())

    def test_get_edges_empty_views_list(self) -> None:
        """Test get_edges with an empty list of views."""
        dag_walker = BigQueryViewDagWalker([])
        edges = dag_walker.get_edges()

        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

    def test_get_edges_single_view_with_source_dependencies(self) -> None:
        """Test get_edges with a single view that has source table dependencies but no view dependencies."""
        view = SimpleBigQueryViewBuilder(
            dataset_id="my_dataset",
            view_id="my_view_id",
            description="my view description",
            view_query_template="""SELECT * FROM `{project_id}.some_dataset.some_table`
            LEFT OUTER JOIN `{project_id}.some_dataset.other_table`
            USING (some_col);
            """,
        ).build()

        dag_walker = BigQueryViewDagWalker([view])
        edges = dag_walker.get_edges()

        # Should have no edges since source tables are not BigQueryView nodes
        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

        # But should have source addresses
        node = dag_walker.node_for_view(view)
        self.assertEqual(len(node.source_addresses), 2)
        expected_sources = {
            BigQueryAddress(dataset_id="some_dataset", table_id="some_table"),
            BigQueryAddress(dataset_id="some_dataset", table_id="other_table"),
        }
        self.assertEqual(node.source_addresses, expected_sources)

    def test_get_edges_returns_correct_tuple_format(self) -> None:
        """Test that get_edges returns tuples in (parent, child) format consistently."""
        dag_walker = BigQueryViewDagWalker([self.view_one, self.view_two])
        edges = dag_walker.get_edges()

        self.assertEqual(len(edges), 1)
        parent, child = edges[0]

        # Verify tuple format and types
        self.assertIsInstance(parent, BigQueryView)
        self.assertIsInstance(child, BigQueryView)

        # Verify the relationship direction (parent -> child)
        self.assertEqual(parent.address, self.view_one.address)
        self.assertEqual(child.address, self.view_two.address)

        # Verify parent is indeed referenced in child's query
        self.assertIn(parent.address.to_str(), child.view_query)

    def test_get_edges_deep_dag_chain(self) -> None:
        """Test get_edges with a deep linear chain of dependencies."""
        # Create a chain: view1 -> view2 -> view3 -> view4 -> view5
        views = []
        for i in range(1, 6):
            if i == 1:
                query = "SELECT 1 as col"
            else:
                prev_dataset = f"chain_dataset_{i-1}"
                prev_view = f"chain_view_{i-1}"
                query = f"SELECT * FROM `{{project_id}}.{prev_dataset}.{prev_view}`"

            view = SimpleBigQueryViewBuilder(
                dataset_id=f"chain_dataset_{i}",
                view_id=f"chain_view_{i}",
                description=f"Chain view {i}",
                view_query_template=query,
            ).build()
            views.append(view)

        dag_walker = BigQueryViewDagWalker(views)
        edges = dag_walker.get_edges()

        # Should have 4 edges in the chain
        self.assertEqual(len(edges), 4)

        # Verify the chain order
        edge_addresses = {(parent.address, child.address) for parent, child in edges}
        for i in range(4):
            parent_addr = views[i].address
            child_addr = views[i + 1].address
            self.assertIn(
                (parent_addr, child_addr),
                edge_addresses,
                f"Missing edge from {parent_addr} to {child_addr}",
            )

    def test_get_edges_wide_dag_many_children(self) -> None:
        """Test get_edges with a single parent having many children."""
        # Create a parent view
        parent_view = SimpleBigQueryViewBuilder(
            dataset_id="parent_dataset",
            view_id="parent_view",
            description="Parent view with many children",
            view_query_template="SELECT 1 as col, 'parent' as source",
        ).build()

        # Create multiple child views
        child_views = []
        for i in range(10):  # 10 children
            child_view = SimpleBigQueryViewBuilder(
                dataset_id="child_dataset",
                view_id=f"child_view_{i}",
                description=f"Child view {i}",
                view_query_template="SELECT * FROM `{project_id}.parent_dataset.parent_view`",
            ).build()
            child_views.append(child_view)

        all_views = [parent_view] + child_views
        dag_walker = BigQueryViewDagWalker(all_views)
        edges = dag_walker.get_edges()

        # Should have 10 edges (one to each child)
        self.assertEqual(len(edges), 10)

        # All edges should have the parent view as the parent
        self.assertTrue(
            all(parent.address == parent_view.address for parent, _ in edges)
        )
        self.assertEqual(set(child_views), {child for _, child in edges})

    def test_get_edges_consistency_with_node_relationships(self) -> None:
        """Test that get_edges results are consistent with node parent/child relationships."""
        dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        edges = dag_walker.get_edges()

        # Convert edges to mappings for easier verification
        parent_to_children: Dict[BigQueryView, Set[BigQueryView]] = {}
        child_to_parents: Dict[BigQueryView, Set[BigQueryView]] = {}

        for parent, child in edges:
            if parent not in parent_to_children:
                parent_to_children[parent] = set()
            parent_to_children[parent].add(child)

            if child not in child_to_parents:
                child_to_parents[child] = set()
            child_to_parents[child].add(parent)

        # Verify consistency with node relationships
        for view in self.diamond_shaped_dag_views_list:
            node = dag_walker.node_for_view(view)

            # Check child relationships
            expected_children = {
                dag_walker.view_for_address(addr) for addr in node.child_node_addresses
            }
            actual_children = parent_to_children.get(view, set())
            self.assertEqual(
                actual_children,
                expected_children,
                f"Child mismatch for view {view.address}",
            )

            # Check parent relationships
            expected_parents = {
                dag_walker.view_for_address(addr) for addr in node.parent_node_addresses
            }
            actual_parents = child_to_parents.get(view, set())
            self.assertEqual(
                actual_parents,
                expected_parents,
                f"Parent mismatch for view {view.address}",
            )

    def test_get_edges_to_source_tables_basic(self) -> None:
        """Test basic get_edges_to_source_tables functionality."""
        # Create a view that references a source table
        view_with_source = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="view_with_source",
            description="View that references source tables",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()

        dag_walker = BigQueryViewDagWalker([view_with_source])

        # Mock source table addresses
        source_table_addresses = {
            BigQueryAddress(dataset_id="source_dataset", table_id="source_table"),
            BigQueryAddress(dataset_id="other_dataset", table_id="other_table"),
        }

        edges = dag_walker.get_edges_to_source_tables(source_table_addresses)

        # Should find edge from source_table to view_with_source
        expected_edge = (
            BigQueryAddress(dataset_id="source_dataset", table_id="source_table"),
            view_with_source,
        )

        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0], expected_edge)

    def test_get_edges_to_source_tables_multiple_sources(self) -> None:
        """Test get_edges_to_source_tables with multiple source tables."""
        # Create a view that references multiple source tables
        view_multi_source = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="view_multi_source",
            description="View that references multiple source tables",
            view_query_template="""
                SELECT * FROM `{project_id}.source_dataset.table1`
                UNION ALL
                SELECT * FROM `{project_id}.source_dataset.table2`
            """,
        ).build()

        dag_walker = BigQueryViewDagWalker([view_multi_source])

        # Mock source table addresses that include both tables referenced by the view
        source_table_addresses = {
            BigQueryAddress(dataset_id="source_dataset", table_id="table1"),
            BigQueryAddress(dataset_id="source_dataset", table_id="table2"),
            BigQueryAddress(
                dataset_id="other_dataset", table_id="table3"
            ),  # Not referenced
        }

        edges = dag_walker.get_edges_to_source_tables(source_table_addresses)

        # Should find edges from both source tables to the view
        expected_edges = {
            (
                BigQueryAddress(dataset_id="source_dataset", table_id="table1"),
                view_multi_source,
            ),
            (
                BigQueryAddress(dataset_id="source_dataset", table_id="table2"),
                view_multi_source,
            ),
        }

        edge_set = set(edges)
        self.assertEqual(len(edges), 2)
        self.assertEqual(edge_set, expected_edges)

    def test_get_edges_to_source_tables_no_matches(self) -> None:
        """Test get_edges_to_source_tables with no matching source tables."""
        view = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="view_no_sources",
            description="View with no source table dependencies",
            view_query_template="SELECT 1 as col",
        ).build()

        dag_walker = BigQueryViewDagWalker([view])

        # Provide source table addresses that don't match any dependencies
        source_table_addresses = {
            BigQueryAddress(dataset_id="unrelated_dataset", table_id="unrelated_table"),
        }

        edges = dag_walker.get_edges_to_source_tables(source_table_addresses)

        # Should return empty list
        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

    def test_get_edges_to_source_tables_mixed_dependencies(self) -> None:
        """Test get_edges_to_source_tables with views that have both view and source dependencies."""
        # Create views where one references another view and a source table
        root_view = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="root_view",
            description="Root view",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.raw_table`",
        ).build()

        dependent_view = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="dependent_view",
            description="View that depends on both another view and a source table",
            view_query_template="""
                SELECT a.* FROM `{project_id}.test_dataset.root_view` a
                JOIN `{project_id}.source_dataset.lookup_table` b ON a.id = b.id
            """,
        ).build()

        dag_walker = BigQueryViewDagWalker([root_view, dependent_view])

        source_table_addresses = {
            BigQueryAddress(dataset_id="source_dataset", table_id="raw_table"),
            BigQueryAddress(dataset_id="source_dataset", table_id="lookup_table"),
        }

        edges = dag_walker.get_edges_to_source_tables(source_table_addresses)

        # Should find edges from both source tables to their respective views
        expected_edges = {
            (
                BigQueryAddress(dataset_id="source_dataset", table_id="raw_table"),
                root_view,
            ),
            (
                BigQueryAddress(dataset_id="source_dataset", table_id="lookup_table"),
                dependent_view,
            ),
        }

        edge_set = set(edges)
        self.assertEqual(len(edges), 2)
        self.assertEqual(edge_set, expected_edges)

    def test_get_edges_to_source_tables_empty_input(self) -> None:
        """Test get_edges_to_source_tables with empty source table addresses."""
        dag_walker = BigQueryViewDagWalker([self.view_one, self.view_two])

        edges = dag_walker.get_edges_to_source_tables(set())

        # Should return empty list
        self.assertEqual(len(edges), 0)
        self.assertEqual(edges, [])

    def test_get_number_of_edges_including_source_tables(self) -> None:
        """Test get_number_of_edges_including_source_tables method."""
        # Create a view that references a source table
        view_with_source = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="view_with_source",
            description="View that references source tables",
            view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        ).build()

        # Create a view that depends on the first view
        dependent_view = SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="dependent_view",
            description="View that depends on another view",
            view_query_template="SELECT * FROM `{project_id}.test_dataset.view_with_source`",
        ).build()

        dag_walker = BigQueryViewDagWalker([view_with_source, dependent_view])

        # Mock the get_source_table_addresses function
        with patch(
            "recidiviz.big_query.big_query_view_dag_walker.get_source_table_addresses"
        ) as mock_get_source_tables:
            mock_get_source_tables.return_value = {
                BigQueryAddress(dataset_id="source_dataset", table_id="source_table"),
            }

            # Get counts
            view_to_view_edges = len(dag_walker.get_edges())
            total_edges_including_sources = (
                dag_walker.get_number_of_edges_including_source_tables()
            )

            # Should have 1 view-to-view edge (view_with_source -> dependent_view)
            # Plus 1 source-to-view edge (source_table -> view_with_source)
            self.assertEqual(view_to_view_edges, 1)
            self.assertEqual(total_edges_including_sources, 2)

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


class AsynchronousBigQueryViewDagWalkerTest(TestBigQueryViewDagWalkerBase):
    """DAG walker tests run with asynchronous processing"""

    __test__ = True

    @property
    def synchronous(self) -> bool:
        return False

    def test_dag_process_time(self) -> None:
        """Test that DAG processing works correctly with timing verification."""
        diamond_shaped_dag_views_list = [
            b.build() for b in DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST
        ]

        num_views = len(diamond_shaped_dag_views_list)
        walker = BigQueryViewDagWalker(diamond_shaped_dag_views_list)

        def process_simple(
            view: BigQueryView, _parent_results: Dict[BigQueryView, BigQueryAddress]
        ) -> BigQueryAddress:
            time.sleep(MOCK_VIEW_PROCESS_TIME_SECONDS)
            return view.address

        start = datetime.datetime.now()
        result = walker.process_dag(process_simple, synchronous=False)
        end = datetime.datetime.now()

        # Verify that all views were processed successfully
        self.assertEqual(num_views, len(result.view_results))

        # Verify the result contains all expected views
        expected_addresses = {view.address for view in diamond_shaped_dag_views_list}
        actual_addresses = set(result.view_results.values())
        self.assertEqual(expected_addresses, actual_addresses)

        processing_time = end - start

        # Verify that processing completed in a reasonable time
        # For 6 views with 0.02s each, even with overhead, should complete quickly
        max_reasonable_time = datetime.timedelta(seconds=2.0)
        self.assertLess(processing_time, max_reasonable_time)


# Non-polymorphic tests


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
