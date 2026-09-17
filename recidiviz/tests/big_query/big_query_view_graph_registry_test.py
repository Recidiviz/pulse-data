# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for big_query_view_graph_registry.py"""

import unittest
from typing import Any
from unittest.mock import patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    ResolvedBigQueryViewGraph,
)
from recidiviz.big_query.big_query_view_graph_registry import BigQueryViewGraphRegistry
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA


def _view_builder(
    dataset_id: str,
    view_id: str,
    should_materialize: bool = False,
    projects_to_deploy: set[str] | None = None,
    clustering_fields: list[str] | None = None,
    time_partitioning: bigquery.TimePartitioning | None = None,
) -> SimpleBigQueryViewBuilder:
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_id,
        view_id=view_id,
        description=f"{view_id} description",
        view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
        should_materialize=should_materialize,
        projects_to_deploy=projects_to_deploy,
        clustering_fields=clustering_fields,
        time_partitioning=time_partitioning,
        schema=MINIMAL_SCHEMA,
    )


def _source_table_collection(
    dataset_id: str, update_groups: set[SourceTableUpdateGroup]
) -> SourceTableCollection:
    return SourceTableCollection(
        dataset_id=dataset_id,
        description=f"{dataset_id} description",
        update_config=SourceTableCollectionUpdateConfig.protected(),
        update_groups=update_groups,
    )


_PROJECT_ID = "recidiviz-testing"
_OTHER_PROJECT_ID = "recidiviz-testing-2"

_CALC_SOURCE_TABLE_COLLECTION = _source_table_collection(
    dataset_id="source_dataset", update_groups={SourceTableUpdateGroup.CALC}
)
_LLM_SOURCE_TABLE_COLLECTION = _source_table_collection(
    dataset_id="llm_dataset",
    update_groups={SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION},
)
_SHARED_SOURCE_TABLE_COLLECTION = _source_table_collection(
    dataset_id="shared_dataset",
    update_groups={
        SourceTableUpdateGroup.CALC,
        SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
    },
)


def _graph(
    name: str,
    view_builders: list[BigQueryViewBuilder],
    input_source_table_update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.CALC,
    project_id: str = _PROJECT_ID,
) -> BigQueryViewGraph:
    return BigQueryViewGraph(
        project_id=project_id,
        name=name,
        input_source_table_update_group=input_source_table_update_group,
        view_builders=view_builders,
    )


def _resolved_graph(
    graph: BigQueryViewGraph,
) -> ResolvedBigQueryViewGraph:
    return ResolvedBigQueryViewGraph(
        view_graph=graph,
        input_source_table_collections=[_CALC_SOURCE_TABLE_COLLECTION],
    )


class TestBigQueryViewGraphRegistry(unittest.TestCase):
    """Tests for BigQueryViewGraphRegistry."""

    project_id_patcher: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value=_PROJECT_ID
        )
        cls.project_id_patcher.start()
        cls.addClassCleanup(cls.project_id_patcher.stop)
        super().setUpClass()

    def test_build_resolves_each_graph(self) -> None:
        calc_graph = _graph("calc_graph", [_view_builder("dataset_1", "table_1")])
        llm_graph = _graph(
            "llm_graph",
            [_view_builder("dataset_2", "table_2")],
            input_source_table_update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        registry = BigQueryViewGraphRegistry.build(
            project_id=_PROJECT_ID,
            view_graphs=[calc_graph, llm_graph],
            candidate_source_table_collections=[
                _CALC_SOURCE_TABLE_COLLECTION,
                _LLM_SOURCE_TABLE_COLLECTION,
                _SHARED_SOURCE_TABLE_COLLECTION,
            ],
        )

        self.assertEqual(_PROJECT_ID, registry.project_id)
        self.assertEqual(
            [
                ResolvedBigQueryViewGraph(
                    view_graph=calc_graph,
                    input_source_table_collections=[
                        _CALC_SOURCE_TABLE_COLLECTION,
                        _SHARED_SOURCE_TABLE_COLLECTION,
                    ],
                ),
                ResolvedBigQueryViewGraph(
                    view_graph=llm_graph,
                    input_source_table_collections=[
                        _LLM_SOURCE_TABLE_COLLECTION,
                        _SHARED_SOURCE_TABLE_COLLECTION,
                    ],
                ),
            ],
            registry.view_graphs,
        )

    def test_build_graph_with_no_input_collections_raises(self) -> None:
        graph = _graph(
            "my_graph",
            [_view_builder("dataset_1", "table_1")],
            input_source_table_update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        with self.assertRaises(ValueError):
            BigQueryViewGraphRegistry.build(
                project_id=_PROJECT_ID,
                view_graphs=[graph],
                candidate_source_table_collections=[_CALC_SOURCE_TABLE_COLLECTION],
            )

    def test_build_dataset_both_materialized_and_source_raises(self) -> None:
        # _CALC_COLLECTION is a plain source collection on "source_dataset"; the
        # graph also materializes a view into "source_dataset".
        graph = _graph(
            "my_graph",
            [_view_builder("source_dataset", "table_1", should_materialize=True)],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Datasets \['source_dataset'\] are both materialized into by a view "
            r"graph and present as plain source table collections\. A dataset must "
            r"hold either view-derived or plain source tables, not both\.$",
        ):
            BigQueryViewGraphRegistry.build(
                project_id=_PROJECT_ID,
                view_graphs=[graph],
                candidate_source_table_collections=[_CALC_SOURCE_TABLE_COLLECTION],
            )

    def test_graph_for_name(self) -> None:
        graph_1 = _resolved_graph(
            _graph("graph_1", [_view_builder("dataset_1", "table_1")])
        )
        graph_2 = _resolved_graph(
            _graph("graph_2", [_view_builder("dataset_2", "table_2")])
        )
        registry = BigQueryViewGraphRegistry(
            project_id=_PROJECT_ID, view_graphs=[graph_1, graph_2]
        )

        self.assertEqual(graph_1, registry.graph_for_name("graph_1"))
        self.assertEqual(graph_2, registry.graph_for_name("graph_2"))

    def test_graph_for_name_unknown_name_raises(self) -> None:
        registry = BigQueryViewGraphRegistry(
            project_id=_PROJECT_ID,
            view_graphs=[
                _resolved_graph(
                    _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                )
            ],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Found no view graph with name \[unknown_graph\]$"
        ):
            registry.graph_for_name("unknown_graph")

    def test_project_id_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"^View graph \[graph_1\] is resolved for project \[{_OTHER_PROJECT_ID}\] "
            rf"but the registry is for project \[{_PROJECT_ID}\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_PROJECT_ID,
                view_graphs=[
                    _resolved_graph(
                        _graph(
                            "graph_1",
                            [_view_builder("dataset_1", "table_1")],
                            project_id=_OTHER_PROJECT_ID,
                        ),
                    )
                ],
            )

    def test_duplicate_graph_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found more than one view graph with name \[graph_1\]$"
        ):
            BigQueryViewGraphRegistry(
                project_id=_PROJECT_ID,
                view_graphs=[
                    _resolved_graph(
                        _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                    ),
                    _resolved_graph(
                        _graph("graph_1", [_view_builder("dataset_2", "table_2")])
                    ),
                ],
            )

    def test_collection_in_multiple_graphs_allowed(self) -> None:
        graph_1 = _resolved_graph(
            _graph("graph_1", [_view_builder("dataset_1", "table_1")])
        )
        graph_2 = _resolved_graph(
            _graph("graph_2", [_view_builder("dataset_2", "table_2")])
        )

        registry = BigQueryViewGraphRegistry(
            project_id=_PROJECT_ID, view_graphs=[graph_1, graph_2]
        )
        self.assertEqual([graph_1, graph_2], registry.view_graphs)

    def test_view_address_in_two_graphs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Address \[dataset_1\.table_1\] in view graph \[graph_2\] already "
            r"belongs to view graph \[graph_1\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_PROJECT_ID,
                view_graphs=[
                    _resolved_graph(
                        _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                    ),
                    _resolved_graph(
                        _graph("graph_2", [_view_builder("dataset_1", "table_1")])
                    ),
                ],
            )

    def test_graph_name_for_address(self) -> None:
        graph_1 = _resolved_graph(
            _graph(
                "graph_1",
                [_view_builder("dataset_1", "table_1", should_materialize=True)],
            )
        )
        graph_2 = _resolved_graph(
            _graph("graph_2", [_view_builder("dataset_2", "table_2")])
        )
        registry = BigQueryViewGraphRegistry(
            project_id=_PROJECT_ID, view_graphs=[graph_1, graph_2]
        )

        self.assertEqual(
            "graph_1",
            registry.graph_name_for_address(
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1")
            ),
        )
        self.assertEqual(
            "graph_1",
            registry.graph_name_for_address(
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1_materialized")
            ),
        )
        self.assertEqual(
            "graph_2",
            registry.graph_name_for_address(
                BigQueryAddress(dataset_id="dataset_2", table_id="table_2")
            ),
        )

    def test_graph_name_for_address_unknown_address_returns_none(self) -> None:
        registry = BigQueryViewGraphRegistry(
            project_id=_PROJECT_ID,
            view_graphs=[
                _resolved_graph(
                    _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                )
            ],
        )

        self.assertIsNone(
            registry.graph_name_for_address(
                BigQueryAddress(dataset_id="unknown_dataset", table_id="unknown_table")
            )
        )

    def test_materialized_address_in_two_graphs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Address \[dataset_1\.table_1_materialized\] in view graph \[graph_2\] "
            r"already belongs to view graph \[graph_1\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_PROJECT_ID,
                view_graphs=[
                    _resolved_graph(
                        _graph(
                            "graph_1",
                            [
                                _view_builder(
                                    "dataset_1", "table_1", should_materialize=True
                                )
                            ],
                        )
                    ),
                    _resolved_graph(
                        _graph(
                            "graph_2",
                            [
                                SimpleBigQueryViewBuilder(
                                    dataset_id="dataset_2",
                                    view_id="table_2",
                                    description="table_2 description",
                                    view_query_template="SELECT * FROM `{project_id}.a.b`",
                                    should_materialize=True,
                                    materialized_address_override=_view_builder(
                                        "dataset_1",
                                        "table_1",
                                        should_materialize=True,
                                    ).materialized_address,
                                    schema=MINIMAL_SCHEMA,
                                )
                            ],
                        )
                    ),
                ],
            )
