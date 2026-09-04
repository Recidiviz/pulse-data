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
"""Tests for big_query_view_graph.py"""

import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    BigQueryViewGraphRegistry,
    ResolvedBigQueryViewGraph,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA
from recidiviz.utils.metadata import local_project_id_override

_STAGING = "recidiviz-staging"
_PRODUCTION = "recidiviz-123"


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


def _collection(
    dataset_id: str, update_groups: set[SourceTableUpdateGroup]
) -> SourceTableCollection:
    return SourceTableCollection(
        dataset_id=dataset_id,
        description=f"{dataset_id} description",
        update_config=SourceTableCollectionUpdateConfig.protected(),
        update_groups=update_groups,
    )


_CALC_COLLECTION = _collection("source_dataset", {SourceTableUpdateGroup.CALC})
_LLM_COLLECTION = _collection(
    "llm_dataset", {SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION}
)
_SHARED_COLLECTION = _collection(
    "shared_dataset",
    {SourceTableUpdateGroup.CALC, SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION},
)


def _graph(
    name: str,
    view_builder_candidates: list[BigQueryViewBuilder],
    input_source_table_update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.CALC,
) -> BigQueryViewGraph:
    return BigQueryViewGraph(
        name=name,
        view_builder_candidates=view_builder_candidates,
        input_source_table_update_group=input_source_table_update_group,
    )


def _resolved(
    graph: BigQueryViewGraph, project_id: str = _STAGING
) -> ResolvedBigQueryViewGraph:
    return ResolvedBigQueryViewGraph(
        project_id=project_id,
        view_graph=graph,
        input_source_table_collections=[_CALC_COLLECTION],
    )


class TestResolvedBigQueryViewGraph(unittest.TestCase):
    """Tests for ResolvedBigQueryViewGraph."""

    def test_view_builders_filtered_to_project(self) -> None:
        everywhere = _view_builder("dataset_1", "everywhere")
        staging_only = _view_builder(
            "dataset_1", "staging_only", projects_to_deploy={_STAGING}
        )
        graph = _graph("my_graph", [everywhere, staging_only])

        self.assertEqual(
            [everywhere, staging_only],
            _resolved(graph, project_id=_STAGING).view_builders,
        )
        self.assertEqual(
            [everywhere], _resolved(graph, project_id=_PRODUCTION).view_builders
        )

    def test_no_view_builders_in_project_raises(self) -> None:
        graph = _graph(
            "my_graph",
            [_view_builder("dataset_1", "staging_only", projects_to_deploy={_STAGING})],
        )

        with self.assertRaisesRegex(
            ValueError,
            rf"^View graph \[my_graph\] has no view builders deployed in project "
            rf"\[{_PRODUCTION}\]$",
        ):
            _resolved(graph, project_id=_PRODUCTION)

    def test_delegates_to_wrapped_graph(self) -> None:
        graph = _graph("my_graph", [_view_builder("dataset_1", "table_1")])
        resolved = _resolved(graph)

        self.assertEqual(graph.name, resolved.name)
        self.assertEqual(
            graph.input_source_table_update_group,
            resolved.input_source_table_update_group,
        )

    def test_build_dag_walker(self) -> None:
        graph = _graph(
            "my_graph",
            [
                _view_builder("dataset_1", "table_1"),
                _view_builder("dataset_2", "table_2"),
                _view_builder(
                    "dataset_3", "staging_only", projects_to_deploy={_STAGING}
                ),
            ],
        )
        resolved = _resolved(graph, project_id=_PRODUCTION)

        with local_project_id_override(_PRODUCTION):
            walker = resolved.build_dag_walker()

        self.assertEqual(
            {b.address for b in resolved.view_builders},
            set(walker.nodes_by_address.keys()),
        )

    def test_output_source_table_configs_by_dataset(self) -> None:
        resolved = _resolved(
            _graph(
                "my_graph",
                [
                    _view_builder(
                        "dataset_1",
                        "table_1",
                        should_materialize=True,
                        clustering_fields=["col"],
                    ),
                    _view_builder("dataset_1", "table_2", should_materialize=True),
                    _view_builder("dataset_2", "table_3", should_materialize=True),
                    # Not materialized, so it contributes no output config.
                    _view_builder("dataset_2", "table_4"),
                ],
            )
        )

        schema_fields = [
            bigquery.SchemaField("col", "STRING", "NULLABLE", description="col")
        ]
        with local_project_id_override("recidiviz-456"):
            self.assertEqual(
                {
                    "dataset_1": [
                        SourceTableConfig(
                            address=BigQueryAddress(
                                dataset_id="dataset_1", table_id="table_1_materialized"
                            ),
                            description="Materialized data from view [dataset_1.table_1]. "
                            "View description:\ntable_1 description\nExplore this view's "
                            "lineage at https://go/lineage-staging/dataset_1.table_1",
                            schema_fields=schema_fields,
                            clustering_fields=["col"],
                        ),
                        SourceTableConfig(
                            address=BigQueryAddress(
                                dataset_id="dataset_1", table_id="table_2_materialized"
                            ),
                            description="Materialized data from view [dataset_1.table_2]. "
                            "View description:\ntable_2 description\nExplore this view's "
                            "lineage at https://go/lineage-staging/dataset_1.table_2",
                            schema_fields=schema_fields,
                            clustering_fields=[],
                        ),
                    ],
                    "dataset_2": [
                        SourceTableConfig(
                            address=BigQueryAddress(
                                dataset_id="dataset_2", table_id="table_3_materialized"
                            ),
                            description="Materialized data from view [dataset_2.table_3]. "
                            "View description:\ntable_3 description\nExplore this view's "
                            "lineage at https://go/lineage-staging/dataset_2.table_3",
                            schema_fields=schema_fields,
                            clustering_fields=[],
                        ),
                    ],
                },
                resolved.build_output_source_table_configs_by_dataset(),
            )

    def test_output_source_table_configs_partitioned_view_raises(self) -> None:
        resolved = _resolved(
            _graph(
                "my_graph",
                [
                    _view_builder(
                        "dataset_1",
                        "table_1",
                        should_materialize=True,
                        time_partitioning=bigquery.TimePartitioning(field="col"),
                    ),
                ],
            )
        )

        with local_project_id_override("recidiviz-456"):
            with self.assertRaisesRegex(
                ValueError,
                r"^Graph \[my_graph\] materializes partitioned view "
                r"\[dataset_1\.table_1\]; partitioned outputs cannot be derived as "
                r"source tables\.$",
            ):
                _ = resolved.build_output_source_table_configs_by_dataset()


class TestBigQueryViewGraphRegistryBuild(unittest.TestCase):
    """Tests for BigQueryViewGraphRegistry.build()."""

    def test_build_resolves_each_graph(self) -> None:
        calc_graph = _graph("calc_graph", [_view_builder("dataset_1", "table_1")])
        llm_graph = _graph(
            "llm_graph",
            [_view_builder("dataset_2", "table_2")],
            input_source_table_update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        registry = BigQueryViewGraphRegistry.build(
            project_id=_STAGING,
            view_graphs=[calc_graph, llm_graph],
            candidate_collections=[
                _CALC_COLLECTION,
                _LLM_COLLECTION,
                _SHARED_COLLECTION,
            ],
        )

        self.assertEqual(_STAGING, registry.project_id)
        self.assertEqual(
            [
                ResolvedBigQueryViewGraph(
                    project_id=_STAGING,
                    view_graph=calc_graph,
                    input_source_table_collections=[
                        _CALC_COLLECTION,
                        _SHARED_COLLECTION,
                    ],
                ),
                ResolvedBigQueryViewGraph(
                    project_id=_STAGING,
                    view_graph=llm_graph,
                    input_source_table_collections=[
                        _LLM_COLLECTION,
                        _SHARED_COLLECTION,
                    ],
                ),
            ],
            registry.view_graphs,
        )

    def test_build_filters_view_builders_to_project(self) -> None:
        graph = _graph(
            "my_graph",
            [
                _view_builder("dataset_1", "everywhere"),
                _view_builder(
                    "dataset_1", "staging_only", projects_to_deploy={_STAGING}
                ),
            ],
        )

        staging_registry = BigQueryViewGraphRegistry.build(
            project_id=_STAGING,
            view_graphs=[graph],
            candidate_collections=[_CALC_COLLECTION],
        )
        production_registry = BigQueryViewGraphRegistry.build(
            project_id=_PRODUCTION,
            view_graphs=[graph],
            candidate_collections=[_CALC_COLLECTION],
        )

        self.assertEqual(
            {"everywhere", "staging_only"},
            {
                b.view_id
                for b in staging_registry.graph_for_name("my_graph").view_builders
            },
        )
        self.assertEqual(
            {"everywhere"},
            {
                b.view_id
                for b in production_registry.graph_for_name("my_graph").view_builders
            },
        )

    def test_build_graph_with_no_input_collections_raises(self) -> None:
        graph = _graph(
            "my_graph",
            [_view_builder("dataset_1", "table_1")],
            input_source_table_update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        with self.assertRaises(ValueError):
            BigQueryViewGraphRegistry.build(
                project_id=_STAGING,
                view_graphs=[graph],
                candidate_collections=[_CALC_COLLECTION],
            )


class TestBigQueryViewGraphRegistry(unittest.TestCase):
    """Tests for BigQueryViewGraphRegistry."""

    def test_graph_for_name(self) -> None:
        graph_1 = _resolved(_graph("graph_1", [_view_builder("dataset_1", "table_1")]))
        graph_2 = _resolved(_graph("graph_2", [_view_builder("dataset_2", "table_2")]))
        registry = BigQueryViewGraphRegistry(
            project_id=_STAGING, view_graphs=[graph_1, graph_2]
        )

        self.assertEqual(graph_1, registry.graph_for_name("graph_1"))
        self.assertEqual(graph_2, registry.graph_for_name("graph_2"))

    def test_graph_for_name_unknown_name_raises(self) -> None:
        registry = BigQueryViewGraphRegistry(
            project_id=_STAGING,
            view_graphs=[
                _resolved(_graph("graph_1", [_view_builder("dataset_1", "table_1")]))
            ],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Found no view graph with name \[unknown_graph\]$"
        ):
            registry.graph_for_name("unknown_graph")

    def test_project_id_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"^View graph \[graph_1\] is resolved for project \[{_PRODUCTION}\] "
            rf"but the registry is for project \[{_STAGING}\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_STAGING,
                view_graphs=[
                    _resolved(
                        _graph("graph_1", [_view_builder("dataset_1", "table_1")]),
                        project_id=_PRODUCTION,
                    )
                ],
            )

    def test_duplicate_graph_name_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found more than one view graph with name \[graph_1\]$"
        ):
            BigQueryViewGraphRegistry(
                project_id=_STAGING,
                view_graphs=[
                    _resolved(
                        _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                    ),
                    _resolved(
                        _graph("graph_1", [_view_builder("dataset_2", "table_2")])
                    ),
                ],
            )

    def test_collection_in_multiple_graphs_allowed(self) -> None:
        graph_1 = _resolved(_graph("graph_1", [_view_builder("dataset_1", "table_1")]))
        graph_2 = _resolved(_graph("graph_2", [_view_builder("dataset_2", "table_2")]))

        registry = BigQueryViewGraphRegistry(
            project_id=_STAGING, view_graphs=[graph_1, graph_2]
        )
        self.assertEqual([graph_1, graph_2], registry.view_graphs)

    def test_view_address_in_two_graphs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Address \[dataset_1\.table_1\] in view graph \[graph_2\] already "
            r"belongs to view graph \[graph_1\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_STAGING,
                view_graphs=[
                    _resolved(
                        _graph("graph_1", [_view_builder("dataset_1", "table_1")])
                    ),
                    _resolved(
                        _graph("graph_2", [_view_builder("dataset_1", "table_1")])
                    ),
                ],
            )

    def test_materialized_address_in_two_graphs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Address \[dataset_1\.table_1_materialized\] in view graph \[graph_2\] "
            r"already belongs to view graph \[graph_1\]$",
        ):
            BigQueryViewGraphRegistry(
                project_id=_STAGING,
                view_graphs=[
                    _resolved(
                        _graph(
                            "graph_1",
                            [
                                _view_builder(
                                    "dataset_1", "table_1", should_materialize=True
                                )
                            ],
                        )
                    ),
                    _resolved(
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
