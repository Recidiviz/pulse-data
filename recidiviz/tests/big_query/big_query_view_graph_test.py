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
    ResolvedBigQueryViewGraph,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
    SourceTableUpdateGroup,
)
from recidiviz.tests.big_query.big_query_view_test_utils import MINIMAL_SCHEMA
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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


_CALC_COLLECTION = _source_table_collection(
    dataset_id="source_dataset", update_groups={SourceTableUpdateGroup.CALC}
)


def _graph(
    name: str,
    view_builder_candidates: list[BigQueryViewBuilder],
    input_source_table_update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.CALC,
    project_id: str = GCP_PROJECT_STAGING,
) -> BigQueryViewGraph:
    return BigQueryViewGraph.build(
        project_id=project_id,
        name=name,
        input_source_table_update_group=input_source_table_update_group,
        view_builder_candidates=view_builder_candidates,
    )


def _resolved(
    graph: BigQueryViewGraph,
) -> ResolvedBigQueryViewGraph:
    return ResolvedBigQueryViewGraph(
        view_graph=graph,
        input_source_table_collections=[_CALC_COLLECTION],
    )


class TestBigQueryViewGraph(unittest.TestCase):
    """Tests for BigQueryViewGraph."""

    def test_build_filters_view_builders_to_project(self) -> None:
        everywhere = _view_builder("dataset_1", "everywhere")
        staging_only = _view_builder(
            "dataset_1", "staging_only", projects_to_deploy={GCP_PROJECT_STAGING}
        )

        self.assertEqual(
            [everywhere, staging_only],
            BigQueryViewGraph.build(
                project_id=GCP_PROJECT_STAGING,
                name="my_graph",
                input_source_table_update_group=SourceTableUpdateGroup.CALC,
                view_builder_candidates=[everywhere, staging_only],
            ).view_builders,
        )
        self.assertEqual(
            [everywhere],
            BigQueryViewGraph.build(
                project_id=GCP_PROJECT_PRODUCTION,
                name="my_graph",
                input_source_table_update_group=SourceTableUpdateGroup.CALC,
                view_builder_candidates=[everywhere, staging_only],
            ).view_builders,
        )

    def test_build_no_view_builders_in_project_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"^View graph \[my_graph\] has no view builders deployed in project "
            rf"\[{GCP_PROJECT_PRODUCTION}\]$",
        ):
            BigQueryViewGraph.build(
                project_id=GCP_PROJECT_PRODUCTION,
                name="my_graph",
                input_source_table_update_group=SourceTableUpdateGroup.CALC,
                view_builder_candidates=[
                    _view_builder(
                        "dataset_1",
                        "staging_only",
                        projects_to_deploy={GCP_PROJECT_STAGING},
                    )
                ],
            )

    def test_empty_view_builders_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Field \[view_builders\] on \[BigQueryViewGraph\] must be a non-empty list",
        ):
            BigQueryViewGraph(
                view_builders=[],
                project_id=GCP_PROJECT_STAGING,
                name="my_graph",
                input_source_table_update_group=SourceTableUpdateGroup.CALC,
            )

    def test_constructor_with_builder_not_deployed_in_project_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^View graph \[my_graph\] contains builders that do not deploy in "
            rf"project \[{GCP_PROJECT_PRODUCTION}\]: \['dataset_1.staging_only'\]$",
        ):
            BigQueryViewGraph(
                view_builders=[
                    _view_builder(
                        "dataset_1",
                        "staging_only",
                        projects_to_deploy={GCP_PROJECT_STAGING},
                    )
                ],
                project_id=GCP_PROJECT_PRODUCTION,
                name="my_graph",
                input_source_table_update_group=SourceTableUpdateGroup.CALC,
            )

    def test_dag_walker(self) -> None:
        graph = _graph(
            "my_graph",
            [
                _view_builder("dataset_1", "table_1"),
                _view_builder("dataset_2", "table_2"),
                _view_builder(
                    "dataset_3",
                    "staging_only",
                    projects_to_deploy={GCP_PROJECT_STAGING},
                ),
            ],
            project_id=GCP_PROJECT_PRODUCTION,
        )

        with local_project_id_override(GCP_PROJECT_PRODUCTION):
            walker = graph.dag_walker

        self.assertEqual(
            {b.address for b in graph.view_builders},
            set(walker.nodes_by_address.keys()),
        )

    def test_output_source_table_configs_by_dataset(self) -> None:
        graph = _graph(
            name="my_graph",
            view_builder_candidates=[
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
            project_id="recidiviz-456",
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
                graph.output_source_table_configs_by_dataset,
            )
            self.assertEqual({"dataset_1", "dataset_2"}, graph.output_datasets)

    def test_output_source_table_configs_partitioned_view_raises(self) -> None:
        graph = _graph(
            name="my_graph",
            view_builder_candidates=[
                _view_builder(
                    "dataset_1",
                    "table_1",
                    should_materialize=True,
                    time_partitioning=bigquery.TimePartitioning(field="col"),
                ),
            ],
        )

        with local_project_id_override("recidiviz-456"):
            with self.assertRaisesRegex(
                ValueError,
                r"^Graph \[my_graph\] materializes partitioned view "
                r"\[dataset_1\.table_1\]; partitioned outputs cannot be derived as "
                r"source tables\.$",
            ):
                _ = graph.output_source_table_configs_by_dataset

    def test_root_datasets(self) -> None:
        graph = _graph(
            name="my_graph",
            view_builder_candidates=[
                # References source_dataset.source_table but not the materialized
                # output of table_1, so only source_dataset is a root.
                _view_builder("dataset_1", "table_1", should_materialize=True),
            ],
        )

        with local_project_id_override(GCP_PROJECT_STAGING):
            self.assertEqual({"source_dataset"}, graph.root_datasets)


class TestResolvedBigQueryViewGraph(unittest.TestCase):
    """Tests for ResolvedBigQueryViewGraph."""

    def test_delegates_to_wrapped_graph(self) -> None:
        graph = _graph(
            name="my_graph",
            view_builder_candidates=[_view_builder("dataset_1", "table_1")],
        )
        resolved = _resolved(graph)

        self.assertEqual(graph.name, resolved.name)
        self.assertEqual(graph.project_id, resolved.project_id)
        self.assertEqual(graph.view_builders, resolved.view_builders)
        self.assertEqual(
            graph.input_source_table_update_group,
            resolved.input_source_table_update_group,
        )

        with local_project_id_override(GCP_PROJECT_STAGING):
            self.assertIs(graph.dag_walker, resolved.dag_walker)

    def test_mistagged_input_collection_raises(self) -> None:
        graph = _graph(
            name="my_graph",
            view_builder_candidates=[_view_builder("dataset_1", "table_1")],
            input_source_table_update_group=SourceTableUpdateGroup.CALC,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Graph \[my_graph\] resolved to input collections \['source_dataset'\] "
            r"not tagged with the graph's update group "
            r"\[SourceTableUpdateGroup.CALC\]\.$",
        ):
            ResolvedBigQueryViewGraph(
                view_graph=graph,
                input_source_table_collections=[
                    _source_table_collection(
                        dataset_id="source_dataset",
                        update_groups={SourceTableUpdateGroup.IDENTITY_INGEST},
                    )
                ],
            )
