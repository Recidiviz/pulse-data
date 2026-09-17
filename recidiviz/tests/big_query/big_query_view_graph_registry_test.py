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
from recidiviz.big_query.big_query_view_graph_registry import (
    BigQueryViewGraphRegistry,
    view_derived_source_table_collection,
)
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
    reads_from_address: str = "source_dataset.source_table",
) -> SimpleBigQueryViewBuilder:
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_id,
        view_id=view_id,
        description=f"{view_id} description",
        view_query_template="SELECT * FROM `{project_id}." + reads_from_address + "`",
        should_materialize=should_materialize,
        projects_to_deploy=projects_to_deploy,
        clustering_fields=clustering_fields,
        time_partitioning=time_partitioning,
        schema=MINIMAL_SCHEMA,
    )


def _source_table_collection(
    dataset_id: str,
    update_groups: set[SourceTableUpdateGroup],
    table_ids: list[str] | None = None,
) -> SourceTableCollection:
    collection = SourceTableCollection(
        dataset_id=dataset_id,
        description=f"{dataset_id} description",
        update_config=SourceTableCollectionUpdateConfig.protected(),
        update_groups=update_groups,
    )
    for table_id in table_ids or []:
        collection.add_source_table(
            table_id, schema_fields=[c.as_schema_field() for c in MINIMAL_SCHEMA]
        )
    return collection


_PROJECT_ID = "recidiviz-testing"
_OTHER_PROJECT_ID = "recidiviz-testing-2"

_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS = [
    _source_table_collection(
        dataset_id="source_dataset",
        update_groups={SourceTableUpdateGroup.CALC},
        table_ids=["a", "b"],
    )
]


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
        input_source_table_collections=_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS,
    )


def _single_view_graph(
    name: str,
    reads_from_address: str,
    writes_to_dataset: str | None = None,
    update_group: SourceTableUpdateGroup = SourceTableUpdateGroup.CALC,
) -> BigQueryViewGraph:
    """A graph with one view reading |reads_from_address|. The view materializes
    into |writes_to_dataset| if set; otherwise it's a leaf view whose
    output lands in "<name>_out.out".
    """
    return _graph(
        name,
        [
            _view_builder(
                dataset_id=writes_to_dataset or f"{name}_out",
                view_id="out",
                should_materialize=writes_to_dataset is not None,
                reads_from_address=reads_from_address,
            )
        ],
        input_source_table_update_group=update_group,
    )


# TODO(OBT-44681) Add test for more complex graph scenarios
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

    def test_build_registry(self) -> None:
        raw_collection = _source_table_collection(
            dataset_id="us_xx_raw_data",
            update_groups={
                SourceTableUpdateGroup.CALC,
                SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
            },
            table_ids=["case_notes"],
        )
        fake_llm_graph = _graph(
            name="fake_llm_graph",
            view_builders=[
                _view_builder(
                    dataset_id="us_xx_case_notes_raw",
                    view_id="extraction_raw_results",
                    reads_from_address="us_xx_raw_data.case_notes",
                ),
                _view_builder(
                    dataset_id="us_xx_case_notes_outputs",
                    view_id="extraction_results",
                    should_materialize=True,
                    reads_from_address="us_xx_case_notes_raw.extraction_raw_results",
                ),
            ],
        )
        fake_calc_graph = _graph(
            name="fake_calc_graph",
            view_builders=[
                _view_builder(
                    dataset_id="analyst_data",
                    view_id="random_view",
                    reads_from_address="us_xx_raw_data.case_notes",
                ),
                _view_builder(
                    dataset_id="sessions",
                    view_id="cni_view",
                    should_materialize=True,
                    reads_from_address="us_xx_case_notes_outputs.extraction_results",
                ),
            ],
        )

        registry = BigQueryViewGraphRegistry.build(
            project_id=_PROJECT_ID,
            view_graphs=[fake_llm_graph, fake_calc_graph],
            candidate_source_table_collections=[raw_collection],
        )
        resolved_llm_graph = registry.graph_for_name("fake_llm_graph")
        resolved_calc_graph = registry.graph_for_name("fake_calc_graph")

        # fake_llm_graph only reads the plain raw data, so it resolves to just
        # that externally-hydrated collection, untouched.
        self.assertEqual(
            [raw_collection],
            resolved_llm_graph.input_source_table_collections,
        )

        # fake_calc_graph reads the raw data plus fake_llm_graph's materialized
        # output, so it resolves to the derived boundary collection for that output
        # and the plain raw collection.
        expected_boundary_collection = view_derived_source_table_collection(
            dataset_id="us_xx_case_notes_outputs",
            update_groups={SourceTableUpdateGroup.CALC},
            source_tables_by_address={
                config.address: config
                for config in fake_llm_graph.output_source_table_configs_by_dataset[
                    "us_xx_case_notes_outputs"
                ]
            },
        )
        self.assertEqual(
            [expected_boundary_collection, raw_collection],
            resolved_calc_graph.input_source_table_collections,
        )

        # Addresses are owned by the graph that writes them.
        self.assertEqual(
            "fake_llm_graph",
            registry.graph_name_for_address(
                BigQueryAddress(
                    dataset_id="us_xx_case_notes_outputs", table_id="extraction_results"
                )
            ),
        )
        self.assertEqual(
            "fake_llm_graph",
            registry.graph_name_for_address(
                BigQueryAddress(
                    dataset_id="us_xx_case_notes_outputs",
                    table_id="extraction_results_materialized",
                )
            ),
        )
        self.assertEqual(
            "fake_calc_graph",
            registry.graph_name_for_address(
                BigQueryAddress(dataset_id="sessions", table_id="cni_view_materialized")
            ),
        )

    def test_build_input_dataset_lacking_graph_group_raises(self) -> None:
        graph = _single_view_graph(
            "my_graph",
            reads_from_address="source_dataset.b",
            update_group=SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Graph \[my_graph\] resolved to input collections \['source_dataset'\] not "
            r"tagged with the graph's update group \[SourceTableUpdateGroup\.LLM_DOCUMENT_EXTRACTION\]\.$",
        ):
            BigQueryViewGraphRegistry.build(
                project_id=_PROJECT_ID,
                view_graphs=[graph],
                candidate_source_table_collections=_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS,
            )

    def test_build_graph_referencing_unregistered_dataset_raises(self) -> None:
        # This graph references a dataset that is not found in the candidate source table collections
        # or materialized into by a view graph.
        graph = _single_view_graph("my_graph", reads_from_address="unregistered.b")

        with self.assertRaisesRegex(
            ValueError,
            r"^Graph \[my_graph\] reads datasets \['unregistered'\] that no source "
            r"table collection provides and no view graph materializes into\. Add a "
            r"source table collection for them to "
            r"collect_source_table_collections_hydrated_outside_view_graphs, or "
            r"remove the views that reference them\.$",
        ):
            BigQueryViewGraphRegistry.build(
                project_id=_PROJECT_ID,
                view_graphs=[graph],
                candidate_source_table_collections=_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS,
            )

    def test_build_graph_referencing_no_source_tables_raises(self) -> None:
        # A view reading no source table leaves the graph with empty roots, so
        # ResolvedBigQueryViewGraph's non-empty-list validator rejects it.
        sourceless = SimpleBigQueryViewBuilder(
            dataset_id="dataset_1",
            view_id="sourceless",
            description="sourceless description",
            view_query_template="SELECT 1 AS col",
            schema=MINIMAL_SCHEMA,
        )
        graph = _graph("my_graph", [sourceless])

        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[input_source_table_collections\] on "
            r"\[ResolvedBigQueryViewGraph\] must be a non-empty list\. "
            r"Found value \[\[\]\]$",
        ):
            BigQueryViewGraphRegistry.build(
                project_id=_PROJECT_ID,
                view_graphs=[graph],
                candidate_source_table_collections=_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS,
            )

    def test_build_dataset_both_materialized_and_source_raises(self) -> None:
        # _CALC_SOURCE_TABLE_COLLECTION is a plain source collection on "source_dataset"; the
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
                candidate_source_table_collections=_EXTERNALLY_HYDRATED_SOURCE_TABLE_COLLECTIONS,
            )

    def test_build_dataset_with_two_collections_resolves_to_both(self) -> None:
        # TODO(OBT-46919): Once a dataset maps to exactly one collection, a second
        # collection on the same dataset should raise rather than resolve to both.
        graph = _graph(
            "my_graph",
            [_view_builder("dataset_1", "table_1", reads_from_address="shared.a")],
        )
        first = _source_table_collection("shared", {SourceTableUpdateGroup.CALC})
        second = _source_table_collection("shared", {SourceTableUpdateGroup.CALC})

        registry = BigQueryViewGraphRegistry.build(
            project_id=_PROJECT_ID,
            view_graphs=[graph],
            candidate_source_table_collections=[first, second],
        )

        self.assertEqual(
            [first, second],
            registry.graph_for_name("my_graph").input_source_table_collections,
        )

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

    def test_empty_view_graphs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[view_graphs\] on \[BigQueryViewGraphRegistry\] must be a "
            r"non-empty list\. Found value \[\[\]\]$",
        ):
            BigQueryViewGraphRegistry(project_id=_PROJECT_ID, view_graphs=[])

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
