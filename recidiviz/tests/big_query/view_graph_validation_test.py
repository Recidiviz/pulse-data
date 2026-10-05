# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for verifying view graph syntax and column names"""
import logging
from typing import AbstractSet, Sequence
from unittest.mock import patch

import attr
import pytest
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_schema_utils import (
    diff_declared_schema_to_bq_schema,
    format_schema_diffs,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    BigQueryViewDagWalkerProcessingFailureMode,
    ProcessDagResult,
)
from recidiviz.big_query.big_query_view_graph_registry import BigQueryViewGraphRegistry
from recidiviz.big_query.view_update_manager import (
    CreateOrUpdateViewResult,
    CreateOrUpdateViewStatus,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.common import attr_validators
from recidiviz.ingest.views.dataset_config import (
    VIEWS_DATASET as INGEST_METADATA_VIEWS_DATASET,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.utils.big_query_emulator_log_parser import (
    BigQueryEmulatorLogParser,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type
from recidiviz.view_registry.deployed_view_graphs import deployed_view_graph_registry

DEFAULT_TEMPORARY_TABLE_EXPIRATION = 60 * 60 * 1000  # 1 hour


@attr.define(frozen=True, kw_only=True)
class _ViewGraphTestSpec:
    """The inputs for one deployed view graph's compilation subtest."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the view graph under test."""

    view_builders_to_update: list[BigQueryViewBuilder] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(BigQueryViewBuilder),
        ]
    )
    """Builders for the graph's views, filtered by addresses_to_test when set."""

    source_table_collections: list[SourceTableCollection] = attr.ib(
        validator=attr_validators.is_list_of(SourceTableCollection)
    )
    """Source table collections to seed the emulator with for this graph."""


def _filter_collections_to_addresses(
    collections: list[SourceTableCollection],
    addresses: AbstractSet[BigQueryAddress],
) -> list[SourceTableCollection]:
    """Returns copies of these collections that contain only the tables at the given
    addresses, dropping collections left with no tables.
    """
    filtered_collections = []
    for collection in collections:
        source_tables_by_address = {
            address: config
            for address, config in collection.source_tables_by_address.items()
            if address in addresses
        }
        if source_tables_by_address:
            filtered_collections.append(
                attr.evolve(
                    collection, source_tables_by_address=source_tables_by_address
                )
            )
    return filtered_collections


def _preprocess_views_to_load_to_emulator(
    candidate_view_builders: Sequence[BigQueryViewBuilder],
) -> set[BigQueryAddress]:
    """Skips views that do not need to be tested by the emulator"""
    dag_walker = BigQueryViewDagWalker(
        [view_builder.build() for view_builder in candidate_view_builders]
    )

    def determine_skip_status(
        v: BigQueryView, parent_results: dict[BigQueryView, CreateOrUpdateViewStatus]
    ) -> CreateOrUpdateViewStatus:
        node = dag_walker.node_for_view(v)
        # Raw data views are fairly well tested and their logic duplicative. Only test views that are in use
        if "_raw_data_up_to_date_views" in v.dataset_id and (
            node.is_leaf or len(node.child_node_addresses) == 0
        ):
            logging.info("Skipping unused raw data view: %s", v.address)
            return CreateOrUpdateViewStatus.SKIPPED

        # These views are largely duplicative queries that are rarely touched
        # We're fine with the tradeoff of missing coverage in favor of saving test time
        if v.dataset_id == INGEST_METADATA_VIEWS_DATASET:
            logging.info("Skipping ingest metadata view: %s", v.address)
            return CreateOrUpdateViewStatus.SKIPPED

        if any(
            parent_view.address
            for parent_view, parent_status in parent_results.items()
            if parent_status == CreateOrUpdateViewStatus.SKIPPED
        ):
            logging.info("Skipping due to skipped parents: %s", v.address)
            return CreateOrUpdateViewStatus.SKIPPED

        # TODO(goccy/bigquery-emulator#318): The emulator does not support use of the bqutil UDFs
        if "bqutil.fn" in v.view_query_template:
            logging.info("Skipping due to unsupported  UDF: %s", v.address)
            return CreateOrUpdateViewStatus.SKIPPED

        return CreateOrUpdateViewStatus.SUCCESS_WITHOUT_CHANGES

    results = dag_walker.process_dag(
        view_process_fn=determine_skip_status, synchronous=False
    )
    results.log_processing_stats(0)

    return {
        view.address
        for view, result in results.view_results.items()
        if result == CreateOrUpdateViewStatus.SKIPPED
    }


@pytest.mark.view_graph_validation
class BaseViewGraphTest(BigQueryEmulatorTestCase):
    """Base class for view graph validation tests"""

    # The project_id to use for all view collection / building operations.
    gcp_project_id: str | None = None

    # Each emulator is discarded when the next view graph's subtest restarts it,
    # and the last one is stopped in tearDownClass, so wiping data first only
    # costs time
    wipe_emulator_data_on_teardown = False

    # Each view graph's subtest boots its own emulator seeded with that graph's
    # source tables
    start_emulator_automatically = False

    # When developing features, it may be beneficial to select a subset of addresses to run this test for
    # Subclasses can override and provide a list of address strings
    addresses_to_test: list[str] = []

    _graph_test_specs: list[_ViewGraphTestSpec] = []
    # Emulator logs from each view graph's subtest, keyed by graph name
    _emulator_logs_by_graph_name: dict[str, str] = {}

    @classmethod
    def _get_gcp_project_id(cls) -> str:
        if cls.gcp_project_id is None:
            raise ValueError(
                "Must specify gcp_project_id when running the view graph validation test"
            )

        if cls.gcp_project_id not in DATA_PLATFORM_GCP_PROJECTS:
            raise ValueError(f"Invalid project id: {cls.gcp_project_id}")

        return cls.gcp_project_id

    @classmethod
    def setUpClass(cls) -> None:
        with local_project_id_override(cls._get_gcp_project_id()):
            registry = deployed_view_graph_registry(metadata.project_id())
            cls._graph_test_specs = cls._build_graph_test_specs(registry)
        cls._emulator_logs_by_graph_name = {}
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        """Prints each view graph's emulator logs, then stops the last emulator so
        the base class does not print its logs a second time."""
        cls._print_emulator_logs_by_graph()
        cls.control.stop_emulator()
        super().tearDownClass()

    @classmethod
    def _print_emulator_logs_by_graph(cls) -> None:
        """Prints query stats for each view graph's emulator run, and the raw logs
        when show_emulator_logs_on_failure is set."""
        for graph_name, logs in cls._emulator_logs_by_graph_name.items():
            parser = BigQueryEmulatorLogParser()
            parser.parse_logs(logs)
            print(f"\n\nStats for {cls.__name__} view graph [{graph_name}]")
            print("=" * 80)
            parser.print_stats(n=10)
            print("=" * 80)
            if cls.show_emulator_logs_on_failure:
                print(logs)

    @classmethod
    def _build_graph_test_specs(
        cls, registry: BigQueryViewGraphRegistry
    ) -> list[_ViewGraphTestSpec]:
        """Returns one test spec per view graph in the registry. When the
        addresses_to_test debug hook is set, each graph's spec is filtered down to
        the ancestors of the requested addresses in that graph, graphs containing
        none of the requested addresses get no spec, and an address found in no
        graph raises.
        """
        if not cls.addresses_to_test:
            return [
                _ViewGraphTestSpec(
                    name=graph.name,
                    view_builders_to_update=graph.view_builders,
                    # The emulator holds every loaded table in memory, so load
                    # only the registered inputs the graph's views actually read.
                    source_table_collections=_filter_collections_to_addresses(
                        graph.input_source_table_collections,
                        graph.dag_walker.get_referenced_source_tables(),
                    ),
                )
                for graph in registry.view_graphs
            ]

        addresses_to_test = {
            BigQueryAddress.from_str(address) for address in cls.addresses_to_test
        }
        specs = []
        matched_addresses: set[BigQueryAddress] = set()
        for graph in registry.view_graphs:
            dag_walker = graph.dag_walker
            addresses_in_graph = addresses_to_test & set(dag_walker.nodes_by_address)
            if not addresses_in_graph:
                continue
            matched_addresses |= addresses_in_graph
            sub_dag = dag_walker.get_sub_dag(
                views=[
                    dag_walker.view_for_address(address)
                    for address in addresses_in_graph
                ],
                include_ancestors=True,
                include_descendants=False,
            )
            specs.append(
                _ViewGraphTestSpec(
                    name=graph.name,
                    view_builders_to_update=[
                        vb
                        for vb in graph.view_builders
                        if vb.address in sub_dag.nodes_by_address
                    ],
                    source_table_collections=_filter_collections_to_addresses(
                        graph.input_source_table_collections,
                        sub_dag.get_referenced_source_tables(),
                    ),
                )
            )
        if unmatched_addresses := addresses_to_test - matched_addresses:
            raise ValueError(
                f"Found no view graph containing these addresses_to_test addresses:"
                f"{BigQueryAddress.addresses_to_str(unmatched_addresses, indent_level=2)}"
            )
        return specs

    def setUp(self) -> None:
        super().setUp()
        # Patch row level permissions to reduce the number of queries submitted to the emulator
        patched_client = BigQueryClientImpl()

        patch.object(
            patched_client,
            "drop_row_level_permissions",
            new=lambda table: None,
        ).start()
        patch.object(
            patched_client,
            "apply_row_level_permissions",
            new=lambda table: None,
        ).start()

        self.view_update_client_patcher = patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl",
            autospec=True,
            return_value=patched_client,
        )
        self.view_update_client_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.view_update_client_patcher.stop()

    def _verify_declared_schemas_match_actual_schemas(
        self, process_dag_result: ProcessDagResult[CreateOrUpdateViewResult]
    ) -> None:
        """Throws if we find a view whose declared schema does not match the
        schema of the deployed view.
        """
        mismatched_schemas: dict[str, list[tuple[str, bigquery.SchemaField]]] = {}

        for view, result in process_dag_result.view_results.items():
            deployed_schema = assert_type(result.updated_view, bigquery.Table).schema
            if not deployed_schema:
                raise ValueError(f"Found empty schema for [{view.address.to_str()}]")
            schema_diff = diff_declared_schema_to_bq_schema(
                view.schema, deployed_schema
            )
            if schema_diff:
                mismatched_schemas[view.address.to_str()] = schema_diff

        if mismatched_schemas:
            raise ValueError(
                f"Found {len(mismatched_schemas)} view(s) with declared schemas that don't"
                f" match the deployed view schemas:\n"
                f"{format_schema_diffs(mismatched_schemas)}"
            )

    def run_all_view_graphs_test(self) -> None:
        """Compiles every deployed view graph against the emulator, one subtest per
        graph, seeding the emulator with exactly that graph's registered input
        source tables."""
        for graph_test_spec in self._graph_test_specs:
            with self.subTest(view_graph=graph_test_spec.name):
                # The emulator only accepts source tables at boot, so each graph
                # gets a fresh emulator seeded with its own inputs. This also
                # stops the previous graph's emulator, if any.
                self.restart_emulator_with_source_tables(
                    graph_test_spec.source_table_collections
                )
                try:
                    self._compile_graph_and_run_schema_checks(graph_test_spec)
                finally:
                    self._emulator_logs_by_graph_name[
                        graph_test_spec.name
                    ] = self.control.get_logs()

    def _compile_graph_and_run_schema_checks(
        self, graph_test_spec: _ViewGraphTestSpec
    ) -> None:
        """Runs an end-to-end test of one view graph"""
        skipped_views = _preprocess_views_to_load_to_emulator(
            graph_test_spec.view_builders_to_update
        )
        view_builders_to_update = [
            view_builder
            for view_builder in graph_test_spec.view_builders_to_update
            if view_builder.address not in skipped_views
        ]
        (
            process_dag_result,
            _,
        ) = create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=view_builders_to_update,
            view_update_sandbox_context=None,
            default_table_expiration_for_new_datasets=DEFAULT_TEMPORARY_TABLE_EXPIRATION,
            views_might_exist=False,
            # We expect each node in the view
            # DAG to process quickly, but also don't care if a node takes longer
            # than expected (we see this happen occasionally, perhaps because we
            # are being rate-limited?), because it does not indicate that overall
            # view materialization has gotten too expensive for that view.
            allow_slow_views=True,
            # None of the tables exist already, so always re-materialize
            rematerialize_changed_views_only=False,
            # we want to try to surface as many failures as possible, so set mode to
            # fail exhaustively
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )
        self._verify_declared_schemas_match_actual_schemas(process_dag_result)


class StagingViewGraphTest(BaseViewGraphTest):
    gcp_project_id = GCP_PROJECT_STAGING

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_all_view_graphs(self) -> None:
        self.run_all_view_graphs_test()


class ProductionViewGraphTest(BaseViewGraphTest):
    gcp_project_id = GCP_PROJECT_PRODUCTION

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_all_view_graphs(self) -> None:
        self.run_all_view_graphs_test()
