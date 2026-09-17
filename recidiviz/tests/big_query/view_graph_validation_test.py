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
from concurrent import futures
from typing import AbstractSet, Literal, NamedTuple, Sequence
from unittest.mock import patch

import attr
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_schema_utils import (
    diff_declared_schema_to_bq_schema,
    format_schema_diffs,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    BigQueryViewColumn,
    Record,
)
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.big_query.big_query_view_graph_registry import BigQueryViewGraphRegistry
from recidiviz.big_query.view_update_manager import (
    CreateOrUpdateViewStatus,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.calculator.query.state.views.reference.product_display_person_external_ids import (
    PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.product_stable_person_external_ids import (
    PRODUCT_STABLE_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.common import attr_validators
from recidiviz.ingest.views.dataset_config import STATE_BASE_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import (
    VIEWS_DATASET as INGEST_METADATA_VIEWS_DATASET,
)
from recidiviz.metrics.export.exported_view_utils import (
    get_all_metric_export_view_addresses,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.known_undocumented_columns import (
    KNOWN_UNDOCUMENTED_COLUMNS,
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
from recidiviz.utils.string import is_meaningful_docstring
from recidiviz.utils.types import assert_type_list
from recidiviz.validation.views.view_config import (
    get_view_builders_for_views_to_update as get_validation_view_builders,
)
from recidiviz.view_registry.deployed_address_schema_utils import (
    get_deployed_addresses_without_state_code_column,
)
from recidiviz.view_registry.deployed_view_external_id_exemptions import (
    NORMALIZED_STATE_VIEWS_DATASET,
    get_known_non_export_views_with_person_external_id_column,
    get_known_views_with_unqualified_external_id,
)
from recidiviz.view_registry.deployed_view_graphs import deployed_view_graph_registry

DEFAULT_TEMPORARY_TABLE_EXPIRATION = 60 * 60 * 1000  # 1 hour


class ViewSchemaPair(NamedTuple):
    """The declared BigQueryViewColumnschema and deployed BigQuery SchemaField schema
    for a single view."""

    declared: Sequence[BigQueryViewColumn]
    deployed: list[bigquery.SchemaField]


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
    _all_deployed_view_addresses: set[BigQueryAddress] = set()
    _known_no_state_col_addresses: set[BigQueryAddress] = set()
    _known_has_external_id_addresses: set[BigQueryAddress] = set()
    _known_non_export_views_with_person_external_id: set[BigQueryAddress] = set()
    _metric_export_view_addresses: set[BigQueryAddress] = set()
    _validation_view_addresses: set[BigQueryAddress] = set()

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
            cls._all_deployed_view_addresses = {
                vb.address
                for graph in registry.view_graphs
                for vb in graph.view_builders
            }
            cls._known_no_state_col_addresses = (
                get_deployed_addresses_without_state_code_column(
                    cls._get_gcp_project_id()
                )
            )
            cls._known_has_external_id_addresses = (
                get_known_views_with_unqualified_external_id(cls._get_gcp_project_id())
            )
            cls._known_non_export_views_with_person_external_id = (
                get_known_non_export_views_with_person_external_id_column(
                    cls._get_gcp_project_id()
                )
            )
            cls._metric_export_view_addresses = get_all_metric_export_view_addresses()
            cls._validation_view_addresses = {
                vb.address for vb in get_validation_view_builders()
            }

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

    @classmethod
    def _allowed_has_person_external_id_addresses(cls) -> set[BigQueryAddress]:
        """This is the set of views which may have a person external_id col and if they do,
        we are fine with it. They also might not have a person external_id and that is
        fine too.
        """

        return (
            # Views that are part of metric exports are allowed to have
            # person_external_id columns (we will enforce in other tests that these
            # columns are properly named).
            cls._metric_export_view_addresses
            # It can be useful to have a person external id in a validation view output
            # (when paired with an id_type). Do not penalize if these have a
            # person_external_id column.
            | cls._validation_view_addresses
            | {
                # These reference views have the external ids that should be pulled into
                # exported views at the very end but are not exported directly.
                PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER.address,
                PRODUCT_STABLE_PERSON_EXTERNAL_IDS_VIEW_BUILDER.address,
            }
            | {
                # These views just mirror our state/normalized_state schemas
                BigQueryAddress(
                    dataset_id=dataset,
                    table_id="state_person_external_id_view",
                )
                for dataset in [
                    NORMALIZED_STATE_VIEWS_DATASET,
                    STATE_BASE_VIEWS_DATASET,
                ]
            }
        )

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

    def _get_schema(
        self, address: BigQueryAddress
    ) -> list[bigquery.SchemaField] | None:
        try:
            schema = self.bq_client.get_table(address=address).schema
        except NotFound:
            # This view was skipped for optimization reasons, do not check for a
            # state_code column.
            return None

        if not schema:
            raise ValueError(f"Found empty schema for [{address.to_str()}]")

        return list(assert_type_list(schema, bigquery.SchemaField))

    # TODO(#80204): Update this check to just verify that the schemas deployed in this
    #  test match the schemas declared in the views, then migrate all the remaining
    #  checks in this function to a new test that verifies against declared schemas.
    def _run_view_schema_checks(
        self, view_builders: Sequence[BigQueryViewBuilder]
    ) -> None:
        deployed_view_address_to_schemas = self._load_view_schemas_by_address(
            view_builders
        )

        self._verify_declared_schemas_match_actual_schemas(
            deployed_view_address_to_schemas
        )
        self._verify_views_all_have_state_code_column(deployed_view_address_to_schemas)
        self._verify_views_have_no_unqualified_external_id_columns(
            deployed_view_address_to_schemas
        )
        self._verify_non_export_views_have_no_person_external_id_columns(
            deployed_view_address_to_schemas
        )
        self._verify_meaningful_column_descriptions(deployed_view_address_to_schemas)

    def _schema_has_field(
        self, schema: list[bigquery.SchemaField], field_name: str
    ) -> bool:
        return any(f.name == field_name for f in schema)

    def _split_by_has_field(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
        field_name: str,
        match_type: Literal["exact", "suffix"],
    ) -> tuple[set[BigQueryAddress], set[BigQueryAddress]]:
        """Given a map of view addresses to schemas, splits the addresses into those
        that have a column matching field_name and those that do not. The match_type
        controls whether we match on exact column names or suffixes (e.g. to match
        columns like stable_person_external_id and display_person_external_id).
        """
        does_not_have_field_addresses = set()
        has_field_addresses = set()
        for address, schemas in view_address_to_schemas.items():
            # Check if any column name contains the field
            match match_type:
                case "exact":
                    has_matching_field = any(
                        field_name == f.name for f in schemas.deployed
                    )
                case "suffix":
                    has_matching_field = any(
                        f.name.endswith(field_name) for f in schemas.deployed
                    )
            if not has_matching_field:
                does_not_have_field_addresses.add(address)
            else:
                has_field_addresses.add(address)
        return has_field_addresses, does_not_have_field_addresses

    def _load_view_schemas_by_address(
        self, view_builders: Sequence[BigQueryViewBuilder]
    ) -> dict[BigQueryAddress, ViewSchemaPair]:
        """Loads schemas for every view address into a map, pairing each view's
        declared schema (from the builder) with its deployed schema (from
        BigQuery). Views that were not loaded for the view graph test as an
        optimization are omitted from the returned map.
        """
        declared_by_address: dict[BigQueryAddress, Sequence[BigQueryViewColumn]] = {
            vb.address: vb.build().schema for vb in view_builders
        }
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair] = {}
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            get_schema_futures = {
                executor.submit(self._get_schema, vb.address): vb.address
                for vb in view_builders
            }
            for future in futures.as_completed(get_schema_futures):
                address = get_schema_futures[future]
                deployed = future.result()
                if deployed is None:
                    continue
                view_address_to_schemas[address] = ViewSchemaPair(
                    declared=declared_by_address[address], deployed=deployed
                )
        return view_address_to_schemas

    def _verify_declared_schemas_match_actual_schemas(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
    ) -> None:
        """Throws if we find a view whose declared schema does not match the
        schema of the loaded view.
        """
        mismatched_schemas: dict[str, list[tuple[str, bigquery.SchemaField]]] = {}

        for address, schemas in view_address_to_schemas.items():
            schema_diff = diff_declared_schema_to_bq_schema(
                schemas.declared, schemas.deployed
            )
            if len(schema_diff) > 0:
                mismatched_schemas[address.to_str()] = schema_diff

        if mismatched_schemas:
            raise ValueError(
                f"Found {len(mismatched_schemas)} view(s) with declared schemas that don't"
                f" match the deployed view schemas:\n"
                f"{format_schema_diffs(mismatched_schemas)}"
            )

    def _verify_views_all_have_state_code_column(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
    ) -> None:
        """Throws if we find any view that does not have a state_code column (and is not
        in our list of exempted views).
        """
        (
            has_state_code_addresses,
            missing_state_code_addresses,
        ) = self._split_by_has_field(
            view_address_to_schemas, "state_code", match_type="exact"
        )

        expected_missing_state_code_addresses = self._known_no_state_col_addresses

        unexpected_missing_state_code_addresses = (
            missing_state_code_addresses - expected_missing_state_code_addresses
        )
        if unexpected_missing_state_code_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_missing_state_code_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with no state_code column:{addresses_list}"
                f"\nIf there is an expected reason why these views don't have a "
                f"state_code column, add exemptions in "
                f"recidiviz/view_registry/deployed_address_schema_utils.py."
            )

        unexpected_has_state_code_addresses = has_state_code_addresses.intersection(
            expected_missing_state_code_addresses
        )
        if unexpected_has_state_code_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_state_code_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that have state_code columns but are returned "
                f"by get_deployed_addresses_without_state_code_column() but which have "
                f"a state_code column:{addresses_list}\nThese should be removed from "
                f"that list."
            )

    def _verify_views_have_no_unqualified_external_id_columns(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
    ) -> None:
        """Validates that no view in our BQ view graph has a column named "external_id".
        All external id columns should have a qualified name, like sentence_external_id
        or stable_person_external_id.
        """
        has_external_id_addresses, no_external_id_addresses = self._split_by_has_field(
            view_address_to_schemas, "external_id", match_type="exact"
        )

        expected_has_external_id_addresses = self._known_has_external_id_addresses

        invalid_addresses = (
            expected_has_external_id_addresses - self._all_deployed_view_addresses
        )
        if invalid_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                invalid_addresses, indent_level=2
            )
            raise ValueError(
                f"Found addresses returned by "
                f"get_known_views_with_unqualified_external_id() which are not a valid "
                f"view address: {addresses_list}"
            )

        unexpected_has_external_id_addresses = (
            has_external_id_addresses - expected_has_external_id_addresses
        )
        if unexpected_has_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with an unqualified external_id column:"
                f"{addresses_list}\nThe name external_id is not specific enough and "
                f"can lead to confusion. Please rename to a more specific name, like "
                f"sentence_external_id or display_person_external_id. If there is an "
                f"expected reason why this view has an external_id column (rare!), "
                f"please discuss with someone on Doppler and then add it to "
                f"_KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN."
            )

        unexpected_no_external_id_addresses = no_external_id_addresses.intersection(
            expected_has_external_id_addresses
        )
        if unexpected_no_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_no_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that do not have an external_id columns but are "
                f"listed in _KNOWN_VIEWS_WITH_UNQUALIFIED_EXTERNAL_ID_COLUMN:"
                f"{addresses_list}\nThese should be removed from that list (yay!)."
            )

    def _verify_non_export_views_have_no_person_external_id_columns(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
    ) -> None:
        """Validates that views not part of metric exports don't have columns matching
        the pattern *person_external_id. We should not pass external id information
        through our internal, foundational views but rather should join at the very end
        to get a relevant person external id.
        """
        (
            has_person_external_id_addresses,
            no_person_external_id_addresses,
        ) = self._split_by_has_field(
            view_address_to_schemas, "person_external_id", match_type="suffix"
        )

        # Views that are part of metric exports are allowed to have person_external_id columns
        expected_has_person_external_id_addresses = (
            self._allowed_has_person_external_id_addresses()
            | self._known_non_export_views_with_person_external_id
        )

        unexpected_has_person_external_id_addresses = (
            has_person_external_id_addresses - expected_has_person_external_id_addresses
        )
        if unexpected_has_person_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_has_person_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found unexpected views with a *person_external_id column that are "
                f"not part of metric exports:{addresses_list}\n"
                f"We should not pass external id information through our internal, "
                f"foundational views but rather should join to one of the "
                f"product_display_person_external_id / product_stable_person_external_id "
                f"views at the very end to get a relevant person external id. If there "
                f"is an expected reason why this view has a *person_external_id "
                f"column, please discuss with someone on Doppler and then add it to "
                f"_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN in "
                f"recidiviz/view_registry/deployed_view_external_id_exemptions.py."
            )

        unexpected_no_person_external_id_addresses = (
            no_person_external_id_addresses.intersection(
                self._known_non_export_views_with_person_external_id
            )
        )
        if unexpected_no_person_external_id_addresses:
            addresses_list = BigQueryAddress.addresses_to_str(
                unexpected_no_person_external_id_addresses, indent_level=2
            )
            raise ValueError(
                f"Found views / tables that do not have a *person_external_id column "
                f"but are listed in "
                f"_KNOWN_NON_EXPORT_VIEWS_WITH_PERSON_EXTERNAL_ID_COLUMN:"
                f"{addresses_list}\nThese should be removed from that list (yay!)."
            )

    @classmethod
    def _bad_description_columns(
        cls, columns: Sequence[BigQueryViewColumn]
    ) -> list[str]:
        """Returns the names of declared columns (recursing into Records) whose
        description is not meaningful. Record subfield names are returned as "parent_name.subfield_name".
        """
        names: list[str] = []
        if not columns:
            return names
        for col in columns:
            if (
                col.description == COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT
                or not is_meaningful_docstring(col.description)
            ):
                names.append(col.name)
            if isinstance(col, Record):
                for sub_name in cls._bad_description_columns(col.fields):
                    names.append(f"{col.name}.{sub_name}")
        return names

    def _verify_meaningful_column_descriptions(
        self,
        view_address_to_schemas: dict[BigQueryAddress, ViewSchemaPair],
    ) -> None:
        """Validates that every declared column description is "meaningful" —
        non-empty, not indicating unfinished work, and not equal to the
        COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT sentinel. (view_address, column_name)
        pairs in KNOWN_UNDOCUMENTED_COLUMNS are exempted.
        """
        unexpected_bad: dict[BigQueryAddress, list[str]] = {}
        unexpected_fixed: dict[BigQueryAddress, list[str]] = {}

        for address, schemas in view_address_to_schemas.items():

            actual_set = set(self._bad_description_columns(schemas.declared))
            expected_set = set(KNOWN_UNDOCUMENTED_COLUMNS.get(address, []))

            new_bad = sorted(actual_set - expected_set)
            if new_bad:
                unexpected_bad[address] = new_bad

            now_fixed = sorted(expected_set - actual_set)
            if now_fixed:
                unexpected_fixed[address] = now_fixed

        error_chunks: list[str] = []
        if unexpected_bad:
            lines = [
                f"  {addr.to_str()}: {cols}"
                for addr, cols in sorted(
                    unexpected_bad.items(),
                    key=lambda kv: (kv[0].dataset_id, kv[0].table_id),
                )
            ]
            error_chunks.append(
                "Found new columns with non-meaningful descriptions (empty, "
                "TO" + "DO/XXX prefix, or the COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT "
                "sentinel) that are not in KNOWN_UNDOCUMENTED_COLUMNS:\n"
                + "\n".join(lines)
                + "\n\nPlease write a real description for these columns. If there are "
                "known undocumented columns in the same view, please document those as "
                "well and remove them from the KNOWN_UNDOCUMENTED_COLUMNS exemption list."
            )
        if unexpected_fixed:
            lines = [
                f"  {addr.to_str()}: {cols}"
                for addr, cols in sorted(
                    unexpected_fixed.items(),
                    key=lambda kv: (kv[0].dataset_id, kv[0].table_id),
                )
            ]
            error_chunks.append(
                "Found columns listed in KNOWN_UNDOCUMENTED_COLUMNS that now "
                "have a meaningful description (yay!). Please remove them from "
                "recidiviz/tests/big_query/known_undocumented_columns.py:\n"
                + "\n".join(lines)
            )
        if error_chunks:
            raise ValueError("\n\n".join(error_chunks))

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
        create_managed_dataset_and_deploy_views_for_view_builders(
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
        self._run_view_schema_checks(graph_test_spec.view_builders_to_update)


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
