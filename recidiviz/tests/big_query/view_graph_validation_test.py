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
from itertools import groupby
from typing import Literal, Sequence
from unittest.mock import patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    BigQueryViewDagWalkerProcessingFailureMode,
)
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
from recidiviz.ingest.views.dataset_config import STATE_BASE_VIEWS_DATASET
from recidiviz.ingest.views.dataset_config import (
    VIEWS_DATASET as INGEST_METADATA_VIEWS_DATASET,
)
from recidiviz.metrics.export.exported_view_utils import (
    get_all_metric_export_view_addresses,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type_list
from recidiviz.validation.views.view_config import (
    CROSS_PROJECT_VALIDATION_VIEW_BUILDERS,
)
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
from recidiviz.view_registry.deployed_views import deployed_view_builders

DEFAULT_TEMPORARY_TABLE_EXPIRATION = 60 * 60 * 1000  # 1 hour


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

        cross_project_view_builder_addresses = [
            view_builder.address
            for view_builder in CROSS_PROJECT_VALIDATION_VIEW_BUILDERS
        ]
        # The cross-project view builders hardcode references to the staging and production projects
        # which end up raising errors in the view graph as we're loading to the recidiviz-bq-emulator-project project
        # TODO(#15080): consider deleting these views entirely since they're not used.
        if v.address in cross_project_view_builder_addresses:
            logging.info(
                "Skipping due to hardcoded prod/staging references: %s", v.address
            )
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

    # Currently, we only run one test per emulator set-up/teardown, so there's no benefit to wiping emulator data
    # We disable this functionality in order to save time on teardown
    wipe_emulator_data_on_teardown = False

    # When developing features, it may be beneficial to select a subset of addresses to run this test for
    # Subclasses can override and provide a list of address strings
    addresses_to_test: list[str] = []

    _view_builders_to_update: list[BigQueryViewBuilder] = []
    _source_table_addresses: list[BigQueryAddress] = []
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
            view_builders_to_update = deployed_view_builders()
            dag_walker = BigQueryViewDagWalker(
                [view_builder.build() for view_builder in view_builders_to_update]
            )
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

        if cls.addresses_to_test:
            sub_dag = dag_walker.get_sub_dag(
                views=[
                    dag_walker.view_for_address(BigQueryAddress.from_str(address))
                    for address in cls.addresses_to_test
                ],
                include_ancestors=True,
                include_descendants=False,
            )
            cls._view_builders_to_update = [
                view_builder
                for view_builder in view_builders_to_update
                if view_builder.address in sub_dag.nodes_by_address.keys()
            ]
            cls._source_table_addresses = list(sub_dag.get_referenced_source_tables())
        else:
            cls._view_builders_to_update = view_builders_to_update
            cls._source_table_addresses = list(
                dag_walker.get_referenced_source_tables()
            )

        super().setUpClass()

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

    # TODO(#18306): Once the BigQueryViewBuilders store the schema, update this check
    #  to just verify that the schema generated in this test matches the schema defined
    #  in the builder, then migrate all the remaining checks in this function to just
    #  query against the schema defined on the builder.
    def _run_view_schema_checks(self) -> None:
        view_address_to_schema = self._load_view_schemas_by_address()
        filtered_view_address_to_schema = {
            address: schema
            for address, schema in view_address_to_schema.items()
            if schema is not None
        }
        skipped_view_addresses = {
            address
            for address, schema in view_address_to_schema.items()
            if schema is None
        }

        self._verify_views_all_have_state_code_column(
            filtered_view_address_to_schema, skipped_view_addresses
        )
        self._verify_views_have_no_unqualified_external_id_columns(
            filtered_view_address_to_schema, skipped_view_addresses
        )
        self._verify_non_export_views_have_no_person_external_id_columns(
            filtered_view_address_to_schema, skipped_view_addresses
        )

    def _schema_has_field(
        self, schema: list[bigquery.SchemaField], field_name: str
    ) -> bool:
        return any(f.name == field_name for f in schema)

    def _split_by_has_field(
        self,
        view_address_to_schema: dict[BigQueryAddress, list[bigquery.SchemaField]],
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
        for address, schema in view_address_to_schema.items():
            # Check if any column name contains the field
            match match_type:
                case "exact":
                    has_matching_field = any(field_name == f.name for f in schema)
                case "suffix":
                    has_matching_field = any(
                        f.name.endswith(field_name) for f in schema
                    )
            if not has_matching_field:
                does_not_have_field_addresses.add(address)
            else:
                has_field_addresses.add(address)
        return has_field_addresses, does_not_have_field_addresses

    def _load_view_schemas_by_address(
        self,
    ) -> dict[BigQueryAddress, list[bigquery.SchemaField] | None]:
        """Loads schemas for every view address into a map. If the view was not loaded
        during the view graph test as an optimization, the schema returned for that
        view will be None.
        """
        view_address_to_schema: dict[
            BigQueryAddress, list[bigquery.SchemaField] | None
        ] = {}
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            get_schema_futures = {
                executor.submit(self._get_schema, vb.address): vb.address
                for vb in self._view_builders_to_update
            }
            for future in futures.as_completed(get_schema_futures):
                address = get_schema_futures[future]
                schema = future.result()
                view_address_to_schema[address] = schema
        return view_address_to_schema

    def _verify_views_all_have_state_code_column(
        self,
        view_address_to_schema: dict[BigQueryAddress, list[bigquery.SchemaField]],
        skipped_addresses: set[BigQueryAddress],
    ) -> None:
        """Throws if we find any view that does not have a state_code column (and is not
        in our list of exempted views).
        """
        (
            has_state_code_addresses,
            missing_state_code_addresses,
        ) = self._split_by_has_field(
            view_address_to_schema, "state_code", match_type="exact"
        )

        expected_missing_state_code_addresses = {
            a for a in self._known_no_state_col_addresses if a not in skipped_addresses
        }

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
        view_address_to_schema: dict[BigQueryAddress, list[bigquery.SchemaField]],
        skipped_addresses: set[BigQueryAddress],
    ) -> None:
        """Validates that no view in our BQ view graph has a column named "external_id".
        All external id columns should have a qualified name, like sentence_external_id
        or stable_person_external_id.
        """
        has_external_id_addresses, no_external_id_addresses = self._split_by_has_field(
            view_address_to_schema, "external_id", match_type="exact"
        )

        expected_has_external_id_addresses = {
            a
            for a in self._known_has_external_id_addresses
            if a not in skipped_addresses
        }

        invalid_addresses = expected_has_external_id_addresses - set(
            view_address_to_schema
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
        view_address_to_schema: dict[BigQueryAddress, list[bigquery.SchemaField]],
        skipped_addresses: set[BigQueryAddress],
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
            view_address_to_schema, "person_external_id", match_type="suffix"
        )

        # Views that are part of metric exports are allowed to have person_external_id columns
        expected_has_person_external_id_addresses = {
            a
            for a in (
                self._allowed_has_person_external_id_addresses()
                | self._known_non_export_views_with_person_external_id
            )
            if a not in skipped_addresses
        }

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
    def get_source_tables(cls) -> list[SourceTableCollection]:
        # The view graph validation test uses all source tables
        # When debugging failures, it may be easier to filter this list of collections
        # down to just the failing set
        with local_project_id_override(cls._get_gcp_project_id()):
            repository = build_source_table_repository_for_collected_schemata(
                project_id=cls.gcp_project_id
            )

        if cls._source_table_addresses:
            return [
                SourceTableCollection(
                    dataset_id=dataset_id,
                    source_tables_by_address={
                        address: repository.source_tables[address]
                        for address in list(source_table_addresses)
                    },
                    update_config=SourceTableCollectionUpdateConfig.protected(),
                    description=f"Fake description for dataset {dataset_id}",
                )
                for dataset_id, source_table_addresses in groupby(
                    sorted(
                        cls._source_table_addresses,
                        key=lambda address: address.dataset_id,
                    ),
                    key=lambda address: address.dataset_id,
                )
            ]

        return repository.source_table_collections

    def run_view_graph_test(self) -> None:
        """Runs an end-to-end test of our view graph"""
        skipped_views = _preprocess_views_to_load_to_emulator(
            self._view_builders_to_update
        )
        view_builders_to_update = [
            view_builder
            for view_builder in self._view_builders_to_update
            if view_builder.address not in skipped_views
        ]
        create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=view_builders_to_update,
            view_update_sandbox_context=None,
            # This script does not do any clean up of previously managed views
            historically_managed_datasets_to_clean=None,
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
        self._run_view_schema_checks()


class StagingViewGraphTest(BaseViewGraphTest):
    gcp_project_id = GCP_PROJECT_STAGING

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_view_graph(self) -> None:
        self.run_view_graph_test()


class ProductionViewGraphTest(BaseViewGraphTest):
    gcp_project_id = GCP_PROJECT_PRODUCTION

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_view_graph(self) -> None:
        self.run_view_graph_test()
