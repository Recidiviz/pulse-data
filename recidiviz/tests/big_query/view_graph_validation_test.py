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
from typing import Sequence

import pytest
from google.api_core.exceptions import InternalServerError

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager import (
    CreateOrUpdateViewStatus,
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.ingest.views.dataset_config import (
    VIEWS_DATASET as INGEST_METADATA_VIEWS_DATASET,
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
from recidiviz.utils.types import assert_type
from recidiviz.validation.views.view_config import (
    CROSS_PROJECT_VALIDATION_VIEW_BUILDERS,
)
from recidiviz.view_registry.deployed_address_schema_utils import (
    get_deployed_addresses_without_state_code_column,
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

    def _has_state_code_column(self, address: BigQueryAddress) -> bool | None:
        if not self.bq_client.table_exists(address):
            # This view was skipped for optimization reasons, do not check for a
            # state_code column.
            return None

        project_specific_address = address.to_project_specific_address(
            assert_type(self.project_id, str)
        )

        # TODO(#33979): The emulator does not properly hydrate the the schema field
        #  on tables so we test for the existence of a state_code column by trying
        #  to query it. We can replace this with a call to
        #  self.bq_client.get_table(address).schema when that issue is fixed.
        try:
            query = f"""
                SELECT state_code 
                FROM {project_specific_address.format_address_for_query()}
                LIMIT 0
            """
            query_job = self.bq_client.run_query_async(
                query_str=query, use_query_cache=False
            )
            query_job.result()
            return True
        except InternalServerError:
            return False

    def _check_for_missing_state_code_column_views(self) -> None:
        """Raises an error if we find any views that do not have a state_code column
        and have not been explicitly exempted from having one.
        """

        has_state_code_addresses = set()
        missing_state_code_addresses = set()
        skipped_addresses = set()
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            get_has_column_futures = {
                executor.submit(self._has_state_code_column, vb.address): vb.address
                for vb in self._view_builders_to_update
            }
            for future in futures.as_completed(get_has_column_futures):
                address = get_has_column_futures[future]
                has_state_code_col = future.result()
                if has_state_code_col is None:
                    skipped_addresses.add(address)
                elif has_state_code_col:
                    has_state_code_addresses.add(address)
                else:
                    missing_state_code_addresses.add(address)

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
        skipped_views = _preprocess_views_to_load_to_emulator(
            self._view_builders_to_update
        )
        view_builders_to_update = [
            view_builder
            for view_builder in self._view_builders_to_update
            if view_builder.address not in skipped_views
        ]
        create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets={
                a.dataset_id for a in self._source_table_addresses
            },
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
            # None of the tables exist already, so always materialize
            materialize_changed_views_only=False,
        )

        self._check_for_missing_state_code_column_views()


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
