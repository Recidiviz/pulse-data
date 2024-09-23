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
from itertools import groupby
from typing import Sequence

import pytest

from recidiviz.big_query.big_query_address import BigQueryAddress
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
    get_all_source_table_datasets,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.view_config import (
    CROSS_PROJECT_VALIDATION_VIEW_BUILDERS,
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

    project_id: str | None = None

    # Currently, we only run one test per emulator set-up/teardown, so there's no benefit to wiping emulator data
    # We disable this functionality in order to save time on teardown
    wipe_emulator_data_on_teardown = False

    # When developing features, it may be beneficial to select a subset of addresses to run this test for
    # Subclasses can override and provide a list of address strings
    addresses_to_test: list[str] = []

    _view_builders_to_update: list[BigQueryViewBuilder] = []
    _source_table_addresses: list[BigQueryAddress] = []

    @classmethod
    def setUpClass(cls) -> None:
        if cls.project_id is None:
            raise ValueError(
                "Must specify project id when running the view graph validation test"
            )

        with local_project_id_override(cls.project_id):
            view_builders_to_update = deployed_view_builders()
            dag_walker = BigQueryViewDagWalker(
                [view_builder.build() for view_builder in view_builders_to_update]
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

        super().setUpClass()

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        if cls.project_id is None:
            raise ValueError(
                "Must specify project id when running the view graph validation test"
            )

        # The view graph validation test uses all source tables
        # When debugging failures, it may be easier to filter this list of collections down to just the failing set
        with local_project_id_override(cls.project_id):
            repository = build_source_table_repository_for_collected_schemata(
                project_id=cls.project_id
            )

        if cls._source_table_addresses:
            return [
                SourceTableCollection(
                    dataset_id=dataset_id,
                    source_tables_by_address={
                        address: repository.source_tables[address]
                        for address in list(source_table_addresses)
                    },
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
            view_source_table_datasets=get_all_source_table_datasets(),
            view_builders_to_update=view_builders_to_update,
            sandbox_context=None,
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
        )


class StagingViewGraphTest(BaseViewGraphTest):
    project_id = GCP_PROJECT_STAGING

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_view_graph(self) -> None:
        self.run_view_graph_test()


class ProductionViewGraphTest(BaseViewGraphTest):
    project_id = GCP_PROJECT_PRODUCTION

    # When debugging this test, view addresses can be added here in the form of `{dataset_id}.{view_id}`
    addresses_to_test: list[str] = []

    def test_view_graph(self) -> None:
        self.run_view_graph_test()
