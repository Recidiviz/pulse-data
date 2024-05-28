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
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.deploy.deploy_test_empty_views import (
    DEFAULT_TEMPORARY_TABLE_EXPIRATION,
)
from recidiviz.tools.deploy.generate_emulator_table_schema_json import (
    SOURCE_TABLES_JSON_PATH,
    write_emulator_source_tables_json,
)
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_ci,
)
from recidiviz.validation.views.view_config import (
    CROSS_PROJECT_VALIDATION_VIEW_BUILDERS,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import all_deployed_view_builders


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

    @classmethod
    def get_input_schema_json_path(cls) -> str:
        if not cls.project_id:
            raise ValueError("project_id must be set")

        # Source table JSON is built prior to test run in CI
        if in_ci():
            return SOURCE_TABLES_JSON_PATH

        return write_emulator_source_tables_json(cls.project_id)

    def run_view_graph_test(self) -> None:
        view_builders_to_update = [
            view_builder
            for view_builder in all_deployed_view_builders()
            if view_builder.should_deploy_in_project(project_id=self.project_id)
        ]
        skipped_views = _preprocess_views_to_load_to_emulator(view_builders_to_update)
        view_builders_to_update = [
            view_builder
            for view_builder in view_builders_to_update
            if not view_builder.address in skipped_views
        ]
        create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=view_builders_to_update,
            address_overrides=None,
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

    def test_view_graph(self) -> None:
        self.run_view_graph_test()


class ProductionViewGraphTest(BaseViewGraphTest):
    project_id = GCP_PROJECT_PRODUCTION

    def test_view_graph(self) -> None:
        self.run_view_graph_test()
