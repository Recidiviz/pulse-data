# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for federated_cloud_sql_table_big_query_view_collector.py."""

import unittest
from unittest import mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view_collector import (
    StateSegmentedSchemaFederatedBigQueryViewCollector,
    UnsegmentedSchemaFederatedBigQueryViewCollector,
)
from recidiviz.persistence.database.schema.case_triage.schema import CaseTriageBase
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.view_registry.deployed_views import (
    CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
    CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
)

NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML = """
region_codes_to_exclude: []
"""

PAUSED_REGION_CLOUD_SQL_CONFIG_YAML = """
region_codes_to_exclude:
  - US_ND
"""

PAUSED_REGION_LOWERCASE_CLOUD_SQL_CONFIG_YAML = """
region_codes_to_exclude:
  - us_nd
"""


PAUSED_BAD_REGION_CLOUD_SQL_CONFIG_YAML = """
region_codes_to_exclude:
  - us_ab
"""


class FederatedCloudSQLTableBigQueryViewCollectorTest(unittest.TestCase):
    """Tests for federated_cloud_sql_table_big_query_view_collector.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-staging"

        test_secrets = {
            # pylint: disable=protected-access
            SQLAlchemyEngineManager._get_cloudsql_instance_id_key(
                schema_type
            ): f"test-project:us-east2:{schema_type.value}-data"
            for schema_type in SchemaType
        }
        self.get_secret_patcher = mock.patch("recidiviz.utils.secrets.get_secret")

        self.get_secret_patcher.start().side_effect = test_secrets.get

        self.gcs_factory_patcher = mock.patch(
            "recidiviz.admin_panel.dataset_metadata_store.GcsfsFactory.build"
        )

        self.fake_fs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_fs

        self.fake_config_path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-staging-configs/cloud_sql_to_bq_config.yaml"
        )

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.metadata_patcher.stop()

    def test_collect_do_not_crash(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=PAUSED_REGION_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )
        for schema_type in SchemaType:
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                continue
            config = CloudSqlToBQConfig.for_schema_type(schema_type)

            if config.is_state_segmented_refresh_schema():
                _ = StateSegmentedSchemaFederatedBigQueryViewCollector(
                    config
                ).collect_view_builders()
            else:
                _ = UnsegmentedSchemaFederatedBigQueryViewCollector(
                    config
                ).collect_view_builders()

    def test_all_datasets_registered_as_managed(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )

        for schema_type in SchemaType:
            datasets_for_schema_refresh = set()
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                continue
            config = CloudSqlToBQConfig.for_schema_type(schema_type)

            if config.is_state_segmented_refresh_schema():
                view_builders = StateSegmentedSchemaFederatedBigQueryViewCollector(
                    config
                ).collect_view_builders()
            else:
                view_builders = UnsegmentedSchemaFederatedBigQueryViewCollector(
                    config
                ).collect_view_builders()

            for view_builder in view_builders:
                datasets_for_schema_refresh.add(view_builder.dataset_id)
                if view_builder.materialized_address:
                    datasets_for_schema_refresh.add(
                        view_builder.materialized_address.dataset_id
                    )

            self.assertEqual(
                [],
                sorted(
                    datasets_for_schema_refresh.difference(
                        CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA[
                            schema_type
                        ]
                    )
                ),
            )

    def test_all_unioned_regional_datasets_registered_as_managed(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )

        for schema_type in SchemaType:
            datasets_for_schema_refresh = set()
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                continue
            config = CloudSqlToBQConfig.for_schema_type(schema_type)

            if not config.is_state_segmented_refresh_schema():
                # Only state segmented schemas require unioned regional datasets
                # for the refresh
                continue

            datasets_for_schema_refresh.add(
                config.unioned_regional_dataset(dataset_override_prefix=None)
            )

            self.assertEqual(
                [],
                sorted(
                    datasets_for_schema_refresh.difference(
                        CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA[
                            schema_type
                        ]
                    )
                ),
            )

    def test_state_segmented_collector(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.OPERATIONS)
        collector = StateSegmentedSchemaFederatedBigQueryViewCollector(config)
        builders = collector.collect_view_builders()
        direct_ingest_states = get_direct_ingest_states_existing_in_env()
        self.assertEqual(
            len(direct_ingest_states) * len(OperationsBase.metadata.sorted_tables),
            len(builders),
        )
        view_addresses = set()
        materialized_addresses = set()
        for builder in builders:
            view = builder.build()
            view_addresses.add(view.address)
            if not view.materialized_address:
                raise ValueError(f"Materialized address None for view [{view.address}]")
            materialized_addresses.add(view.materialized_address)

        self.assertEqual(
            {"operations_v2_cloudsql_connection"},
            {a.dataset_id for a in view_addresses},
        )

        expected_materialized_datasets = {
            f"{state_code.value.lower()}_operations_regional"
            for state_code in direct_ingest_states
        }
        self.assertEqual(
            expected_materialized_datasets,
            {a.dataset_id for a in materialized_addresses},
        )

        # No addresses should clobber each other
        self.assertEqual(len(view_addresses), len(builders))
        self.assertEqual(len(materialized_addresses), len(builders))
        self.assertEqual(set(), view_addresses.intersection(materialized_addresses))

    def test_state_segmented_collector_paused_regions(self) -> None:
        self._run_paused_region_test(PAUSED_REGION_CLOUD_SQL_CONFIG_YAML)

    def test_state_segmented_collector_paused_regions_lowercase(self) -> None:
        self._run_paused_region_test(PAUSED_REGION_LOWERCASE_CLOUD_SQL_CONFIG_YAML)

    def test_state_segmented_collector_bad_paused_regionn(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=PAUSED_BAD_REGION_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.OPERATIONS)
        with self.assertRaisesRegex(
            ValueError,
            r"Found disabled region\(s\) \[{'US_AB'}\] which are not valid direct "
            r"ingest state codes.",
        ):
            _ = StateSegmentedSchemaFederatedBigQueryViewCollector(config)

    def _run_paused_region_test(self, yaml_str: str) -> None:
        """Runs a test for a config where one region is paused."""
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=yaml_str,
            content_type="text/yaml",
        )
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.OPERATIONS)
        collector = StateSegmentedSchemaFederatedBigQueryViewCollector(config)
        builders = collector.collect_view_builders()
        direct_ingest_states = get_direct_ingest_states_existing_in_env()
        num_schema_tables = len(OperationsBase.metadata.sorted_tables)
        num_paused_regions = 1
        self.assertEqual(
            len(direct_ingest_states) * num_schema_tables
            - num_paused_regions * num_schema_tables,
            len(builders),
        )
        view_addresses = set()
        materialized_addresses = set()
        for builder in builders:
            view = builder.build()
            view_addresses.add(view.address)
            if not view.materialized_address:
                raise ValueError(f"Materialized address None for view [{view.address}]")
            materialized_addresses.add(view.materialized_address)

        self.assertEqual(
            {"operations_v2_cloudsql_connection"},
            {a.dataset_id for a in view_addresses},
        )
        self.assertNotIn(
            "us_nd_operations_regional", {a.dataset_id for a in materialized_addresses}
        )
        self.assertEqual(
            {t.name for t in OperationsBase.metadata.sorted_tables},
            {a.table_id for a in materialized_addresses},
        )
        # No addresses should clobber each other
        self.assertEqual(len(view_addresses), len(builders))
        self.assertEqual(len(materialized_addresses), len(builders))
        self.assertEqual(set(), view_addresses.intersection(materialized_addresses))

    def test_unsegmented_collector_case_triage(self) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=PAUSED_REGION_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.CASE_TRIAGE)
        collector = UnsegmentedSchemaFederatedBigQueryViewCollector(config)
        builders = collector.collect_view_builders()
        self.assertEqual(
            len(CaseTriageBase.metadata.sorted_tables),
            len(builders),
        )
        view_addresses = set()
        materialized_addresses = set()
        for builder in builders:
            view = builder.build()
            view_addresses.add(view.address)
            if not view.materialized_address:
                raise ValueError(f"Materialized address None for view [{view.address}]")
            materialized_addresses.add(view.materialized_address)

            if view.view_id == "client_info":
                # Check that we explicitly select columns
                self.assertTrue("client_info.state_code" in view.view_query)

        self.assertEqual(
            {"case_triage_cloudsql_connection"}, {a.dataset_id for a in view_addresses}
        )
        self.assertEqual(
            {"case_triage_federated_regional"},
            {a.dataset_id for a in materialized_addresses},
        )
        self.assertEqual(
            {t.name for t in CaseTriageBase.metadata.sorted_tables},
            {a.table_id for a in materialized_addresses},
        )

        # No addresses should clobber each other
        self.assertEqual(len(view_addresses), len(builders))
        self.assertEqual(len(materialized_addresses), len(builders))
        self.assertEqual(set(), view_addresses.intersection(materialized_addresses))
