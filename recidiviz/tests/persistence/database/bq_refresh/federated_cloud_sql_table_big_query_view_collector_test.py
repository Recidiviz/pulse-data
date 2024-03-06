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

from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view_collector import (
    FederatedCloudSQLTableBigQueryViewCollector,
)
from recidiviz.persistence.database.schema.case_triage.schema import CaseTriageBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.view_registry.deployed_views import (
    CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA,
)


class FederatedCloudSQLTableBigQueryViewCollectorTest(unittest.TestCase):
    """Tests for federated_cloud_sql_table_big_query_view_collector.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-staging"

        test_secrets = {
            # pylint: disable=protected-access
            SQLAlchemyEngineManager._get_cloudsql_instance_id_key(
                schema_type=schema_type,
                secret_prefix_override=None,
            ): f"test-project:us-east2:{schema_type.value}-data"
            for schema_type in SchemaType
        }
        self.get_secret_patcher = mock.patch("recidiviz.utils.secrets.get_secret")

        self.get_secret_patcher.start().side_effect = test_secrets.get

    def tearDown(self) -> None:
        self.get_secret_patcher.stop()
        self.metadata_patcher.stop()

    def test_collect_do_not_crash(self) -> None:
        for schema_type in SchemaType:
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                continue
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            _ = FederatedCloudSQLTableBigQueryViewCollector(
                config
            ).collect_view_builders()

    def test_all_datasets_registered_as_managed(self) -> None:
        for schema_type in SchemaType:
            datasets_for_schema_refresh = set()
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                continue
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            view_builders = FederatedCloudSQLTableBigQueryViewCollector(
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

    def test_collector_case_triage(self) -> None:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.CASE_TRIAGE)
        collector = FederatedCloudSQLTableBigQueryViewCollector(config)
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
