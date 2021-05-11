# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for sqlalchemy_engine_manager.py"""
from unittest import mock
from unittest.case import TestCase
from mock import call, patch
import sqlalchemy

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils, sqlalchemy_database_key
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


class SQLAlchemyEngineManagerTest(TestCase):
    """Tests"""

    def tearDown(self) -> None:
        SQLAlchemyEngineManager.teardown_engines()

    @patch(
        f"{sqlalchemy_database_key.__name__}.get_existing_direct_ingest_states",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    @patch("sqlalchemy.create_engine")
    @patch("recidiviz.utils.environment.in_gcp_production")
    @patch("recidiviz.utils.environment.in_gcp")
    @patch("recidiviz.utils.secrets.get_secret")
    def testInitEngines_usesCorrectIsolationLevels(
        self,
        mock_get_secret: mock.MagicMock,
        mock_in_gcp: mock.MagicMock,
        mock_in_production: mock.MagicMock,
        mock_create_engine: mock.MagicMock,
        mock_get_states: mock.MagicMock,
    ) -> None:
        # Arrange
        mock_in_gcp.return_value = True
        mock_in_production.return_value = True
        # Pretend all secret values are just the key suffixed with '_value'
        mock_get_secret.side_effect = lambda key: f"{key}_value"

        # Act
        SQLAlchemyEngineManager.attempt_init_engines_for_server(
            set(schema_utils.SchemaType)
        )

        # Assert
        self.assertEqual(
            mock_create_engine.call_args_list,
            [
                call(
                    "postgresql://sqlalchemy_db_user_value:sqlalchemy_db_password_value@/postgres"
                    "?host=/cloudsql/sqlalchemy_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/postgres"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://operations_db_user_value:operations_db_password_value@/postgres"
                    "?host=/cloudsql/operations_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://justice_counts_db_user_value:justice_counts_db_password_value@/"
                    "postgres?host=/cloudsql/justice_counts_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://case_triage_db_user_value:case_triage_db_password_value@/"
                    "postgres?host=/cloudsql/case_triage_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_xx_primary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_ww_primary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_xx_secondary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_ww_secondary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
            ],
        )
        mock_get_states.assert_called()

    @patch(
        f"{sqlalchemy_database_key.__name__}.get_existing_direct_ingest_states",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    @patch("sqlalchemy.create_engine")
    @patch("recidiviz.utils.environment.in_gcp_staging")
    @patch("recidiviz.utils.environment.in_gcp")
    @patch("recidiviz.utils.secrets.get_secret")
    def testInitEngines_usesCorrectIsolationLevelsInStaging(
        self,
        mock_get_secret: mock.MagicMock,
        mock_in_gcp: mock.MagicMock,
        mock_in_staging: mock.MagicMock,
        mock_create_engine: mock.MagicMock,
        mock_get_states: mock.MagicMock,
    ) -> None:
        # Arrange
        mock_in_gcp.return_value = True
        mock_in_staging.return_value = True
        # Pretend all secret values are just the key suffixed with '_value'
        mock_get_secret.side_effect = lambda key: f"{key}_value"

        # Act
        SQLAlchemyEngineManager.attempt_init_engines_for_server(
            set(schema_utils.SchemaType)
        )

        # Assert
        self.assertEqual(
            mock_create_engine.call_args_list,
            [
                call(
                    "postgresql://sqlalchemy_db_user_value:sqlalchemy_db_password_value@/postgres"
                    "?host=/cloudsql/sqlalchemy_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/postgres"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://operations_db_user_value:operations_db_password_value@/postgres"
                    "?host=/cloudsql/operations_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://justice_counts_db_user_value:justice_counts_db_password_value@/"
                    "postgres?host=/cloudsql/justice_counts_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://case_triage_db_user_value:case_triage_db_password_value@/"
                    "postgres?host=/cloudsql/case_triage_cloudsql_instance_id_value",
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_xx_primary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_ww_primary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_xx_secondary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    "postgresql://state_db_user_value:state_db_password_value@/us_ww_secondary"
                    "?host=/cloudsql/state_cloudsql_instance_id_value",
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
            ],
        )
        mock_get_states.assert_called()

    @patch("recidiviz.utils.secrets.get_secret")
    def testGetAllStrippedCloudSqlInstanceIds(
        self, mock_secrets: mock.MagicMock
    ) -> None:
        # Arrange
        mock_secrets.side_effect = [
            "project:region:111",
            "project:region:222",
            "project:region:333",
            "project:region:444",
            "project:region:555",
        ]

        # Act
        ids = SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids()

        # Assert
        self.assertEqual(ids, ["111", "222", "333", "444", "555"])
        mock_secrets.assert_has_calls(
            [
                mock.call("sqlalchemy_cloudsql_instance_id"),
                mock.call("state_cloudsql_instance_id"),
                mock.call("operations_cloudsql_instance_id"),
                mock.call("justice_counts_cloudsql_instance_id"),
                mock.call("case_triage_cloudsql_instance_id"),
            ],
            any_order=True,
        )

    @patch("recidiviz.utils.secrets.get_secret")
    def testGetStrippedCloudSqlInstanceId(self, mock_secrets: mock.MagicMock) -> None:
        # Arrange
        mock_secrets.side_effect = [
            "project:region:111",
        ]

        # Act
        instance_id = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(
            schema_type=SchemaType.OPERATIONS
        )

        # Assert
        self.assertEqual(instance_id, "111")
        mock_secrets.assert_called_with("operations_cloudsql_instance_id")

    @patch("recidiviz.utils.secrets.get_secret")
    def testGetAllStrippedCloudSqlRegion(self, mock_secrets: mock.MagicMock) -> None:
        # Arrange
        mock_secrets.side_effect = [
            "project:us-central1:111",
        ]

        # Act
        region = SQLAlchemyEngineManager.get_cloudsql_instance_region(
            schema_type=SchemaType.OPERATIONS
        )

        # Assert
        self.assertEqual(region, "us-central1")
        mock_secrets.assert_called_with("operations_cloudsql_instance_id")
