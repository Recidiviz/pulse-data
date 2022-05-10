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
import itertools
from typing import List
from unittest import mock
from unittest.case import TestCase

import sqlalchemy
from mock import call, patch
from sqlalchemy.engine import URL

from recidiviz import server_config
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


class SQLAlchemyEngineManagerTest(TestCase):
    """Tests"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id = self.project_id_patcher.start()
        self.mock_project_id.return_value = "recidiviz-456"

    def tearDown(self) -> None:
        SQLAlchemyEngineManager.teardown_engines()
        self.project_id_patcher.stop()

    def _all_db_keys(self) -> List[SQLAlchemyDatabaseKey]:
        return list(
            itertools.chain.from_iterable(
                server_config.database_keys_for_schema_type(schema_type)
                for schema_type in schema_utils.SchemaType
            )
        )

    @patch(
        f"{server_config.__name__}.get_existing_direct_ingest_states",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    @patch(
        f"{server_config.__name__}.get_pathways_enabled_states",
        return_value=[StateCode.US_XX.value, StateCode.US_WW.value],
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
        _mock_pathways_enabled: mock.MagicMock,
        mock_get_states: mock.MagicMock,
    ) -> None:
        # Arrange
        mock_in_gcp.return_value = True
        mock_in_production.return_value = True
        self.mock_project_id.return_value = "recidiviz-123"
        # Pretend all secret values are just the key suffixed with '_value'
        mock_get_secret.side_effect = lambda key: f"{key}_value"

        # Act
        SQLAlchemyEngineManager.attempt_init_engines_for_databases(self._all_db_keys())

        # Assert
        self.assertCountEqual(
            mock_create_engine.call_args_list,
            [
                call(
                    URL.create(
                        drivername="postgresql",
                        username="jails_v2_db_user_value",
                        password="jails_v2_db_password_value",
                        port=5432,
                        database="postgres",
                        query={"host": "/cloudsql/jails_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="operations_v2_db_user_value",
                        password="operations_v2_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/operations_v2_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level=None,
                    poolclass=None,
                    pool_size=2,
                    max_overflow=5,
                    pool_timeout=15,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="justice_counts_db_user_value",
                        password="justice_counts_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/justice_counts_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="case_triage_db_user_value",
                        password="case_triage_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/case_triage_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_xx_primary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_ww_primary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_xx_secondary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_ww_secondary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="pathways_db_user_value",
                        password="pathways_db_password_value",
                        port=5432,
                        database="us_xx",
                        query={"host": "/cloudsql/pathways_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="pathways_db_user_value",
                        password="pathways_db_password_value",
                        port=5432,
                        database="us_ww",
                        query={"host": "/cloudsql/pathways_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
            ],
        )
        mock_get_states.assert_called()

    @patch(
        f"{server_config.__name__}.get_existing_direct_ingest_states",
        return_value=[StateCode.US_XX, StateCode.US_WW],
    )
    @patch(
        f"{server_config.__name__}.get_pathways_enabled_states",
        return_value=[StateCode.US_XX.value, StateCode.US_WW.value],
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
        _mock_pathways_enabled: mock.MagicMock,
        mock_get_states: mock.MagicMock,
    ) -> None:
        # Arrange
        mock_in_gcp.return_value = True
        mock_in_staging.return_value = True
        self.mock_project_id.return_value = "recidiviz-staging"
        # Pretend all secret values are just the key suffixed with '_value'
        mock_get_secret.side_effect = lambda key: f"{key}_value"

        # Act
        SQLAlchemyEngineManager.attempt_init_engines_for_databases(self._all_db_keys())

        # Assert
        self.assertCountEqual(
            mock_create_engine.call_args_list,
            [
                call(
                    URL.create(
                        drivername="postgresql",
                        username="jails_v2_db_user_value",
                        password="jails_v2_db_password_value",
                        port=5432,
                        database="postgres",
                        query={"host": "/cloudsql/jails_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="operations_v2_db_user_value",
                        password="operations_v2_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/operations_v2_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level=None,
                    poolclass=None,
                    pool_size=2,
                    max_overflow=5,
                    pool_timeout=15,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="justice_counts_db_user_value",
                        password="justice_counts_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/justice_counts_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="case_triage_db_user_value",
                        password="case_triage_db_password_value",
                        port=5432,
                        database="postgres",
                        query={
                            "host": "/cloudsql/case_triage_cloudsql_instance_id_value"
                        },
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_xx_primary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_ww_primary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_xx_secondary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="state_v2_db_user_value",
                        password="state_v2_db_password_value",
                        port=5432,
                        database="us_ww_secondary",
                        query={"host": "/cloudsql/state_v2_cloudsql_instance_id_value"},
                    ),
                    isolation_level="SERIALIZABLE",
                    poolclass=sqlalchemy.pool.NullPool,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="pathways_db_user_value",
                        password="pathways_db_password_value",
                        port=5432,
                        database="us_xx",
                        query={"host": "/cloudsql/pathways_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
                    echo_pool=True,
                    pool_recycle=600,
                ),
                call(
                    URL.create(
                        drivername="postgresql",
                        username="pathways_db_user_value",
                        password="pathways_db_password_value",
                        port=5432,
                        database="us_ww",
                        query={"host": "/cloudsql/pathways_cloudsql_instance_id_value"},
                    ),
                    isolation_level=None,
                    poolclass=None,
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
            "project:region:666",
        ]

        # Act
        ids = SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids()

        # Assert
        self.assertEqual(ids, ["111", "222", "333", "444", "555", "666"])
        mock_secrets.assert_has_calls(
            [
                mock.call("jails_v2_cloudsql_instance_id"),
                mock.call("state_v2_cloudsql_instance_id"),
                mock.call("operations_v2_cloudsql_instance_id"),
                mock.call("justice_counts_cloudsql_instance_id"),
                mock.call("case_triage_cloudsql_instance_id"),
                mock.call("pathways_cloudsql_instance_id"),
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
        mock_secrets.assert_called_with("operations_v2_cloudsql_instance_id")

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
        mock_secrets.assert_called_with("operations_v2_cloudsql_instance_id")
