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

from unittest.case import TestCase
from mock import call, patch

from recidiviz.persistence.database.sqlalchemy_engine_manager import \
    SQLAlchemyEngineManager


class SQLAlchemyEngineManagerTest(TestCase):
    """Tests"""
    def tearDown(self):
        SQLAlchemyEngineManager.teardown_engines()

    @patch('sqlalchemy.create_engine')
    @patch('recidiviz.environment.in_gae_production')
    @patch('recidiviz.environment.in_gae')
    @patch.object(SQLAlchemyEngineManager, '_get_server_postgres_instance_url', lambda **_: 'path')
    def testInitEngines_usesCorrectIsolationLevels(self, mock_in_gae, mock_in_production, mock_create_engine):
        # Arrange
        mock_in_gae.return_value = True
        mock_in_production.return_value = True

        # Act
        SQLAlchemyEngineManager.init_engines_for_server_postgres_instances()

        # Assert
        assert mock_create_engine.call_args_list == [
            call('path', isolation_level=None, pool_recycle=600),
            call('path', isolation_level='SERIALIZABLE', pool_recycle=600),
            call('path', isolation_level=None, pool_recycle=600),
        ]

    @patch('sqlalchemy.create_engine')
    @patch('recidiviz.environment.in_gae_staging')
    @patch('recidiviz.environment.in_gae')
    @patch.object(SQLAlchemyEngineManager, '_get_server_postgres_instance_url', lambda **_: 'path')
    def testInitEngines_usesCorrectIsolationLevelsInStaging(self, mock_in_gae, mock_in_staging, mock_create_engine):
        # Arrange
        mock_in_gae.return_value = True
        mock_in_staging.return_value = True

        # Act
        SQLAlchemyEngineManager.init_engines_for_server_postgres_instances()

        # Assert
        assert mock_create_engine.call_args_list == [
            call('path', isolation_level=None, pool_recycle=600),
            call('path', isolation_level='SERIALIZABLE', pool_recycle=600),
            call('path', isolation_level=None, pool_recycle=600),
        ]

    @patch('recidiviz.utils.secrets.get_secret')
    def testGetAllStrippedCloudSqlInstanceIds_returnsOnlyConfiguredIds(self, mock_secrets):
        # Arrange
        mock_secrets.side_effect = ['project:zone:123', 'project:zone:456', 'project:zone:789']

        # Act
        ids = SQLAlchemyEngineManager.get_all_stripped_cloudsql_instance_ids()

        # Assert
        assert ids == ['123', '456', '789']
