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
"""Tests for CloudSqlConnectionMixin"""
from unittest import TestCase
from unittest.mock import create_autospec, patch

from recidiviz.persistence.database.cloud_sql_connection_mixin import (
    CloudSqlConnectionMixin,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class CloudSqlProxyAccessor(CloudSqlConnectionMixin):
    def __init__(self, *, key: SQLAlchemyDatabaseKey) -> None:
        self.key = key

    def write(self, *, with_proxy: bool) -> None:
        with self.get_session(database_key=self.key, with_proxy=with_proxy):
            pass


class CloudSqlProxyMixinTest(TestCase):
    def setUp(self) -> None:
        self.session_factory_patcher = patch(
            "recidiviz.persistence.database.cloud_sql_connection_mixin.SessionFactory"
        )
        self.session_factory_mock = self.session_factory_patcher.start()

    def tearDown(self) -> None:
        self.session_factory_patcher.stop()

    def test_mixin_with_proxy(self) -> None:
        key = create_autospec(SQLAlchemyDatabaseKey)
        accessor = CloudSqlProxyAccessor(key=key)
        accessor.write(with_proxy=True)
        self.session_factory_mock.for_proxy.assert_called_with(
            database_key=key, autocommit=True, secret_prefix_override=None
        )

    def test_mixin_without_proxy(self) -> None:
        key = create_autospec(SQLAlchemyDatabaseKey)
        accessor = CloudSqlProxyAccessor(key=key)
        accessor.write(with_proxy=False)
        self.session_factory_mock.using_database.assert_called_with(
            database_key=key, autocommit=True, secret_prefix_override=None
        )
