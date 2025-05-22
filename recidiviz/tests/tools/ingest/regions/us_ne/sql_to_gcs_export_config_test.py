# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the DatabaseConnectionConfigProvider class."""
import unittest
from unittest.mock import MagicMock, patch

import paramiko

from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config import (
    UsNeDatabaseConnectionConfigProvider,
)
from recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_tasks import (
    UsNeDatabaseName,
)


class TestDatabaseConnectionConfigProvider(unittest.TestCase):
    """Tests for the DatabaseConnectionConfigProvider class."""

    @patch("recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config.get_secret")
    @patch(
        "recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config.paramiko.RSAKey.from_private_key"
    )
    def test_ssh_tunnel_config(
        self, mock_from_private_key: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.side_effect = lambda secret_id, project_id: {
            "us_ne_ssh_key": "mock_ssh_key",
            "us_ne_ssh_user": "mock_ssh_user",
            "us_ne_vpn_jumpbox_ip": "192.168.1.1",
            "us_ne_sql_server_ip": "192.168.1.2",
        }[secret_id]
        mock_ssh_key = MagicMock(spec=paramiko.RSAKey)
        mock_from_private_key.return_value = mock_ssh_key

        config_provider = UsNeDatabaseConnectionConfigProvider(
            project_id="test_project",
        )

        expected_config = {
            "ssh_address_or_host": ("192.168.1.1", 22),
            "ssh_username": "mock_ssh_user",
            "ssh_pkey": mock_ssh_key,
            "remote_bind_address": ("192.168.1.2", 1433),
            "local_bind_address": ("127.0.0.1", 1433),
        }
        self.assertEqual(config_provider.ssh_tunnel_config, expected_config)

    @patch("recidiviz.tools.ingest.regions.us_ne.sql_to_gcs_export_config.get_secret")
    def test_db_connection_config(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.side_effect = lambda secret_id, project_id: {
            "us_ne_db_user": "mock_db_user",
            "us_ne_db_password": "mock_db_password",
        }[secret_id]

        config_provider = UsNeDatabaseConnectionConfigProvider(
            project_id="test_project",
        )

        expected_config = {
            "server": "127.0.0.1",
            "database": "DCS_MVS",
            "user": "mock_db_user",
            "password": "mock_db_password",
        }
        self.assertEqual(
            config_provider.get_db_connection_config(db_name=UsNeDatabaseName.DCS_MVS),
            expected_config,
        )
