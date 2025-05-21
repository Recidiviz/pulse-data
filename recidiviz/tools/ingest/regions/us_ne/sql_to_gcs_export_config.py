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
"""Configuration module for connecting to the US_NE database via SSH tunnel."""
from io import StringIO
from typing import Any

import attr
import paramiko

from recidiviz.common import attr_validators
from recidiviz.utils.secrets import get_secret

# Bandit flags these constants as hardcoded secrets, but they are not
US_NE_SSH_KEY_SECRET_ID = "us_ne_ssh_key"  # nosec B105
US_NE_SSH_USER_SECRET_ID = "us_ne_ssh_user"  # nosec B105
US_NE_BASTION_HOST_IP_SECRET_ID = "us_ne_vpn_jumpbox_ip"  # nosec B105
US_NE_SQL_SERVER_IP_SECRET_ID = "us_ne_sql_server_ip"  # nosec B105
US_NE_DB_PASSWORD_SECRET_ID = "us_ne_db_password"  # nosec B105
US_NE_DB_USER_SECRET_ID = "us_ne_db_user"  # nosec B105

LOCAL_ADDRESS = "127.0.0.1"
SSH_PORT = 22
DB_PORT = 1433


@attr.define
class UsNeDatabaseConnectionConfigProvider:
    """
    This class provides configuration for connecting to US_NE's SQL Server
    via an SSH tunnel. It retrieves the necessary configuration from secrets
    stored in the recidiviz-ingest-us-ne project."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    _ssh_key: paramiko.RSAKey | None = attr.ib(
        default=None,
        validator=attr.validators.optional(
            attr.validators.instance_of(paramiko.RSAKey)
        ),
    )
    _ssh_user: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    _bastion_host_ip: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    _sql_server_ip: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    _db_user: str | None = attr.ib(default=None, validator=attr_validators.is_opt_str)
    _db_password: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    @property
    def ssh_key(self) -> paramiko.RSAKey | None:
        """Get SSH private key as paramiko RSAKey object."""
        if self._ssh_key is None:
            private_key_str = get_secret(
                secret_id=US_NE_SSH_KEY_SECRET_ID, project_id=self.project_id
            )
            self._ssh_key = paramiko.RSAKey.from_private_key(StringIO(private_key_str))
        return self._ssh_key

    @property
    def ssh_user(self) -> str | None:
        """Get SSH username."""
        if self._ssh_user is None:
            self._ssh_user = get_secret(
                secret_id=US_NE_SSH_USER_SECRET_ID, project_id=self.project_id
            )
        return self._ssh_user

    @property
    def bastion_host_ip(self) -> str | None:
        """Get bastion host IP address."""
        if self._bastion_host_ip is None:
            self._bastion_host_ip = get_secret(
                secret_id=US_NE_BASTION_HOST_IP_SECRET_ID, project_id=self.project_id
            )
        return self._bastion_host_ip

    @property
    def sql_server_ip(self) -> str | None:
        """Get SQL server IP address."""
        if self._sql_server_ip is None:
            self._sql_server_ip = get_secret(
                secret_id=US_NE_SQL_SERVER_IP_SECRET_ID, project_id=self.project_id
            )
        return self._sql_server_ip

    @property
    def db_user(self) -> str | None:
        """Get database username."""
        if self._db_user is None:
            self._db_user = get_secret(
                secret_id=US_NE_DB_USER_SECRET_ID, project_id=self.project_id
            )
        return self._db_user

    @property
    def db_password(self) -> str | None:
        """Get database password."""
        if self._db_password is None:
            self._db_password = get_secret(
                secret_id=US_NE_DB_PASSWORD_SECRET_ID, project_id=self.project_id
            )
        return self._db_password

    @property
    def ssh_tunnel_config(self) -> dict[str, Any]:
        """Get SSH tunnel configuration parameters."""
        return {
            "ssh_address_or_host": (self.bastion_host_ip, SSH_PORT),
            "ssh_username": self.ssh_user,
            "ssh_pkey": self.ssh_key,
            "remote_bind_address": (self.sql_server_ip, DB_PORT),
            "local_bind_address": (LOCAL_ADDRESS, DB_PORT),
        }

    def get_db_connection_config(self, db_name: str) -> dict[str, str | None]:
        """Get database connection configuration parameters."""
        return {
            "server": LOCAL_ADDRESS,
            "database": db_name,
            "user": self.db_user,
            "password": self.db_password,
        }
