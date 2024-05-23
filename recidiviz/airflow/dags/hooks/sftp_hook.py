# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A custom SFTPHook that supports two-factor authenticated SFTP servers."""
import io
import logging
from typing import Any, Dict, Optional

import paramiko
from airflow.providers.sftp.hooks.sftp import SFTPHook
from paramiko.transport import Transport


class RecidivizSFTPHook(SFTPHook):
    """Custom SFTPHook that supports two-factor authenticated SFTP servers."""

    def __init__(
        self,
        ssh_conn_id: str,
        transport_kwargs: Dict[str, Any],
        *args: Any,
        **kwargs: Dict[str, Any]
    ) -> None:
        super().__init__(ssh_conn_id=ssh_conn_id, ssh_hook=None, *args, **kwargs)
        self.conn: Optional[paramiko.SFTPClient] = None
        self._transport_kwargs = transport_kwargs

        # Enable debug logging when creating SFTP connections
        logging.basicConfig(level=logging.DEBUG)
        paramiko_loggers = [
            logging.getLogger("paramiko"),
            logging.getLogger("paramiko.sftp"),
            logging.getLogger("paramiko.transport"),
        ]

        for logger in paramiko_loggers:
            logger.setLevel(logging.DEBUG)

    def get_conn(self) -> paramiko.SFTPClient:
        """Retrieves the SFTP connection. This is overridden from the base class in order
        to support private key and password two-factor authentication."""
        if self.conn is None:
            connection = self.get_connection(self.ssh_conn_id)
            raw_private_key = connection.extra_dejson.get("private_key", None)
            private_key = (
                None
                if not raw_private_key
                else paramiko.RSAKey.from_private_key(io.StringIO(raw_private_key))
            )

            transport = Transport(
                (connection.host, connection.port or 22), **self._transport_kwargs
            )

            try:
                transport.connect()
                if private_key:
                    try:
                        transport.auth_publickey(connection.login, private_key)
                    except Exception:
                        # Generally, if a private key authentication method is required, then
                        # a password has to be inputted as well as a second factor of auth.
                        # This allows paramiko to continue to create the connection by manually
                        # sending a message with password credentials forward.
                        message = paramiko.Message()
                        message.add_byte(paramiko.common.cMSG_USERAUTH_REQUEST)
                        message.add_string(connection.login)
                        message.add_string("ssh-connection")
                        message.add_string("password")
                        message.add_boolean(False)
                        message.add_string(connection.password)
                        # pylint: disable=protected-access
                        transport._send_message(message)  # type: ignore
                else:
                    transport.auth_password(connection.login, connection.password)
                client = paramiko.SFTPClient.from_transport(transport)
            except Exception as e:
                raise ConnectionError(
                    "Encountered an error while trying to establish an sftp connection. "
                    "If you are not sure the cause of this error, see go/sftp-debugging "
                    "for past errors and resolutions"
                ) from e
            if not client:
                raise ValueError("Expected proper SFTP client to be created.")
            self.conn = client
        return self.conn
