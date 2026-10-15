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

from recidiviz.utils.types import assert_type

# Timeout set on the SFTPClient's socket (paramiko.Channel class). This timeout is used
# each time the underlying socket is reading or writing a single packet (which is, at most,
# 2^15 bytes, or 0.03 mb), not for the entire file. This is set as a backstop against
# flaky and/or silently dropped SFTP reads that can hang indefinitely.
SFTP_SOCKET_PACKET_READ_TIMEOUT = 180.0  # 3 minutes, in seconds


def _host_key_algorithms(host_key: paramiko.PKey) -> list[str]:
    """Returns the host key algorithms that negotiate |host_key|'s type, matching
    the ones paramiko's Transport.connect(hostkey=...) would allow.
    """
    if isinstance(host_key, paramiko.RSAKey):
        return ["rsa-sha2-512", "rsa-sha2-256", "ssh-rsa"]
    return [host_key.get_name()]


class RecidivizSFTPHook(SFTPHook):
    """Custom SFTPHook that supports two-factor authenticated SFTP servers."""

    def __init__(
        self,
        *,
        ssh_conn_id: str,
        transport_kwargs: Dict[str, Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        super().__init__(ssh_conn_id=ssh_conn_id, ssh_hook=None, **kwargs)
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

    def get_conn(self) -> paramiko.SFTPClient:  # type: ignore[override]
        """Retrieves the SFTP connection. This is overridden from the base class in order
        to support private key and password two-factor authentication. The server's host
        key is compared against the connection's `host_key` extra, which the base
        SSHHook parses into self.host_key. A missing or mismatched key is logged but
        does not block the connection.
        """
        if self.conn is None:
            connection = self.get_connection(assert_type(self.ssh_conn_id, str))
            raw_private_key = connection.extra_dejson.get("private_key", None)
            private_key = (
                None
                if not raw_private_key
                else paramiko.RSAKey.from_private_key(io.StringIO(raw_private_key))
            )

            transport = Transport(
                (connection.host, connection.port or 22), **self._transport_kwargs
            )

            if self.host_key is None:
                logging.warning(
                    "No host_key configured for connection [%s]; the SFTP server's "
                    "host key will not be verified.",
                    self.ssh_conn_id,
                )
            else:
                security_options = transport.get_security_options()
                configured_key_types = _host_key_algorithms(self.host_key)
                security_options.key_types = configured_key_types + [
                    key_type
                    for key_type in security_options.key_types
                    if key_type not in configured_key_types
                ]

            try:
                transport.connect()
                if self.host_key is not None:
                    self._log_host_key_audit(transport.get_remote_server_key())
                if private_key:
                    logging.info("starting public key auth flow...")
                    next_steps = transport.auth_publickey(connection.login, private_key)
                    if next_steps:
                        if len(next_steps) == 1 and next_steps[0] == "password":
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
                            raise ValueError(
                                f"Unknown next authentication steps after public key auth: {next_steps}"
                            )
                else:
                    logging.info("starting password auth flow...")
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
            client.sock.settimeout(SFTP_SOCKET_PACKET_READ_TIMEOUT)
            self.conn = client
        return self.conn

    def _log_host_key_audit(self, server_key: paramiko.PKey) -> None:
        configured_key = assert_type(self.host_key, paramiko.PKey)
        if (
            server_key.get_name() == configured_key.get_name()
            and server_key.asbytes() == configured_key.asbytes()
        ):
            logging.info(
                "SFTP host key for connection [%s] matches the configured host_key "
                "[%s %s].",
                self.ssh_conn_id,
                configured_key.get_name(),
                configured_key.fingerprint,
            )
            return
        logging.warning(
            "SFTP host key mismatch for connection [%s]: configured host_key is "
            "[%s %s] but the server presented [%s %s]. Connecting anyway because "
            "host keys are not enforced yet.",
            self.ssh_conn_id,
            configured_key.get_name(),
            configured_key.fingerprint,
            server_key.get_name(),
            server_key.fingerprint,
        )
