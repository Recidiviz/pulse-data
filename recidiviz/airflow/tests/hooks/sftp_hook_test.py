# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for RecidivizSFTPHook"""
# pylint: disable=protected-access
import base64
import hashlib
import io
import json
import logging
import socket
import unittest
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch

import paramiko
from airflow.models import Connection
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from recidiviz.airflow.dags.hooks.sftp_hook import (
    SFTP_SOCKET_PACKET_READ_TIMEOUT,
    RecidivizSFTPHook,
)

_HOOK_MODULE = "recidiviz.airflow.dags.hooks.sftp_hook"
_CONN_ID = "us_xx_sftp_conn_id"
_HOST = "sftp.us-xx.example.com"
_PORT = 2222
_LOGIN = "test_user"
_PASSWORD = "test_password"
_TRANSPORT_KWARGS = {"disabled_algorithms": {"pubkeys": ["rsa-sha2-512"]}}


def _serialize_private_key(key: paramiko.RSAKey) -> str:
    buffer = io.StringIO()
    key.write_private_key(buffer)
    return buffer.getvalue()


def _host_key_extra(key: paramiko.PKey) -> str:
    return f"{key.get_name()} {key.get_base64()}"


def _openssh_sha256_fingerprint(key: paramiko.PKey) -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode().rstrip("=")


def _generate_ed25519_host_key() -> paramiko.Ed25519Key:
    public_bytes = (
        Ed25519PrivateKey.generate()
        .public_key()
        .public_bytes(Encoding.Raw, PublicFormat.Raw)
    )
    message = paramiko.Message()
    message.add_string("ssh-ed25519")
    message.add_string(public_bytes)
    return paramiko.Ed25519Key(data=message.asbytes())


class RecidivizSFTPHookTest(unittest.TestCase):
    """Tests for RecidivizSFTPHook"""

    private_key: paramiko.RSAKey
    server_host_key: paramiko.RSAKey
    other_host_key: paramiko.RSAKey

    @classmethod
    def setUpClass(cls) -> None:
        cls.private_key = paramiko.RSAKey.generate(bits=2048)
        cls.server_host_key = paramiko.RSAKey.generate(bits=2048)
        cls.other_host_key = paramiko.RSAKey.generate(bits=2048)

    def setUp(self) -> None:
        self.transport_patcher = patch(f"{_HOOK_MODULE}.Transport")
        self.mock_transport_cls = self.transport_patcher.start()
        self.mock_transport = self.mock_transport_cls.return_value
        self.mock_transport.auth_publickey.return_value = []

        self.from_transport_patcher = patch.object(
            paramiko.SFTPClient, "from_transport"
        )
        self.mock_from_transport = self.from_transport_patcher.start()
        self.mock_client = MagicMock()
        self.mock_from_transport.return_value = self.mock_client

        self.connection = self._build_connection(
            extra={"private_key": _serialize_private_key(self.private_key)}
        )
        self.get_connection_patcher = patch.object(
            RecidivizSFTPHook, "get_connection", side_effect=self._get_connection
        )
        self.mock_get_connection = self.get_connection_patcher.start()

    def tearDown(self) -> None:
        self.transport_patcher.stop()
        self.from_transport_patcher.stop()
        self.get_connection_patcher.stop()

    def _get_connection(self, conn_id: str) -> Connection:
        self.assertEqual(_CONN_ID, conn_id)
        return self.connection

    @staticmethod
    def _build_connection(
        *, extra: dict[str, Any], port: int | None = _PORT
    ) -> Connection:
        connection = create_autospec(Connection, instance=True)
        connection.conn_id = _CONN_ID
        connection.host = _HOST
        connection.port = port
        connection.login = _LOGIN
        connection.password = _PASSWORD
        connection.extra = json.dumps(extra)
        connection.extra_dejson = extra
        return connection

    @staticmethod
    def _build_hook() -> RecidivizSFTPHook:
        return RecidivizSFTPHook(
            ssh_conn_id=_CONN_ID, transport_kwargs=_TRANSPORT_KWARGS
        )

    def test_init(self) -> None:
        hook = self._build_hook()

        self.assertIsNone(hook.conn)
        self.assertEqual(_CONN_ID, hook.ssh_conn_id)
        self.assertEqual(
            _TRANSPORT_KWARGS,
            hook._transport_kwargs,
        )
        for logger_name in ["paramiko", "paramiko.sftp", "paramiko.transport"]:
            self.assertEqual(logging.DEBUG, logging.getLogger(logger_name).level)
        self.mock_transport_cls.assert_not_called()

    def test_get_conn_private_key_with_password_second_factor(self) -> None:
        self.mock_transport.auth_publickey.return_value = ["password"]

        client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.mock_transport.auth_publickey.assert_called_once()
        login, key = self.mock_transport.auth_publickey.call_args.args
        self.assertEqual(_LOGIN, login)
        self.assertEqual(self.private_key, key)
        self.mock_transport.auth_password.assert_not_called()

        self.mock_transport._send_message.assert_called_once()
        sent_message: paramiko.Message = (
            self.mock_transport._send_message.call_args.args[0]
        )
        message = paramiko.Message(sent_message.asbytes())
        self.assertEqual(paramiko.common.cMSG_USERAUTH_REQUEST, message.get_byte())
        self.assertEqual(_LOGIN, message.get_text())
        self.assertEqual("ssh-connection", message.get_text())
        self.assertEqual("password", message.get_text())
        self.assertFalse(message.get_boolean())
        self.assertEqual(_PASSWORD, message.get_text())
        self.assertEqual(b"", message.get_remainder())

        self.mock_from_transport.assert_called_once_with(self.mock_transport)

    def test_get_conn_private_key_no_next_steps(self) -> None:
        self.mock_transport.auth_publickey.return_value = []

        client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.mock_transport.auth_publickey.assert_called_once()
        self.mock_transport._send_message.assert_not_called()
        self.mock_transport.auth_password.assert_not_called()

    def test_get_conn_private_key_unknown_next_steps(self) -> None:
        self.mock_transport.auth_publickey.return_value = ["keyboard-interactive"]

        with self.assertRaises(ConnectionError) as e:
            self._build_hook().get_conn()

        self.assertIsInstance(e.exception.__cause__, ValueError)
        self.assertIn("keyboard-interactive", str(e.exception.__cause__))
        self.mock_transport._send_message.assert_not_called()
        self.mock_from_transport.assert_not_called()

    def test_get_conn_private_key_multiple_next_steps(self) -> None:
        self.mock_transport.auth_publickey.return_value = [
            "password",
            "keyboard-interactive",
        ]

        with self.assertRaises(ConnectionError) as e:
            self._build_hook().get_conn()

        self.assertIsInstance(e.exception.__cause__, ValueError)
        self.mock_transport._send_message.assert_not_called()

    def test_get_conn_password_only(self) -> None:
        self.connection = self._build_connection(extra={})

        client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.mock_transport.auth_password.assert_called_once_with(_LOGIN, _PASSWORD)
        self.mock_transport.auth_publickey.assert_not_called()
        self.mock_transport._send_message.assert_not_called()

    def test_get_conn_empty_private_key_uses_password(self) -> None:
        self.connection = self._build_connection(extra={"private_key": ""})

        self._build_hook().get_conn()

        self.mock_transport.auth_password.assert_called_once_with(_LOGIN, _PASSWORD)
        self.mock_transport.auth_publickey.assert_not_called()

    def test_get_conn_wraps_connect_error(self) -> None:
        self.mock_transport.connect.side_effect = paramiko.SSHException("boom")

        with self.assertRaises(ConnectionError) as e:
            self._build_hook().get_conn()

        self.assertIn("go/sftp-debugging", str(e.exception))
        self.assertIsInstance(e.exception.__cause__, paramiko.SSHException)
        self.mock_transport.auth_publickey.assert_not_called()

    def test_get_conn_wraps_auth_error(self) -> None:
        self.mock_transport.auth_publickey.side_effect = (
            paramiko.AuthenticationException("denied")
        )

        with self.assertRaises(ConnectionError) as e:
            self._build_hook().get_conn()

        self.assertIsInstance(e.exception.__cause__, paramiko.AuthenticationException)
        self.mock_from_transport.assert_not_called()

    def test_get_conn_no_client_created(self) -> None:
        self.mock_from_transport.return_value = None

        hook = self._build_hook()
        with self.assertRaisesRegex(
            ValueError, "Expected proper SFTP client to be created."
        ):
            hook.get_conn()

        self.assertIsNone(hook.conn)

    def test_get_conn_sets_socket_timeout(self) -> None:
        self._build_hook().get_conn()

        self.mock_client.sock.settimeout.assert_called_once_with(
            SFTP_SOCKET_PACKET_READ_TIMEOUT
        )

    def test_get_conn_caches_connection(self) -> None:
        hook = self._build_hook()

        first_client = hook.get_conn()
        second_client = hook.get_conn()

        self.assertIs(first_client, second_client)
        self.assertIs(self.mock_client, hook.conn)
        self.mock_transport_cls.assert_called_once()
        self.mock_transport.connect.assert_called_once()
        self.mock_from_transport.assert_called_once()

    def test_get_conn_passes_host_port_and_transport_kwargs(self) -> None:
        self._build_hook().get_conn()

        self.mock_transport_cls.assert_called_once_with(
            (_HOST, _PORT), **_TRANSPORT_KWARGS
        )

    def test_get_conn_defaults_port_to_22(self) -> None:
        self.connection = self._build_connection(extra={}, port=None)

        self._build_hook().get_conn()

        self.mock_transport_cls.assert_called_once_with(
            (_HOST, 22), **_TRANSPORT_KWARGS
        )

    def test_get_conn_does_not_enforce_configured_host_key(self) -> None:
        self.mock_transport.get_remote_server_key.return_value = self.server_host_key
        self.connection = self._build_connection(
            extra={
                "private_key": _serialize_private_key(self.private_key),
                "host_key": _host_key_extra(self.server_host_key),
            }
        )

        self._build_hook().get_conn()

        self.mock_transport.connect.assert_called_once_with()
        self.mock_transport.get_remote_server_key.assert_called_once_with()

    def test_get_conn_without_host_key_warns(self) -> None:
        with self.assertLogs(level=logging.WARNING) as logs:
            client = self._build_hook().get_conn()

        self.assertEqual(1, len(logs.output))
        self.assertIn("No host_key configured for connection", logs.output[0])
        self.assertIn(_CONN_ID, logs.output[0])
        self.assertIs(self.mock_client, client)
        self.mock_transport.connect.assert_called_once_with()
        self.mock_transport.get_remote_server_key.assert_not_called()

    def _set_up_real_transport(
        self, *, presented_host_key: paramiko.PKey
    ) -> list[paramiko.Transport]:
        """Replaces the mocked Transport with a real paramiko Transport whose key
        exchange is stubbed to present |presented_host_key|. Returns the list that
        each Transport the hook builds is appended to.
        """
        local_socket, remote_socket = socket.socketpair()
        self.addCleanup(local_socket.close)
        self.addCleanup(remote_socket.close)
        transports: list[paramiko.Transport] = []

        def build_transport(
            _address: tuple[str, int], **kwargs: Any
        ) -> paramiko.Transport:
            transport = paramiko.Transport(local_socket, **kwargs)
            transports.append(transport)
            return transport

        self.mock_transport_cls.side_effect = build_transport
        return_values_by_attribute: dict[str, Any] = {
            "start_client": None,
            "get_remote_server_key": presented_host_key,
            "auth_publickey": [],
        }
        for attribute, return_value in return_values_by_attribute.items():
            patcher = patch.object(
                paramiko.Transport, attribute, return_value=return_value
            )
            patcher.start()
            self.addCleanup(patcher.stop)
        return transports

    def test_get_conn_matching_host_key_connects_without_warning(self) -> None:
        transports = self._set_up_real_transport(
            presented_host_key=self.server_host_key
        )
        self.connection = self._build_connection(
            extra={
                "private_key": _serialize_private_key(self.private_key),
                "host_key": _host_key_extra(self.server_host_key),
            }
        )

        with self.assertNoLogs(level=logging.WARNING):
            client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.assertEqual(
            ("rsa-sha2-512", "rsa-sha2-256", "ssh-rsa"),
            transports[0].get_security_options().key_types[:3],
        )

    def test_get_conn_mismatched_host_key_warns_and_connects(self) -> None:
        self._set_up_real_transport(presented_host_key=self.other_host_key)
        self.connection = self._build_connection(
            extra={
                "private_key": _serialize_private_key(self.private_key),
                "host_key": _host_key_extra(self.server_host_key),
            }
        )

        with self.assertLogs(level=logging.WARNING) as logs:
            client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.assertEqual(1, len(logs.output))
        self.assertIn("SFTP host key mismatch for connection", logs.output[0])
        self.assertIn(_CONN_ID, logs.output[0])
        self.assertIn(
            f"[ssh-rsa {_openssh_sha256_fingerprint(self.server_host_key)}]",
            logs.output[0],
        )
        self.assertIn(
            f"[ssh-rsa {_openssh_sha256_fingerprint(self.other_host_key)}]",
            logs.output[0],
        )

    def test_get_conn_mismatched_host_key_type_warns_and_connects(self) -> None:
        configured_host_key = _generate_ed25519_host_key()
        transports = self._set_up_real_transport(
            presented_host_key=self.server_host_key
        )
        self.connection = self._build_connection(
            extra={
                "private_key": _serialize_private_key(self.private_key),
                "host_key": _host_key_extra(configured_host_key),
            }
        )

        with self.assertLogs(level=logging.WARNING) as logs:
            client = self._build_hook().get_conn()

        self.assertIs(self.mock_client, client)
        self.assertIn("SFTP host key mismatch for connection", logs.output[0])
        self.assertIn(
            f"[ssh-ed25519 {_openssh_sha256_fingerprint(configured_host_key)}]",
            logs.output[0],
        )
        self.assertIn(
            f"[ssh-rsa {_openssh_sha256_fingerprint(self.server_host_key)}]",
            logs.output[0],
        )
        key_types = transports[0].get_security_options().key_types
        self.assertEqual("ssh-ed25519", key_types[0])
        self.assertIn("rsa-sha2-256", key_types)
