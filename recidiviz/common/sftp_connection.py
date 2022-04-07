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
"""A subclass of the pysftp.Connection object"""
import socket
from typing import Optional, Sequence

import paramiko
import pysftp
from paramiko import Transport
from pysftp import CnOpts, ConnectionException


class RecidivizSftpConnection(pysftp.Connection):
    """Subclass of the pysftp.Connection object needed for Recidiviz partner needs."""

    def __init__(  # pylint: disable=super-init-not-called
        self,
        host: str,
        username: Optional[str] = None,
        private_key: Optional[paramiko.PKey] = None,
        password: Optional[str] = None,
        port: int = 22,
        _private_key_pass: Optional[str] = None,
        _ciphers: Optional[Sequence[str]] = None,
        _log: bool = False,
        cnopts: Optional[pysftp.CnOpts] = None,
        default_path: Optional[str] = None,
    ) -> None:
        """A custom __init__ method for our SFTP connections in order to support multiple
        authentication methods."""
        if not username or not password:
            raise ValueError("SFTP username and password required.")
        # starting point for transport.connect options
        self._tconnect = {
            "username": username,
            "password": password,
            "hostkey": None,
            "pkey": None,
        }
        self._cnopts: CnOpts = cnopts or CnOpts()
        self._default_path = default_path
        # check that we have a hostkey to verify
        if self._cnopts.hostkeys is not None:
            self._tconnect["hostkey"] = self._cnopts.get_hostkey(host)  # type: ignore

        self._sftp_live = False
        self._sftp = None
        # Begin the SSH transport.
        self._transport: Optional[Transport] = None
        self._start_transport(host, port)
        self._auth_transport(username, password, private_key)

    def _start_transport(self, host: str, port: int) -> None:
        """This base method is overwritten in order to support disabling certain key
        algorithms introduced by a change in Paramiko."""
        try:
            self._transport = Transport(
                (host, port),
                # A recent change to Paramiko defined a preference order for algorithms that
                # are used to decrypt host keys and private keys. For RSA keys, we have
                # identified that these two algorithms take precedent over ssh-rsa, which is
                # the algorithm preferred by state partners. Therefore, if there are other
                # channel closed errors, we should consider adding algorithms to this list
                # in order to force preference of the algorithms that our hostkey secrets
                # contain by our state partners.
                disabled_algorithms=dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"]),
            )
        except (AttributeError, socket.gaierror) as e:
            # couldn't connect
            raise ConnectionException(host, port) from e

    def _auth_transport(
        self,
        username: str,
        password: str,
        private_key: Optional[paramiko.PKey],
    ) -> None:
        """This method is created to strictly support multiple factors of authentication
        from state partners, namely first public+private key authentication, falling back
        to password authentication."""
        if not self._transport:
            raise ValueError("A transport must be defined in order to authenticate.")
        self._transport.connect()  # type: ignore
        if private_key:
            try:
                self._transport.auth_publickey(username, private_key)
            except Exception:
                # Generally, if a private key authentication method is required, then
                # a password has to be inputted as well as a second factor of auth.
                # This allows paramiko to continue to create the connection by manually
                # sending a message with password credentials forward.
                message = paramiko.Message()
                message.add_byte(paramiko.common.cMSG_USERAUTH_REQUEST)
                message.add_string(username)
                message.add_string("ssh-connection")
                message.add_string("password")
                message.add_boolean(False)
                message.add_string(password)
                # pylint: disable=protected-access
                self._transport._send_message(message)  # type: ignore
        else:
            self._transport.auth_password(username, password)
