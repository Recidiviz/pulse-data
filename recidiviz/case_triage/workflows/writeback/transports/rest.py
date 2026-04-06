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
"""REST transport for sending HTTP requests to external state systems."""
import abc
import base64
import logging
import time

import attr
import requests

from recidiviz.common.http import HttpMethod
from recidiviz.utils.secrets import get_secret


class AuthStrategy(abc.ABC):
    """Produces auth headers from a credential string."""

    @abc.abstractmethod
    def headers(self, credential: str) -> dict[str, str]:
        ...


class BasicAuth(AuthStrategy):
    """HTTP Basic auth: ``Authorization: Basic base64(credential)``."""

    def headers(self, credential: str) -> dict[str, str]:
        encoded = base64.b64encode(credential.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}


class TokenHeaderAuth(AuthStrategy):
    """Custom header token auth, e.g. ``Recidiviz-Credential-Token: key``."""

    def __init__(self, header_name: str) -> None:
        self.header_name = header_name

    def headers(self, credential: str) -> dict[str, str]:
        return {self.header_name: credential}


@attr.s(frozen=True)
class RestTransportConfig:
    """Configuration for resolving secrets and making HTTP requests."""

    # Human-readable label for the external system, used only in log messages
    # and error strings (e.g. "TN TOMIS", "ND Elite").
    system_name: str = attr.ib()

    # Name of a secret in Google Secret Manager (in the current deployed
    # project) whose value is the credential string used for authentication.
    # How the credential is applied to requests is determined by
    # |auth_strategy|.
    credential_secret: str = attr.ib()

    # Name of a secret in Google Secret Manager (in the current deployed
    # project) whose value is the production URL to send requests to.
    url_secret: str = attr.ib()

    # Name of a secret in Google Secret Manager (in the current deployed
    # project) whose value is a test/staging URL. Used instead of |url_secret|
    # when RestTransport is constructed with ``use_test_url=True``, e.g. for
    # dry-run or staging writeback calls.
    test_url_secret: str = attr.ib()

    # Strategy that turns the resolved |credential_secret| value into HTTP
    # headers (e.g. Basic auth, custom token header).
    auth_strategy: AuthStrategy = attr.ib()

    # HTTP method used for requests. Defaults to PUT.
    http_method: HttpMethod = attr.ib(default=HttpMethod.PUT)

    # Timeout in seconds for each outbound request. Defaults to 360.
    timeout_seconds: int = attr.ib(default=360)


class RestTransport:
    """Resolves secrets and sends HTTP requests to an external system.

    Secrets are resolved once at construction. Call :meth:`send` one or more
    times to issue requests (e.g. once per page for TN contact notes).
    """

    def __init__(self, config: RestTransportConfig, *, use_test_url: bool) -> None:
        self._config = config
        url_secret = config.test_url_secret if use_test_url else config.url_secret
        url = get_secret(url_secret)
        credential = get_secret(config.credential_secret)

        if url is None or credential is None:
            logging.error("Unable to get secrets for %s", config.system_name)
            raise EnvironmentError(f"Unable to get secrets for {config.system_name}")

        self._url = url
        self._headers = {
            "Content-Type": "application/json",
            **config.auth_strategy.headers(credential),
        }

    @property
    def system_name(self) -> str:
        return self._config.system_name

    def send(self, body: str, *, log_context: str | None = None) -> None:
        """Send a request. Raises on non-200 or connection error.

        Args:
            body: JSON string request body.
            log_context: Optional extra context for log messages
                (e.g. ``"page 3"``).
        """
        context_suffix = f" for {log_context}" if log_context else ""
        start_time = time.perf_counter()

        try:
            response = requests.request(
                method=self._config.http_method.value,
                url=self._url,
                headers=self._headers,
                data=body,
                timeout=self._config.timeout_seconds,
            )
            if response.status_code != requests.codes.ok:
                response.raise_for_status()
        except requests.exceptions.HTTPError:
            duration = round(time.perf_counter() - start_time, 2)
            logging.error(
                "Request to %s%s failed with code %s in %s seconds: %s",
                self._config.system_name,
                context_suffix,
                response.status_code,
                duration,
                response.text,
            )
            raise
        except Exception as e:
            duration = round(time.perf_counter() - start_time, 2)
            logging.error(
                "Request to %s%s failed in %s seconds with error: %s",
                self._config.system_name,
                context_suffix,
                duration,
                e,
            )
            raise

        duration = round(time.perf_counter() - start_time, 2)
        logging.info(
            "Request to %s%s completed with status code %s in %s seconds",
            self._config.system_name,
            context_suffix,
            response.status_code,
            duration,
        )
