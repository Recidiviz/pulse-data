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
"""Tests for building the PyGithub client the GitHub metrics crawl uses."""
import os
from typing import Any, Iterator
from unittest import mock

import pytest
from github import Auth, Github

from recidiviz.github.metrics.credentials import (
    APP_ID_SECRET_ID,
    APP_INSTALLATION_ID_SECRET_ID,
    APP_PRIVATE_KEY_SECRET_ID,
    GITHUB_METRICS_TOKEN_ENV,
    resolve_github_client,
)
from recidiviz.utils.types import assert_type

_FAKE_ENV_TOKEN = "fake-env-token-for-tests"  # nosec B105
_FAKE_PRIVATE_KEY = "fake-private-key-for-tests"  # nosec B105
_FAKE_APP_ID = "12345"
_FAKE_INSTALLATION_ID = "67890"
_APP_SECRETS = {
    APP_ID_SECRET_ID: _FAKE_APP_ID,
    APP_INSTALLATION_ID_SECRET_ID: _FAKE_INSTALLATION_ID,
    APP_PRIVATE_KEY_SECRET_ID: _FAKE_PRIVATE_KEY,
}


@pytest.fixture(autouse=True, name="environ")
def environ_fixture() -> Iterator[None]:
    with mock.patch.dict(os.environ):
        os.environ.pop(GITHUB_METRICS_TOKEN_ENV, None)
        yield


@pytest.fixture(name="secrets")
def secrets_fixture() -> Iterator[dict[str, str]]:
    values: dict[str, str] = {}
    with mock.patch(
        "recidiviz.github.metrics.credentials.get_secret",
        side_effect=values.get,
    ):
        yield values


def _recorded_app_auth() -> Any:
    """Returns a patch that records the arguments Auth.AppAuth is built with and
    still builds it, because PyGithub checks the type of the object."""
    return mock.patch.object(
        Auth.AppAuth, "__init__", autospec=True, side_effect=Auth.AppAuth.__init__
    )


def _token_auth(client: Github) -> Auth.Token:
    return assert_type(client.requester.auth, Auth.Token)


def _installation_auth(client: Github) -> Auth.AppInstallationAuth:
    return assert_type(client.requester.auth, Auth.AppInstallationAuth)


def test_env_var_wins_over_secrets(secrets: dict[str, str]) -> None:
    os.environ[GITHUB_METRICS_TOKEN_ENV] = _FAKE_ENV_TOKEN
    secrets.update(_APP_SECRETS)

    assert _token_auth(resolve_github_client()).token == _FAKE_ENV_TOKEN


def test_pat_secret_is_never_read(secrets: dict[str, str]) -> None:
    # A value in a secret with the old personal-token name must not replace the App.
    secrets["github_metrics_crawl_pat"] = "fake-pat-for-tests"  # nosec B105
    secrets.update(_APP_SECRETS)

    _installation_auth(resolve_github_client())


def test_app_secrets_build_installation_auth(secrets: dict[str, str]) -> None:
    secrets.update(_APP_SECRETS)

    auth = _installation_auth(resolve_github_client())

    assert auth.app_id == _FAKE_APP_ID
    assert auth.installation_id == 67890


def test_client_retries_four_times_with_backoff(secrets: dict[str, str]) -> None:
    del secrets
    os.environ[GITHUB_METRICS_TOKEN_ENV] = _FAKE_ENV_TOKEN

    retry = resolve_github_client().requester.kwargs["retry"]

    assert retry.total == 4
    assert retry.backoff_factor == 2


@pytest.mark.parametrize("missing_secret_id", list(_APP_SECRETS))
def test_partial_app_secrets_raise_naming_ids(
    secrets: dict[str, str], missing_secret_id: str
) -> None:
    secrets.update(_APP_SECRETS)
    del secrets[missing_secret_id]

    with pytest.raises(ValueError) as exc_info:
        resolve_github_client()

    message = str(exc_info.value)
    for name in [
        GITHUB_METRICS_TOKEN_ENV,
        APP_ID_SECRET_ID,
        APP_INSTALLATION_ID_SECRET_ID,
        APP_PRIVATE_KEY_SECRET_ID,
    ]:
        assert name in message
    for value in _APP_SECRETS.values():
        assert value not in message


def test_no_credential_raises(secrets: dict[str, str]) -> None:
    del secrets  # Every secret is empty.

    with pytest.raises(ValueError, match=GITHUB_METRICS_TOKEN_ENV):
        resolve_github_client()


def test_non_integer_installation_id_raises_without_value(
    secrets: dict[str, str],
) -> None:
    secrets.update(_APP_SECRETS)
    secrets[APP_INSTALLATION_ID_SECRET_ID] = "not-a-number"

    with pytest.raises(ValueError) as exc_info:
        resolve_github_client()

    assert APP_INSTALLATION_ID_SECRET_ID in str(exc_info.value)
    assert "not-a-number" not in str(exc_info.value)


def test_env_var_with_trailing_newline_is_stripped(secrets: dict[str, str]) -> None:
    del secrets
    os.environ[GITHUB_METRICS_TOKEN_ENV] = f"{_FAKE_ENV_TOKEN}\n"

    assert _token_auth(resolve_github_client()).token == _FAKE_ENV_TOKEN


def test_app_secrets_with_whitespace_are_stripped(secrets: dict[str, str]) -> None:
    secrets.update(
        {
            APP_ID_SECRET_ID: f" {_FAKE_APP_ID}\n",
            APP_INSTALLATION_ID_SECRET_ID: f"{_FAKE_INSTALLATION_ID}\n",
            APP_PRIVATE_KEY_SECRET_ID: f"{_FAKE_PRIVATE_KEY}\n",
        }
    )
    with _recorded_app_auth() as app_auth:
        auth = _installation_auth(resolve_github_client())

    assert auth.installation_id == 67890
    app_auth.assert_called_once_with(mock.ANY, _FAKE_APP_ID, _FAKE_PRIVATE_KEY)


@pytest.mark.parametrize("source", [GITHUB_METRICS_TOKEN_ENV, APP_ID_SECRET_ID])
@pytest.mark.parametrize(
    "bad_value", ["fake-token\nfor-tests", "fake token", "fake\x07"]
)
def test_value_with_interior_whitespace_is_rejected_without_value(
    secrets: dict[str, str], source: str, bad_value: str
) -> None:
    if source == GITHUB_METRICS_TOKEN_ENV:
        os.environ[GITHUB_METRICS_TOKEN_ENV] = bad_value
    else:
        secrets.update(_APP_SECRETS)
        secrets[source] = bad_value

    with pytest.raises(ValueError) as exc_info:
        resolve_github_client()

    assert f"[{source}]" in str(exc_info.value)
    assert bad_value not in str(exc_info.value)


def test_private_key_keeps_its_interior_newlines(secrets: dict[str, str]) -> None:
    multi_line_key = "fake-private-key\nfor-tests\n"
    secrets.update({**_APP_SECRETS, APP_PRIVATE_KEY_SECRET_ID: multi_line_key})
    with _recorded_app_auth() as app_auth:
        resolve_github_client()

    app_auth.assert_called_once_with(
        mock.ANY, _FAKE_APP_ID, "fake-private-key\nfor-tests"
    )
