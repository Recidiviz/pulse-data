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
"""Builds the PyGithub client the engineering metrics crawl reads GitHub with.

The deployed job authenticates as a GitHub App installation, whose tokens last one
hour; PyGithub mints a new one when the current one is about to expire. The
credential comes from, in order:

1. The GITHUB_METRICS_TOKEN environment variable, for local runs only. This never
   reads Secret Manager, and the Cloud Run job does not set it.
2. The three github_metrics_crawl_app_* secrets, used to mint installation tokens.

There is no secret for a personal access token. A long-lived token in Secret Manager
would replace the App's short-lived ones without notice, and GitHub returns a
token owner's unsubmitted draft reviews to that owner only.
"""
import os

from github import Auth, Github, GithubRetry

from recidiviz.utils.secrets import get_secret

GITHUB_METRICS_TOKEN_ENV = "GITHUB_METRICS_TOKEN"  # nosec
APP_ID_SECRET_ID = "github_metrics_crawl_app_id"  # nosec
APP_INSTALLATION_ID_SECRET_ID = "github_metrics_crawl_app_installation_id"  # nosec
APP_PRIVATE_KEY_SECRET_ID = "github_metrics_crawl_app_private_key"  # nosec

# Retries per request for a 5xx response, a network error or a 403 rate limit.
_MAX_RETRIES = 4

# Seconds the wait between retries of a 5xx response or a network error grows by.
# The first retry is immediate and the next three wait 4, 8 and 16 seconds, which
# outlasts a short GitHub outage. PyGithub's default of 0 sends every retry at
# once.
_RETRY_BACKOFF_FACTOR = 2


def _github_client(auth: Auth.Auth) -> Github:
    return Github(
        auth=auth,
        retry=GithubRetry(total=_MAX_RETRIES, backoff_factor=_RETRY_BACKOFF_FACTOR),
    )


def _stripped(value: str | None) -> str | None:
    """Returns value without leading or trailing whitespace, such as the newline a
    secret added through echo and gcloud ends in."""
    return value.strip() if value is not None else None


def _header_safe(value: str | None, source: str) -> str | None:
    """Returns the stripped value, or raises ValueError naming source, but not the
    value, if the value still contains whitespace or a control character. Such a
    value cannot go in an Authorization header, and requests quotes the whole
    header in the error it raises."""
    stripped = _stripped(value)
    if stripped and any(c.isspace() or not c.isprintable() for c in stripped):
        raise ValueError(
            f"[{source}] holds a value with whitespace or control characters. "
            "Replace it with the token or id alone, on one line."
        )
    return stripped


def _missing_credential_error(detail: str) -> ValueError:
    return ValueError(
        f"{detail} Set the {GITHUB_METRICS_TOKEN_ENV} environment variable for a "
        f"local run, or give all three of secrets "
        f"[{APP_ID_SECRET_ID}], [{APP_INSTALLATION_ID_SECRET_ID}] and "
        f"[{APP_PRIVATE_KEY_SECRET_ID}] values in this project."
    )


def resolve_github_client() -> Github:
    """Returns a PyGithub client that authenticates with the token in the
    environment variable if it is set, else as the GitHub App installation the
    secrets name.

    Raises ValueError, naming the environment variable and the secret ids but no
    secret value, when no credential resolves, only some of the App secrets have
    values, or a token or id holds whitespace or control characters.
    """
    if env_token := _header_safe(
        os.environ.get(GITHUB_METRICS_TOKEN_ENV), GITHUB_METRICS_TOKEN_ENV
    ):
        return _github_client(Auth.Token(env_token))

    app_secrets = {
        APP_ID_SECRET_ID: _header_safe(get_secret(APP_ID_SECRET_ID), APP_ID_SECRET_ID),
        APP_INSTALLATION_ID_SECRET_ID: _header_safe(
            get_secret(APP_INSTALLATION_ID_SECRET_ID), APP_INSTALLATION_ID_SECRET_ID
        ),
        # A PEM key has newlines inside it, and it signs the App's JWT rather than
        # going in a header.
        APP_PRIVATE_KEY_SECRET_ID: _stripped(get_secret(APP_PRIVATE_KEY_SECRET_ID)),
    }
    app_id = app_secrets[APP_ID_SECRET_ID]
    installation_id = app_secrets[APP_INSTALLATION_ID_SECRET_ID]
    private_key = app_secrets[APP_PRIVATE_KEY_SECRET_ID]
    if not app_id or not installation_id or not private_key:
        missing = [secret_id for secret_id, value in app_secrets.items() if not value]
        if len(missing) == len(app_secrets):
            raise _missing_credential_error("No GitHub credential for the crawl.")
        raise _missing_credential_error(
            f"Only some GitHub App secrets have values; missing {missing}."
        )
    if not installation_id.isdigit():
        raise _missing_credential_error(
            f"Secret [{APP_INSTALLATION_ID_SECRET_ID}] must hold a numeric id."
        )
    return _github_client(
        Auth.AppAuth(app_id, private_key).get_installation_auth(int(installation_id))
    )
