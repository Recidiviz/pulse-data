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
"""Client for the GitHub GraphQL API, used by the engineering metrics crawl.

It sends queries through a PyGithub Github client, whose default GithubRetry
retries 5xx responses and network errors and waits out 403 rate limits. On top
of that, this client waits and retries when GitHub reports RATE_LIMITED in a
response body or returns a 429, pauses before the hourly budget runs out, and
follows pagination cursors.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

import requests
from github import Github, GithubException

logger = logging.getLogger(__name__)

# When a response reports fewer points than this left in the hourly budget, the
# client sleeps until the budget resets, so the next query never hits the limit.
RATE_LIMIT_FLOOR = 200

# Waits per query for a rate limit that GithubRetry does not handle: a
# RATE_LIMITED error in the response body, or a 429 without a Retry-After header.
_MAX_RATE_LIMIT_WAITS = 3

# Added to every wait for a rate-limit reset, so the next request lands after
# GitHub's clock has passed resetAt.
_RESET_PADDING_SECONDS = 1.0

# Wait used when a rate-limited response gives no time to wait until. GitHub asks
# clients to wait at least a minute after a secondary rate limit.
_DEFAULT_RATE_LIMIT_WAIT_SECONDS = 60.0

_GRAPHQL_RATE_LIMITED_ERROR_TYPE = "RATE_LIMITED"


def _parse_github_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _is_rate_limited(e: GithubException) -> bool:
    """Returns whether e is a rate limit this client should wait out: a 429, or
    the 400 PyGithub raises for a body whose errors are all RATE_LIMITED."""
    if e.status == 429:
        return True
    if e.status != 400 or not isinstance(e.data, dict):
        return False
    errors = e.data.get("errors")
    if not errors:
        return False
    return all(
        error.get("type") == _GRAPHQL_RATE_LIMITED_ERROR_TYPE for error in errors
    )


class GitHubGraphQLClient:
    """Runs queries against the GitHub GraphQL API.

    Every query must select rateLimit { cost remaining resetAt }, which the client
    reads to log the cost of each query and to pace itself under the hourly limit.
    """

    def __init__(
        self,
        github_client: Github,
        *,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        """
        Args:
            github_client: The PyGithub client that sends each query. Its auth
                supplies the token for every request, so an AppInstallationAuth
                refreshes the token mid-run.
            sleep: Called with a number of seconds whenever the client waits on a
                rate limit.
            now: Returns the current time as a timezone-aware datetime.
        """
        self._github_client = github_client
        self._sleep = sleep
        self._now = now

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Runs one query and returns its data dict.

        Raises GithubException if the response has any errors other than
        RATE_LIMITED. A RATE_LIMITED error or a 429 waits as long as GitHub asks
        and retries, up to _MAX_RATE_LIMIT_WAITS waits per query in all.
        """
        if "rateLimit" not in query:
            raise ValueError(
                "GitHub GraphQL queries must select rateLimit { cost remaining resetAt }"
            )

        rate_limit_waits_left = _MAX_RATE_LIMIT_WAITS
        while True:
            try:
                data = self._post(query, variables)
            except GithubException as e:
                if not _is_rate_limited(e) or rate_limit_waits_left == 0:
                    raise
                rate_limit_waits_left -= 1
                self._wait_for_rate_limit(e)
                continue
            self._log_cost_and_pace(data["rateLimit"])
            return data

    def paginate(
        self,
        query: str,
        variables: dict[str, Any],
        connection_path: tuple[str, ...],
    ) -> Iterator[dict[str, Any]]:
        """Yields each page of one connection, following its end cursor.

        Each yielded value is the connection object itself, with its nodes,
        pageInfo, and any sibling fields such as issueCount. The query must
        declare an $after: String variable for the cursor.

        Args:
            query: The GraphQL query, which must select rateLimit.
            variables: Variables for every page. An after value, if present, is
                the cursor to start from; the client sets it for later pages.
            connection_path: Field names from the data dict down to the
                connection, for example ("search",) or
                ("repository", "pullRequest", "files").
        """
        cursor: str | None = variables.get("after")
        while True:
            data = self.query(query, {**variables, "after": cursor})
            connection: dict[str, Any] = data
            for field in connection_path:
                connection = connection[field]
            yield connection
            page_info = connection["pageInfo"]
            if not page_info["hasNextPage"]:
                return
            cursor = page_info["endCursor"]

    def _post(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Sends one query and returns its data dict, raising GithubException for
        an error status or any GraphQL errors."""
        try:
            _, body = self._github_client.requester.graphql_query(query, variables)
        except requests.exceptions.InvalidHeader:
            # requests quotes the whole header, token included, in this error, so
            # neither its message nor the exception itself goes into the chain.
            raise ValueError(
                "The GitHub credential contains characters that are not allowed in "
                "an HTTP header"
            ) from None
        return body["data"]

    def _log_cost_and_pace(self, rate_limit: dict[str, Any]) -> None:
        """Logs the query's cost, and sleeps until the hourly budget resets when
        fewer than RATE_LIMIT_FLOOR points remain."""
        logger.info(
            "GitHub GraphQL query cost %s, %s points remaining",
            rate_limit["cost"],
            rate_limit["remaining"],
        )
        if rate_limit["remaining"] < RATE_LIMIT_FLOOR:
            reset_at = _parse_github_timestamp(rate_limit["resetAt"])
            logger.info("Pausing until the GitHub rate limit resets at %s", reset_at)
            self._sleep(self._seconds_until(reset_at))

    def _wait_for_rate_limit(self, e: GithubException) -> None:
        """Sleeps until the rateLimit block's resetAt, else until
        X-RateLimit-Reset when no points remain, else a minute."""
        body_data = e.data.get("data") if isinstance(e.data, dict) else None
        rate_limit = body_data.get("rateLimit") if body_data else None
        # PyGithub lower-cases response header names.
        headers = e.headers or {}
        if rate_limit is not None:
            wait_seconds = self._seconds_until(
                _parse_github_timestamp(rate_limit["resetAt"])
            )
        # GitHub sends these headers on every response; a 429 without them, or
        # with points remaining, is a secondary rate limit.
        elif headers.get("x-ratelimit-remaining") == "0":
            wait_seconds = self._seconds_until(
                datetime.fromtimestamp(
                    int(headers["x-ratelimit-reset"]), tz=timezone.utc
                )
            )
        else:
            wait_seconds = _DEFAULT_RATE_LIMIT_WAIT_SECONDS
        logger.warning(
            "GitHub rate limit hit (status %s); sleeping %.0f seconds",
            e.status,
            wait_seconds,
        )
        self._sleep(wait_seconds)

    def _seconds_until(self, reset_at: datetime) -> float:
        return max(
            (reset_at - self._now()).total_seconds() + _RESET_PADDING_SECONDS, 0.0
        )
