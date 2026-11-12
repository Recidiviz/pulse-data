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
"""Tests for the GitHub GraphQL client."""
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

import pytest
from github import Auth, Github, GithubException

from recidiviz.github.metrics.graphql_client import GitHubGraphQLClient

_NOW = datetime(2026, 11, 5, 12, 0, 0, tzinfo=timezone.utc)
_QUERY = """
query($q: String!, $after: String) {
  rateLimit { cost remaining resetAt }
  search(query: $q, type: ISSUE, first: 20, after: $after) {
    nodes { ... on PullRequest { number } }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def _rate_limit(remaining: int = 4000, reset_in_seconds: int = 3600) -> dict[str, Any]:
    reset_at = _NOW + timedelta(seconds=reset_in_seconds)
    return {
        "cost": 1,
        "remaining": remaining,
        "resetAt": reset_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _page(numbers: list[int], end_cursor: str, has_next_page: bool) -> dict[str, Any]:
    return {
        "data": {
            "rateLimit": _rate_limit(),
            "search": {
                "nodes": [{"number": n} for n in numbers],
                "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
            },
        }
    }


def _client(
    results: list[dict[str, Any] | Exception],
) -> tuple[GitHubGraphQLClient, mock.MagicMock, mock.MagicMock]:
    """Returns a client whose graphql_query calls return each response body, or
    raise each exception, in turn."""
    github_client = mock.MagicMock()
    graphql_query = github_client.requester.graphql_query
    graphql_query.side_effect = [
        result if isinstance(result, Exception) else ({}, result) for result in results
    ]
    sleep = mock.MagicMock()
    client = GitHubGraphQLClient(github_client, sleep=sleep, now=lambda: _NOW)
    return client, graphql_query, sleep


def test_paginate_follows_end_cursor_and_stops() -> None:
    client, graphql_query, _ = _client(
        [
            _page([1, 2], "c1", has_next_page=True),
            _page([3], "c2", has_next_page=False),
        ]
    )

    pages = list(
        client.paginate(_QUERY, {"q": "repo:Recidiviz/synthetic-repo"}, ("search",))
    )

    assert [[node["number"] for node in page["nodes"]] for page in pages] == [
        [1, 2],
        [3],
    ]
    assert graphql_query.call_args_list == [
        mock.call(_QUERY, {"q": "repo:Recidiviz/synthetic-repo", "after": None}),
        mock.call(_QUERY, {"q": "repo:Recidiviz/synthetic-repo", "after": "c1"}),
    ]


def test_paginate_starts_from_after_variable() -> None:
    client, graphql_query, _ = _client([_page([3], "c2", False)])

    list(client.paginate(_QUERY, {"q": "y", "after": "c1"}, ("search",)))

    graphql_query.assert_called_once_with(_QUERY, {"q": "y", "after": "c1"})


def test_query_rejects_query_without_rate_limit() -> None:
    client, graphql_query, _ = _client([])

    with pytest.raises(ValueError, match="rateLimit"):
        client.query("query { viewer { login } }", {})
    graphql_query.assert_not_called()


def test_query_raises_on_non_rate_limit_errors_without_retry() -> None:
    error = GithubException(
        400, {"errors": [{"type": "FORBIDDEN", "message": "partial"}]}, {}
    )
    client, graphql_query, sleep = _client([error])

    with pytest.raises(GithubException, match="partial"):
        client.query(_QUERY, {"q": "y"})

    assert graphql_query.call_count == 1
    sleep.assert_not_called()


def test_query_sleeps_until_reset_on_rate_limited_then_retries() -> None:
    rate_limited = GithubException(
        400,
        {
            "data": {"rateLimit": _rate_limit(remaining=0, reset_in_seconds=90)},
            "errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}],
        },
        {},
    )
    success = _page([7], "c1", False)
    client, graphql_query, sleep = _client([rate_limited, success])

    result = client.query(_QUERY, {"q": "y"})

    assert result == success["data"]
    assert graphql_query.call_count == 2
    sleep.assert_called_once_with(91.0)


def test_query_reads_reset_header_when_rate_limited_body_has_no_data() -> None:
    reset_epoch = int((_NOW + timedelta(seconds=120)).timestamp())
    rate_limited = GithubException(
        400,
        {"errors": [{"type": "RATE_LIMITED", "message": "limit"}]},
        {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(reset_epoch)},
    )
    client, _, sleep = _client([rate_limited, _page([1], "c1", False)])

    client.query(_QUERY, {"q": "y"})

    sleep.assert_called_once_with(121.0)


def test_429_without_headers_sleeps_a_minute_then_retries() -> None:
    client, graphql_query, sleep = _client(
        [GithubException(429, {}, {}), _page([1], "c1", False)]
    )

    client.query(_QUERY, {"q": "y"})

    sleep.assert_called_once_with(60.0)
    assert graphql_query.call_count == 2


def test_query_raises_when_still_rate_limited_after_three_waits() -> None:
    rate_limited = GithubException(
        400,
        {
            "data": {"rateLimit": _rate_limit(remaining=0, reset_in_seconds=30)},
            "errors": [{"type": "RATE_LIMITED", "message": "still limited"}],
        },
        {},
    )
    client, graphql_query, sleep = _client(
        [rate_limited, GithubException(429, {}, {}), rate_limited, rate_limited]
    )

    with pytest.raises(GithubException, match="still limited"):
        client.query(_QUERY, {"q": "y"})

    assert graphql_query.call_count == 4
    assert sleep.call_count == 3


def test_query_sleeps_when_remaining_under_floor() -> None:
    body = _page([1], "c1", False)
    body["data"]["rateLimit"] = _rate_limit(remaining=199, reset_in_seconds=300)
    client, _, sleep = _client([body])

    result = client.query(_QUERY, {"q": "y"})

    assert result == body["data"]
    sleep.assert_called_once_with(301.0)


def test_query_does_not_sleep_at_floor() -> None:
    body = _page([1], "c1", False)
    body["data"]["rateLimit"] = _rate_limit(remaining=200)
    client, _, sleep = _client([body])

    client.query(_QUERY, {"q": "y"})

    sleep.assert_not_called()


def test_invalid_header_raises_without_chained_token() -> None:
    fake_token = "synthetic\nsecret-token"  # nosec B105
    # requests rejects the header before any network call, so this real Github
    # client never reaches GitHub.
    client = GitHubGraphQLClient(Github(auth=Auth.Token(fake_token)))

    with pytest.raises(ValueError) as exc_info:
        client.query(_QUERY, {"q": "y"})

    assert "secret-token" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__
