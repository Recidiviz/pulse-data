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
"""Tests for the GitHub metrics pull request crawler."""
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Iterator, cast
from unittest import mock

import pytest
from _pytest.logging import LogCaptureFixture
from github import BadCredentialsException, GithubException, UnknownObjectException

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.github.metrics.crawler import (
    SEARCH_WINDOW,
    WATERMARK_MARGIN,
    GitHubMetricsCrawler,
    day_windows,
    search_query_for_updated_window,
)
from recidiviz.github.metrics.graphql_client import GitHubGraphQLClient
from recidiviz.github.metrics.queries import (
    NESTED_CONNECTIONS,
    PR_SEARCH_COUNT_QUERY,
    PR_SEARCH_QUERY,
    REPOSITORY_ACCESS_QUERY,
)
from recidiviz.github.metrics.rows import CrawlContext
from recidiviz.github.metrics.tables import CHILD_TABLE_ADDRESSES, CRAWL_RUNS_ADDRESS

_REPO = "Recidiviz/synthetic-repo"
_NEW_REPO = "Recidiviz/synthetic-new-repo"
_HIDDEN_REPO = "Recidiviz/synthetic-hidden-repo"
_CTX = CrawlContext(
    crawl_id="00000000-0000-0000-0000-000000000001",
    crawled_at=datetime(2026, 11, 5, 3, 0, 0, tzinfo=timezone.utc),
)


def _connection(
    nodes: list[dict[str, Any]], *, total_count: bool = True
) -> dict[str, Any]:
    connection: dict[str, Any] = {
        "pageInfo": {"hasNextPage": False, "endCursor": None},
        "nodes": nodes,
    }
    if total_count:
        connection["totalCount"] = len(nodes)
    return connection


def _file(path: str) -> dict[str, Any]:
    return {"path": path, "additions": 1, "deletions": 0, "changeType": "MODIFIED"}


def _node(number: int, *, updated_at: str) -> dict[str, Any]:
    return {
        "id": f"PR_synthetic_{number}",
        "number": number,
        "title": f"Synthetic PR {number}",
        "state": "OPEN",
        "isDraft": False,
        "createdAt": "2026-10-20T15:00:00Z",
        "updatedAt": updated_at,
        "closedAt": None,
        "mergedAt": None,
        "additions": 1,
        "deletions": 0,
        "changedFiles": 1,
        "baseRefName": "main",
        "headRefName": "synthetic-branch",
        "isCrossRepository": False,
        "author": {"login": "synthetic-user-1", "__typename": "User"},
        "mergedBy": None,
        "mergeCommit": None,
        "labels": _connection([]),
        "commits": _connection([]),
        "reviews": _connection([]),
        "comments": _connection([]),
        "files": _connection([_file("src/synthetic.py")], total_count=False),
        "timelineItems": _connection([]),
    }


def _overflow_query(name: str) -> str:
    [query] = [c.overflow_query for c in NESTED_CONNECTIONS if c.name == name]
    assert query is not None
    return query


class FakeGraphQLClient:
    """Serves scripted issue counts and pages, and records every request."""

    def __init__(
        self,
        *,
        issue_counts: dict[str, int] | None = None,
        search_pages: dict[str, list[list[dict[str, Any]]]] | None = None,
        overflow_pages: dict[str, list[list[dict[str, Any]]]] | None = None,
        access_errors: dict[str, Exception] | None = None,
        hidden_repos: set[str] | None = None,
        search_errors: dict[str, Exception] | None = None,
        search_passes: dict[str, list[list[list[dict[str, Any]]]]] | None = None,
    ) -> None:
        # Every repo passes the access check unless access_errors names an error
        # to raise for it or hidden_repos says GitHub returns a null repository.
        self.access_errors = access_errors or {}
        self.hidden_repos = hidden_repos or set()
        self.access_checks: list[str] = []
        self.issue_counts = issue_counts or {}
        self.search_pages = search_pages or {}
        # A search string here raises its error when paginated.
        self.search_errors = search_errors or {}
        # A search string here serves one entry of pages per search of it, in
        # order, in place of its search_pages entry.
        self.search_passes = search_passes or {}
        self.overflow_pages = overflow_pages or {}
        self.count_queries: list[str] = []
        self.paginated: list[tuple[str, dict[str, Any]]] = []

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if query == REPOSITORY_ACCESS_QUERY:
            repo = f"{variables['owner']}/{variables['name']}"
            self.access_checks.append(repo)
            if repo in self.access_errors:
                raise self.access_errors[repo]
            if repo in self.hidden_repos:
                return {"repository": None}
            return {"repository": {"id": f"R_{variables['name']}"}}
        assert query == PR_SEARCH_COUNT_QUERY
        self.count_queries.append(variables["q"])
        return {"search": {"issueCount": self.issue_counts.get(variables["q"], 0)}}

    def paginate(
        self,
        query: str,
        variables: dict[str, Any],
        connection_path: tuple[str, ...],
    ) -> Iterator[dict[str, Any]]:
        del connection_path
        self.paginated.append((query, variables))
        if query == PR_SEARCH_QUERY and variables["q"] in self.search_errors:
            raise self.search_errors[variables["q"]]
        if query == PR_SEARCH_QUERY and variables["q"] in self.search_passes:
            pages = self.search_passes[variables["q"]].pop(0)
        elif query == PR_SEARCH_QUERY:
            pages = self.search_pages.get(variables["q"], [[]])
        else:
            [connection] = [
                c.name for c in NESTED_CONNECTIONS if c.overflow_query == query
            ]
            pages = self.overflow_pages[connection]
        for i, page in enumerate(pages):
            yield {
                "issueCount": self.issue_counts.get(
                    variables.get("q", ""), sum(len(p) for p in pages)
                ),
                "nodes": page,
                "pageInfo": {
                    "hasNextPage": i < len(pages) - 1,
                    "endCursor": f"c{i + 1}",
                },
            }

    def searched(self) -> list[str]:
        return [v["q"] for q, v in self.paginated if q == PR_SEARCH_QUERY]


class RecordingSink:
    """Records each write in call order."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, list[dict[str, Any]]]] = []

    def write(self, address: BigQueryAddress, rows: list[dict[str, Any]]) -> None:
        self.writes.append((address.table_id, rows))

    def rows(self, table_id: str) -> list[dict[str, Any]]:
        return [row for t, rows in self.writes if t == table_id for row in rows]


# The fake clock's time, which becomes finished_at.
_FINISHED_AT = datetime(2026, 11, 5, 4, 30, tzinfo=timezone.utc)


def _crawler(
    client: FakeGraphQLClient,
    sink: RecordingSink,
    *,
    bq_client: mock.MagicMock | None = None,
    repositories: list[str] | None = None,
) -> GitHubMetricsCrawler:
    return GitHubMetricsCrawler(
        client=cast(GitHubGraphQLClient, client),
        bq_client=bq_client,
        sink=sink,
        repositories=repositories or [_REPO],
        ctx=_CTX,
        now=lambda: _FINISHED_AT,
    )


def _bq_client(
    *,
    max_updated_at: datetime | None = None,
    max_updated_at_by_repo: dict[str, datetime | None] | None = None,
) -> mock.MagicMock:
    """Returns a fake BigQuery client that answers the watermark query with the
    repo's entry in max_updated_at_by_repo, or max_updated_at for any other repo."""

    def run_query_async(**kwargs: Any) -> mock.MagicMock:
        params = {p.name: p.value for p in kwargs["query_parameters"]}
        repo = params["repository_full_name"]
        job = mock.MagicMock()
        job.result.return_value = [
            {"max_updated_at": (max_updated_at_by_repo or {}).get(repo, max_updated_at)}
        ]
        return job

    bq_client = mock.MagicMock()
    bq_client.project_id = "recidiviz-staging"
    bq_client.run_query_async.side_effect = run_query_async
    return bq_client


def _utc(day: int, hour: int) -> datetime:
    return datetime(2026, 11, day, hour, 0, 0, tzinfo=timezone.utc)


# A repo with no rows starts at midnight UTC on _SINCE. Day windows run from it to
# _CTX.crawled_at, 2026-11-05T03:00Z.
_SINCE = date(2026, 11, 4)
_SINCE_DAY_1 = (_utc(4, 0), _utc(5, 0))
_SINCE_DAY_2 = (_utc(5, 0), _utc(5, 3))

# The watermark for a repo whose MAX(updated_at) is 2026-11-04T10:00Z, minus two
# days.
_WATERMARK_DAY_1 = (_utc(2, 10), _utc(3, 10))
_WATERMARK_DAY_2 = (_utc(3, 10), _utc(4, 10))
_WATERMARK_DAY_3 = (_utc(4, 10), _utc(5, 3))


def _updated_query(window: tuple[datetime, datetime], repo: str = _REPO) -> str:
    return search_query_for_updated_window(repo, *window)


def test_search_query_for_updated_window_string() -> None:
    assert (
        search_query_for_updated_window(_REPO, _utc(2, 10), _utc(3, 10))
        == "repo:Recidiviz/synthetic-repo is:pr "
        "updated:2026-11-02T10:00:00Z..2026-11-03T10:00:00Z"
    )


def test_day_windows_cover_range() -> None:
    assert day_windows(_utc(2, 10), _utc(5, 3)) == [
        (_utc(2, 10), _utc(3, 10)),
        (_utc(3, 10), _utc(4, 10)),
        (_utc(4, 10), _utc(5, 3)),
    ]


def test_watermark_margin_covers_a_whole_search_window() -> None:
    # A run that stops partway through a window leaves a watermark inside it, and
    # the next run reads that whole window again only if the margin spans it.
    assert WATERMARK_MARGIN >= SEARCH_WINDOW


def test_crawl_starts_day_windows_from_watermark_minus_two_days() -> None:
    day_1, day_2, day_3 = (
        _updated_query(w)
        for w in (_WATERMARK_DAY_1, _WATERMARK_DAY_2, _WATERMARK_DAY_3)
    )
    client = FakeGraphQLClient(
        issue_counts={day_1: 2, day_2: 2, day_3: 0},
        # PR 2 was updated at the boundary between the two windows, which both
        # inclusive ranges match.
        search_pages={
            day_1: [
                [
                    _node(1, updated_at="2026-11-03T09:00:00Z"),
                    _node(2, updated_at="2026-11-03T10:00:00Z"),
                ]
            ],
            day_2: [
                [
                    _node(2, updated_at="2026-11-03T10:00:00Z"),
                    _node(3, updated_at="2026-11-04T09:00:00Z"),
                ]
            ],
        },
    )
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at=_utc(4, 10))

    _crawler(client, sink, bq_client=bq_client).crawl(_SINCE)

    assert client.count_queries == [day_1, day_2, day_3]
    assert client.searched() == [day_1, day_2]
    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1, 2, 3]


def test_crawl_starts_repo_with_no_rows_at_midnight_utc_on_since(
    caplog: LogCaptureFixture,
) -> None:
    day_1 = _updated_query(_SINCE_DAY_1, _NEW_REPO)
    day_2 = _updated_query(_SINCE_DAY_2, _NEW_REPO)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1},
        search_pages={day_1: [[_node(3, updated_at="2026-11-04T15:00:00Z")]]},
    )
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at_by_repo={_NEW_REPO: None})

    with caplog.at_level(logging.WARNING):
        _crawler(client, sink, bq_client=bq_client, repositories=[_NEW_REPO]).crawl(
            _SINCE
        )

    assert client.count_queries == [day_1, day_2]
    assert client.searched() == [day_1]
    assert [
        (row["repository_full_name"], row["pr_number"])
        for row in sink.rows("pull_requests")
    ] == [(_NEW_REPO, 3)]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_crawl_starts_each_repo_from_its_own_watermark_or_since() -> None:
    repo_day_1, repo_day_2, repo_day_3 = (
        _updated_query(w)
        for w in (_WATERMARK_DAY_1, _WATERMARK_DAY_2, _WATERMARK_DAY_3)
    )
    new_repo_day_1 = _updated_query(_SINCE_DAY_1, _NEW_REPO)
    new_repo_day_2 = _updated_query(_SINCE_DAY_2, _NEW_REPO)
    client = FakeGraphQLClient(
        issue_counts={repo_day_2: 1, new_repo_day_1: 1},
        search_pages={
            repo_day_2: [[_node(1, updated_at="2026-11-04T09:00:00Z")]],
            new_repo_day_1: [[_node(2, updated_at="2026-11-04T15:00:00Z")]],
        },
    )
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at_by_repo={_REPO: _utc(4, 10), _NEW_REPO: None})

    _crawler(client, sink, bq_client=bq_client, repositories=[_REPO, _NEW_REPO]).crawl(
        _SINCE
    )

    assert client.count_queries == [
        repo_day_1,
        repo_day_2,
        repo_day_3,
        new_repo_day_1,
        new_repo_day_2,
    ]
    assert [
        (row["repository_full_name"], row["pr_number"])
        for row in sink.rows("pull_requests")
    ] == [(_REPO, 1), (_NEW_REPO, 2)]
    [row] = sink.rows("crawl_runs")
    assert row["repositories_requested"] == 2
    assert row["repositories_crawled"] == 2
    assert row["pull_requests_crawled"] == 2


def test_crawl_without_bigquery_client_starts_at_since() -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    day_2 = _updated_query(_SINCE_DAY_2)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1, day_2: 1},
        search_pages={
            day_1: [[_node(1, updated_at="2026-11-04T15:00:00Z")]],
            day_2: [[_node(2, updated_at="2026-11-05T01:00:00Z")]],
        },
    )
    sink = RecordingSink()

    _crawler(client, sink, bq_client=None).crawl(_SINCE)

    assert client.count_queries == [day_1, day_2]
    assert client.searched() == [day_1, day_2]
    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1, 2]


def test_crawl_resumes_every_window_from_watermark_of_partly_finished_load() -> None:
    # A first load that stopped after writing pull requests updated through
    # 2026-11-03T12:00Z leaves that as MAX(updated_at). The since date is ignored
    # for a repo with rows.
    client = FakeGraphQLClient()
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at=_utc(3, 12))

    _crawler(client, sink, bq_client=bq_client).crawl(date(2025, 5, 1))

    assert client.count_queries == [
        "repo:Recidiviz/synthetic-repo is:pr "
        "updated:2026-11-01T12:00:00Z..2026-11-02T12:00:00Z",
        "repo:Recidiviz/synthetic-repo is:pr "
        "updated:2026-11-02T12:00:00Z..2026-11-03T12:00:00Z",
        "repo:Recidiviz/synthetic-repo is:pr "
        "updated:2026-11-03T12:00:00Z..2026-11-04T12:00:00Z",
        "repo:Recidiviz/synthetic-repo is:pr "
        "updated:2026-11-04T12:00:00Z..2026-11-05T03:00:00Z",
    ]


def test_crawl_skips_search_when_nothing_updated() -> None:
    client = FakeGraphQLClient()
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at=_utc(4, 10))

    _crawler(client, sink, bq_client=bq_client).crawl(_SINCE)

    assert len(client.count_queries) == 3
    assert not client.paginated
    assert [table_id for table_id, _ in sink.writes] == ["crawl_runs"]


def test_pull_request_updated_after_its_window_is_written_by_the_later_window() -> (
    None
):
    day_1 = _updated_query(_SINCE_DAY_1)
    day_2 = _updated_query(_SINCE_DAY_2)
    client = FakeGraphQLClient(
        issue_counts={day_1: 2, day_2: 1},
        # The day 1 search returns PR 1 although its update time falls on day 2.
        search_pages={
            day_1: [
                [
                    _node(1, updated_at="2026-11-05T01:00:00Z"),
                    _node(2, updated_at="2026-11-04T15:00:00Z"),
                ]
            ],
            day_2: [[_node(1, updated_at="2026-11-05T01:00:00Z")]],
        },
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [2, 1]
    assert [row["pr_number"] for row in sink.rows("files")] == [2, 1]
    assert sink.rows("crawl_runs")[0]["pull_requests_crawled"] == 2


def test_pull_request_updated_after_run_start_is_not_written_or_counted() -> None:
    day_2 = _updated_query(_SINCE_DAY_2)
    client = FakeGraphQLClient(
        issue_counts={day_2: 1},
        search_pages={day_2: [[_node(1, updated_at="2026-11-05T03:00:01Z")]]},
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert client.searched() == [day_2]
    assert [table_id for table_id, _ in sink.writes] == ["crawl_runs"]
    assert sink.rows("crawl_runs")[0]["pull_requests_crawled"] == 0


def test_pull_request_updated_at_window_end_is_written() -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1},
        search_pages={day_1: [[_node(1, updated_at="2026-11-05T00:00:00Z")]]},
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1]


def test_crawl_warns_and_crawls_window_at_search_cap(
    caplog: LogCaptureFixture,
) -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1000},
        search_pages={day_1: [[_node(1, updated_at="2026-11-04T15:00:00Z")]]},
    )
    sink = RecordingSink()

    with caplog.at_level(logging.WARNING):
        _crawler(client, sink).crawl(_SINCE)

    assert client.searched() == [day_1]
    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "2026-11-04T00:00:00" in warnings[0]


def test_overflow_connection_triggers_follow_up_and_clears_flag() -> None:
    node = _node(1, updated_at="2026-11-04T15:00:00Z")
    node["files"]["pageInfo"] = {"hasNextPage": True, "endCursor": "files-c1"}
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1},
        search_pages={day_1: [[node]]},
        overflow_pages={"files": [[_file("src/synthetic_extra.py")]]},
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    overflow_calls = [
        (q, v) for q, v in client.paginated if q == _overflow_query("files")
    ]
    assert overflow_calls == [
        (
            _overflow_query("files"),
            {
                "owner": "Recidiviz",
                "name": "synthetic-repo",
                "number": 1,
                "after": "files-c1",
            },
        )
    ]
    assert sink.rows("pull_requests")[0]["truncated_connections"] == []
    assert [row["path"] for row in sink.rows("files")] == [
        "src/synthetic.py",
        "src/synthetic_extra.py",
    ]


def test_overflow_reviews_then_their_comments() -> None:
    node = _node(1, updated_at="2026-11-04T15:00:00Z")
    node["reviews"]["pageInfo"] = {"hasNextPage": True, "endCursor": "reviews-c1"}
    extra_review = {
        "id": "PRR_synthetic_2",
        "state": "COMMENTED",
        "submittedAt": "2026-11-04T14:00:00Z",
        "body": "",
        "author": {"login": "synthetic-user-2", "__typename": "User"},
        "comments": {
            "totalCount": 11,
            "pageInfo": {"hasNextPage": True, "endCursor": "review-comments-c10"},
            "nodes": [],
        },
    }
    extra_comment = {
        "id": "PRRC_synthetic_11",
        "createdAt": "2026-11-04T14:00:00Z",
        "path": "src/synthetic.py",
        "body": "Synthetic comment.",
        "author": {"login": "synthetic-user-2", "__typename": "User"},
    }
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1},
        search_pages={day_1: [[node]]},
        overflow_pages={
            "reviews": [[extra_review]],
            "review_comments": [[extra_comment]],
        },
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert client.paginated[1:] == [
        (
            _overflow_query("reviews"),
            {
                "owner": "Recidiviz",
                "name": "synthetic-repo",
                "number": 1,
                "after": "reviews-c1",
            },
        ),
        (
            _overflow_query("review_comments"),
            {"reviewId": "PRR_synthetic_2", "after": "review-comments-c10"},
        ),
    ]
    assert [row["comment_id"] for row in sink.rows("review_comments")] == [
        "PRRC_synthetic_11"
    ]
    assert sink.rows("pull_requests")[0]["truncated_connections"] == []


def test_flush_loads_children_before_pull_requests() -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 2},
        search_pages={
            day_1: [
                [
                    _node(1, updated_at="2026-11-04T15:00:00Z"),
                    _node(2, updated_at="2026-11-04T16:00:00Z"),
                ]
            ]
        },
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert [table_id for table_id, _ in sink.writes] == [
        *[address.table_id for address in CHILD_TABLE_ADDRESSES],
        "pull_requests",
        "crawl_runs",
    ]


def test_flush_every_500_prs() -> None:
    nodes = [_node(n, updated_at="2026-11-04T15:00:00Z") for n in range(1, 1002)]
    pages = [nodes[i : i + 20] for i in range(0, len(nodes), 20)]
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(issue_counts={day_1: 1001}, search_pages={day_1: pages})
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    pull_request_loads = [rows for t, rows in sink.writes if t == "pull_requests"]
    assert [len(rows) for rows in pull_request_loads] == [500, 500, 1]


def test_pull_request_repeated_across_pages_is_written_once() -> None:
    # A PR opened mid-crawl shifts later search pages by one, so the PR at a page
    # boundary comes back again on the next page.
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(
        issue_counts={day_1: 3},
        search_pages={
            day_1: [
                [
                    _node(1, updated_at="2026-11-04T15:00:00Z"),
                    _node(2, updated_at="2026-11-04T16:00:00Z"),
                ],
                [
                    _node(2, updated_at="2026-11-04T16:00:00Z"),
                    _node(3, updated_at="2026-11-04T17:00:00Z"),
                ],
            ]
        },
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1, 2, 3]
    assert [row["pr_number"] for row in sink.rows("files")] == [1, 2, 3]


def test_truncated_labels_are_recorded_without_a_follow_up() -> None:
    node = _node(1, updated_at="2026-11-04T15:00:00Z")
    node["labels"]["pageInfo"] = {"hasNextPage": True, "endCursor": "labels-c20"}
    day_1 = _updated_query(_SINCE_DAY_1)
    client = FakeGraphQLClient(issue_counts={day_1: 1}, search_pages={day_1: [[node]]})
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    assert client.searched() == [day_1]
    assert len(client.paginated) == 1
    assert sink.rows("pull_requests")[0]["truncated_connections"] == ["labels"]


def test_crawl_refuses_unreadable_repo_before_any_crawl() -> None:
    # GitHub search reports zero results for a repo the credential cannot see,
    # which would otherwise look like an empty repo.
    client = FakeGraphQLClient(hidden_repos={_HIDDEN_REPO})
    sink = RecordingSink()

    with pytest.raises(ValueError, match=f"cannot read {_HIDDEN_REPO}"):
        _crawler(client, sink, repositories=[_REPO, _HIDDEN_REPO]).crawl(_SINCE)

    assert client.access_checks == [_REPO, _HIDDEN_REPO]
    assert not client.count_queries
    assert not client.paginated
    assert not sink.writes


def test_crawl_refuses_repo_github_cannot_resolve() -> None:
    client = FakeGraphQLClient(
        access_errors={
            _HIDDEN_REPO: UnknownObjectException(
                404,
                {
                    "errors": [
                        {
                            "type": "NOT_FOUND",
                            "message": "Could not resolve to a Repository",
                        }
                    ]
                },
                {},
            )
        }
    )
    sink = RecordingSink()
    bq_client = _bq_client(max_updated_at=_utc(4, 10))

    with pytest.raises(ValueError, match=f"cannot read {_HIDDEN_REPO}"):
        _crawler(
            client, sink, bq_client=bq_client, repositories=[_REPO, _HIDDEN_REPO]
        ).crawl(_SINCE)

    bq_client.run_query_async.assert_not_called()
    assert not client.count_queries
    assert not sink.writes


def test_access_check_names_repo_the_credential_is_forbidden_from() -> None:
    error = GithubException(
        400,
        {"errors": [{"type": "FORBIDDEN", "message": "Resource not accessible"}]},
        {},
    )
    client = FakeGraphQLClient(access_errors={_HIDDEN_REPO: error})

    with pytest.raises(ValueError, match=f"cannot read {_HIDDEN_REPO}"):
        _crawler(client, RecordingSink(), repositories=[_HIDDEN_REPO]).crawl(_SINCE)


@pytest.mark.parametrize(
    "error",
    [
        BadCredentialsException(401, {"message": "Bad credentials"}, {}),
        # The App installation token request, with a wrong private key or app id.
        GithubException(401, {"message": "A JSON web token could not be decoded"}, {}),
        # The App installation token request, with a wrong installation id.
        UnknownObjectException(404, {"message": "Not Found"}, {}),
        GithubException(429, {"message": "Too many requests"}, {}),
        GithubException(
            400,
            {
                "data": None,
                "errors": [
                    {"type": "RATE_LIMITED", "message": "API rate limit exceeded"}
                ],
            },
            {},
        ),
    ],
    ids=[
        "bad_credentials",
        "undecodable_app_jwt",
        "installation_not_found",
        "http_429",
        "graphql_rate_limited",
    ],
)
def test_access_check_lets_errors_that_are_not_about_the_repo_through(
    error: GithubException,
) -> None:
    client = FakeGraphQLClient(access_errors={_REPO: error})

    with pytest.raises(GithubException) as exc_info:
        _crawler(client, RecordingSink()).crawl(_SINCE)

    assert exc_info.value is error


def test_completed_crawl_writes_one_crawl_run_row() -> None:
    repo_day_1 = _updated_query(_SINCE_DAY_1)
    new_repo_day_1 = _updated_query(_SINCE_DAY_1, _NEW_REPO)
    client = FakeGraphQLClient(
        issue_counts={repo_day_1: 2, new_repo_day_1: 1},
        search_pages={
            repo_day_1: [
                [
                    _node(1, updated_at="2026-11-04T15:00:00Z"),
                    _node(2, updated_at="2026-11-04T16:00:00Z"),
                ],
                [_node(2, updated_at="2026-11-04T16:00:00Z")],
            ],
            new_repo_day_1: [[_node(3, updated_at="2026-11-04T17:00:00Z")]],
        },
    )
    sink = RecordingSink()

    _crawler(client, sink, repositories=[_REPO, _NEW_REPO]).crawl(_SINCE)

    assert sink.writes[-1][0] == "crawl_runs"
    assert sink.rows("crawl_runs") == [
        {
            "crawl_id": _CTX.crawl_id,
            "mode": "incremental",
            "started_at": "2026-11-05T03:00:00+00:00",
            "finished_at": "2026-11-05T04:30:00+00:00",
            "repositories_requested": 2,
            "repositories_crawled": 2,
            "pull_requests_crawled": 3,
        }
    ]


def test_run_that_fails_midway_writes_no_crawl_run_row() -> None:
    repo_day_1 = _updated_query(_SINCE_DAY_1)
    new_repo_day_1 = _updated_query(_SINCE_DAY_1, _NEW_REPO)
    client = FakeGraphQLClient(
        issue_counts={repo_day_1: 1, new_repo_day_1: 1},
        search_pages={repo_day_1: [[_node(1, updated_at="2026-11-04T15:00:00Z")]]},
        search_errors={
            new_repo_day_1: GithubException(
                400,
                {"errors": [{"type": "INTERNAL", "message": "Something went wrong"}]},
                {},
            )
        },
    )
    sink = RecordingSink()

    with pytest.raises(GithubException):
        _crawler(client, sink, repositories=[_REPO, _NEW_REPO]).crawl(_SINCE)

    written = [table_id for table_id, _ in sink.writes]
    assert "pull_requests" in written
    assert CRAWL_RUNS_ADDRESS.table_id not in written


def test_search_that_returns_fewer_than_counted_runs_again(
    caplog: LogCaptureFixture,
) -> None:
    # A pull request updated mid-search leaves the results, and the one after it
    # moves up across the page boundary and is never returned.
    day_1 = _updated_query(_SINCE_DAY_1)
    first = _node(1, updated_at="2026-11-04T15:00:00Z")
    missed = _node(2, updated_at="2026-11-04T16:00:00Z")
    client = FakeGraphQLClient(
        issue_counts={day_1: 2},
        search_passes={day_1: [[[first]], [[first, missed]]]},
    )
    sink = RecordingSink()

    with caplog.at_level(logging.WARNING):
        _crawler(client, sink).crawl(_SINCE)

    assert client.searched() == [day_1, day_1]
    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1, 2]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_search_still_short_after_second_pass_warns_and_keeps_going(
    caplog: LogCaptureFixture,
) -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    first = _node(1, updated_at="2026-11-04T15:00:00Z")
    client = FakeGraphQLClient(
        issue_counts={day_1: 2}, search_passes={day_1: [[[first]], [[first]]]}
    )
    sink = RecordingSink()

    with caplog.at_level(logging.WARNING):
        _crawler(client, sink).crawl(_SINCE)

    assert client.searched() == [day_1, day_1]
    assert "counted 2 pull requests but returned 1" in caplog.text
    assert [row["pr_number"] for row in sink.rows("pull_requests")] == [1]
    assert sink.rows("crawl_runs")[0]["pull_requests_crawled"] == 1


def test_rows_from_every_window_of_a_repo_load_together() -> None:
    day_1 = _updated_query(_SINCE_DAY_1)
    day_2 = _updated_query(_SINCE_DAY_2)
    client = FakeGraphQLClient(
        issue_counts={day_1: 1, day_2: 1},
        search_pages={
            day_1: [[_node(1, updated_at="2026-11-04T15:00:00Z")]],
            day_2: [[_node(2, updated_at="2026-11-05T01:00:00Z")]],
        },
    )
    sink = RecordingSink()

    _crawler(client, sink).crawl(_SINCE)

    pull_request_loads = [rows for t, rows in sink.writes if t == "pull_requests"]
    assert [[row["pr_number"] for row in rows] for rows in pull_request_loads] == [
        [1, 2]
    ]


_UPDATED_RANGE_REGEX = re.compile(r"updated:(\S+)\.\.(\S+)$")


class WindowSearchClient:
    """Answers every updated-window search from one list of pull request nodes, by
    their updatedAt, so a second run with a different start still finds them. It
    raises on the search numbered fail_on_search, to stop a run partway."""

    def __init__(
        self, nodes: list[dict[str, Any]], *, fail_on_search: int | None
    ) -> None:
        self._nodes = nodes
        self._fail_on_search = fail_on_search
        self._searches = 0

    def _matches(self, query_string: str) -> list[dict[str, Any]]:
        match = _UPDATED_RANGE_REGEX.search(query_string)
        assert match is not None
        start, end = (datetime.fromisoformat(bound) for bound in match.groups())
        return [
            node
            for node in self._nodes
            if start <= datetime.fromisoformat(node["updatedAt"]) <= end
        ]

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if query == REPOSITORY_ACCESS_QUERY:
            return {"repository": {"id": "R_synthetic"}}
        return {"search": {"issueCount": len(self._matches(variables["q"]))}}

    def paginate(
        self,
        query: str,
        variables: dict[str, Any],
        connection_path: tuple[str, ...],
    ) -> Iterator[dict[str, Any]]:
        del query, connection_path
        self._searches += 1
        if self._searches == self._fail_on_search:
            raise GithubException(
                400,
                {"errors": [{"type": "INTERNAL", "message": "Something went wrong"}]},
                {},
            )
        nodes = self._matches(variables["q"])
        yield {
            "issueCount": len(nodes),
            "nodes": nodes,
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }


@pytest.mark.parametrize("fail_on_search", [1, 2, 3, 4, 5])
def test_run_after_one_that_stopped_partway_reads_every_pull_request(
    fail_on_search: int,
) -> None:
    # One pull request a day for five days, the last on the day of the run.
    nodes = [
        _node(day, updated_at=f"2026-11-{day:02d}T15:00:00Z") for day in range(1, 5)
    ] + [_node(5, updated_at="2026-11-05T01:00:00Z")]
    stopped_sink = RecordingSink()

    with mock.patch(f"{GitHubMetricsCrawler.__module__}.FLUSH_EVERY_PRS", 2):
        with pytest.raises(GithubException):
            GitHubMetricsCrawler(
                client=cast(
                    GitHubGraphQLClient,
                    WindowSearchClient(nodes, fail_on_search=fail_on_search),
                ),
                bq_client=_bq_client(max_updated_at=None),
                sink=stopped_sink,
                repositories=[_REPO],
                ctx=_CTX,
                now=lambda: _FINISHED_AT,
            ).crawl(date(2026, 11, 1))

    written = stopped_sink.rows("pull_requests")
    assert len(written) < len(nodes)
    assert not stopped_sink.rows("crawl_runs")
    # What the watermark query returns for the rows the stopped run wrote.
    max_updated_at = max(
        (datetime.fromisoformat(row["updated_at"]) for row in written), default=None
    )
    next_sink = RecordingSink()

    GitHubMetricsCrawler(
        client=cast(
            GitHubGraphQLClient, WindowSearchClient(nodes, fail_on_search=None)
        ),
        bq_client=_bq_client(max_updated_at=max_updated_at),
        sink=next_sink,
        repositories=[_REPO],
        ctx=_CTX,
        now=lambda: _FINISHED_AT,
    ).crawl(date(2026, 11, 1))

    read = {row["pr_number"] for row in written + next_sink.rows("pull_requests")}
    assert read == {1, 2, 3, 4, 5}
