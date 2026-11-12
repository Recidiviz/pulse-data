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
"""Crawls pull request history for a list of repos from the GitHub GraphQL API
into the github_api_raw tables.

The crawl finds pull requests through the GraphQL search connection, because the
pullRequests connection cannot filter by date. It searches by update time, one
day per search, for example "repo:Recidiviz/pulse-data is:pr
updated:2026-11-02T10:00:00Z..2026-11-03T10:00:00Z". A repo's searches run from
its watermark, the latest updated_at in pull_requests for the repo minus
WATERMARK_MARGIN, through the run's start time. A repo with no rows yet, such as
one just added to the allowlist, starts from the since date instead. A run that
stops early keeps the rows it wrote, and the next run continues from the
watermark they set.

A search that matches more pull requests than GitHub returns logs a warning and
crawls the ones it returns.

Each pull request is read whole, with every nested connection, and written as
one snapshot under the run's crawl_id. A run that finishes every repo then writes
one row to crawl_runs, which the freshness check reads; a run that fails writes
none.
"""
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable

from github import GithubException
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.github.metrics.graphql_client import GitHubGraphQLClient
from recidiviz.github.metrics.queries import (
    PR_SEARCH_COUNT_QUERY,
    PR_SEARCH_QUERY,
    REPOSITORY_ACCESS_QUERY,
    SEARCH_CONNECTION_PATH,
    NestedConnection,
)
from recidiviz.github.metrics.row_sinks import RowSink
from recidiviz.github.metrics.rows import (
    CrawlContext,
    merge_overflow,
    rows_for_pull_request,
    truncated_connections,
)
from recidiviz.github.metrics.tables import (
    ALL_TABLE_ADDRESSES,
    CHILD_TABLE_ADDRESSES,
    CRAWL_RUNS_ADDRESS,
    PULL_REQUESTS_ADDRESS,
)
from recidiviz.utils.types import assert_type

logger = logging.getLogger(__name__)

# The value of crawl_runs.mode. The column stays because the deployed table has
# it, and every run crawls by update time.
_CRAWL_RUNS_MODE = "incremental"

# The first update time the crawl covers for a repo with no rows in pull_requests,
# one day after the webhook archive's first delivery on 2025-04-30.
DEFAULT_CRAWL_SINCE = date(2025, 5, 1)

# The crawl writes its buffered rows after this many pull requests, which bounds
# memory and keeps the number of load jobs per table well under BigQuery's daily
# limit of 1,500.
FLUSH_EVERY_PRS = 500

# The length of one search window.
SEARCH_WINDOW = timedelta(days=1)

# A repo's watermark sits this far before the latest updated_at already in
# pull_requests, to cover a failed night, search index lag and clock skew. It must
# be at least SEARCH_WINDOW: a run that stops partway through a window has written
# only some of that window's pull requests, and the next run reads the whole
# window again only if it starts before it.
WATERMARK_MARGIN = timedelta(days=2)

# GitHub search returns at most this many results for one query.
SEARCH_RESULT_CAP = 1000

# How many times the crawl runs one search before it accepts that the search
# returned fewer pull requests than GitHub counted.
_MAX_SEARCH_PASSES = 2

# The GraphQL error types GitHub returns for a repo the credential cannot see.
_REPOSITORY_ACCESS_ERROR_TYPES = frozenset({"NOT_FOUND", "FORBIDDEN"})


def _is_repository_access_error(e: GithubException) -> bool:
    """Returns whether e carries a GraphQL error that says the credential cannot
    see a repo. A rejected credential, a failed token request and a rate limit all
    return False."""
    # An HTTP error status has a body with a message and no errors list.
    if not isinstance(e.data, dict) or "errors" not in e.data:
        return False
    return any(
        error.get("type") in _REPOSITORY_ACCESS_ERROR_TYPES
        for error in e.data["errors"]
    )


def day_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Returns one-day windows from start to end, each starting where the last one
    ended; the last window may be shorter."""
    windows = []
    window_start = start
    while window_start < end:
        window_end = min(window_start + SEARCH_WINDOW, end)
        windows.append((window_start, window_end))
        window_start = window_end
    return windows


def search_query_for_updated_window(
    repository_full_name: str, start: datetime, end: datetime
) -> str:
    """Returns the search string for pull requests in the repo updated between start
    and end. GitHub's range is inclusive at both ends, so a pull request updated at
    exactly a window boundary matches two windows."""
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    return (
        f"repo:{repository_full_name} is:pr "
        f"updated:{start_utc:%Y-%m-%dT%H:%M:%SZ}..{end_utc:%Y-%m-%dT%H:%M:%SZ}"
    )


def _describe_window(repo: str, start: datetime, end_inclusive: datetime) -> str:
    return f"[{repo}] {start.isoformat()} to {end_inclusive.isoformat()}"


class GitHubMetricsCrawler:
    """Reads pull requests for each repo and writes their rows to a sink.

    Rows for each pull request accumulate in memory and go to the sink every
    FLUSH_EVERY_PRS pull requests and when a repo finishes. Each flush writes the
    six child tables before pull_requests.
    """

    def __init__(
        self,
        *,
        client: GitHubGraphQLClient,
        bq_client: BigQueryClient | None,
        sink: RowSink,
        repositories: list[str],
        ctx: CrawlContext,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        """
        Args:
            client: The GitHub GraphQL client.
            bq_client: Reads each repo's watermark from pull_requests. None when
                the sink writes local files; then every repo starts at the since
                date.
            sink: Where the rows go.
            repositories: Repos to crawl as owner/name, for example
                Recidiviz/pulse-data.
            ctx: The crawl_id and crawled_at stamped on every row.
            now: Returns the current time in UTC, which becomes
                crawl_runs.finished_at.
        """
        self._client = client
        self._bq_client = bq_client
        self._sink = sink
        self._repositories = repositories
        self._ctx = ctx
        self._now = now
        self._buffer: dict[BigQueryAddress, list[dict[str, Any]]] = {
            address: [] for address in ALL_TABLE_ADDRESSES
        }
        self._buffered_prs = 0
        # Node ids of the pull requests this run has crawled. Search pages shift
        # when a pull request is opened or updated mid-crawl, which returns a pull
        # request again; skipping it keeps one snapshot per pull request per
        # crawl_id.
        self._crawled_pr_ids: set[str] = set()

    def crawl(self, since: date) -> None:
        """Crawls every pull request updated since each repo's start: its
        watermark, or midnight UTC on since for a repo with no rows in
        pull_requests.

        Raises ValueError before crawling anything if the credential cannot read
        one of the repos.
        """
        self._check_repository_access()
        since_start = datetime.combine(since, time.min, tzinfo=timezone.utc)
        starts = {
            repo: self._watermark(repo) or since_start for repo in self._repositories
        }
        for repo, start in starts.items():
            crawled = sum(
                self._crawl_updated_window(repo, window_start, window_end)
                for window_start, window_end in day_windows(start, self._ctx.crawled_at)
            )
            self._flush()
            logger.info(
                "Crawl of [%s] from %s crawled %d pull requests",
                repo,
                start.isoformat(),
                crawled,
            )
        self._write_crawl_run()

    def _write_crawl_run(self) -> None:
        """Writes the crawl_runs row for a run that crawled every requested repo."""
        self._sink.write(
            CRAWL_RUNS_ADDRESS,
            [
                {
                    "crawl_id": self._ctx.crawl_id,
                    "mode": _CRAWL_RUNS_MODE,
                    "started_at": self._ctx.crawled_at.isoformat(),
                    "finished_at": self._now().isoformat(),
                    "repositories_requested": len(self._repositories),
                    "repositories_crawled": len(self._repositories),
                    "pull_requests_crawled": len(self._crawled_pr_ids),
                }
            ],
        )

    def _crawl_updated_window(self, repo: str, start: datetime, end: datetime) -> int:
        """Crawls the pull requests updated between start and end and returns the
        number crawled, with a warning when the search matches more than it
        returns."""
        query_string = search_query_for_updated_window(repo, start, end)
        issue_count = self._issue_count(query_string)
        if issue_count == 0:
            return 0
        window = _describe_window(repo, start, end)
        if issue_count >= SEARCH_RESULT_CAP:
            logger.warning(
                "%s matches %d pull requests, at the search cap of %d; some will be "
                "missed",
                window,
                issue_count,
                SEARCH_RESULT_CAP,
            )
        crawled = self._crawl_search(repo, query_string, updated_through=end)
        logger.info(
            "Crawled %s: issueCount %d, crawled %d", window, issue_count, crawled
        )
        return crawled

    def _check_repository_access(self) -> None:
        """Raises ValueError naming the first repo the credential cannot
        read. GitHub search reports zero results rather than an error for such a
        repo, so without this check it would look like a repo with no pull
        requests."""
        for repo in self._repositories:
            owner, name = repo.split("/", 1)
            try:
                data = self._client.query(
                    REPOSITORY_ACCESS_QUERY, {"owner": owner, "name": name}
                )
            except GithubException as e:
                if not _is_repository_access_error(e):
                    raise
                raise ValueError(
                    f"The GitHub credential cannot read {repo}: {e}"
                ) from e
            if data.get("repository") is None:
                raise ValueError(
                    f"The GitHub credential cannot read {repo}: GitHub returned no "
                    "repository"
                )

    def _issue_count(self, query_string: str) -> int:
        """Returns how many pull requests the search string matches."""
        data = self._client.query(PR_SEARCH_COUNT_QUERY, {"q": query_string})
        return data["search"]["issueCount"]

    def _watermark(self, repo: str) -> datetime | None:
        """Returns the latest updated_at in pull_requests for the repo minus
        WATERMARK_MARGIN, or None if the repo has no rows."""
        if self._bq_client is None:
            return None
        address = PULL_REQUESTS_ADDRESS.to_project_specific_address(
            self._bq_client.project_id
        ).format_address_for_query()
        query_job = self._bq_client.run_query_async(
            query_str=f"""
SELECT MAX(updated_at) AS max_updated_at
FROM {address}
WHERE repository_full_name = @repository_full_name
""",
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("repository_full_name", "STRING", repo),
            ],
        )
        rows = list(query_job.result())
        max_updated_at = rows[0]["max_updated_at"] if rows else None
        if max_updated_at is None:
            return None
        return max_updated_at - WATERMARK_MARGIN

    def _crawl_search(
        self, repo: str, query_string: str, *, updated_through: datetime
    ) -> int:
        """Crawls every pull request the search string matches that this run has
        not crawled yet and that was updated no later than updated_through,
        completes its truncated connections, buffers its rows, and returns the
        number crawled. Flushes every FLUSH_EVERY_PRS pull requests.

        A search page is an offset into results that change when a pull request
        is updated mid-search, which can move another pull request across a page
        boundary unread. So a search that returns fewer pull requests than GitHub
        counted runs again, and logs a warning if it is still short.
        """
        crawled = 0
        for _ in range(_MAX_SEARCH_PASSES):
            issue_count = 0
            returned_ids: set[str] = set()
            for page in self._client.paginate(
                PR_SEARCH_QUERY, {"q": query_string}, SEARCH_CONNECTION_PATH
            ):
                issue_count = max(issue_count, page["issueCount"])
                for node in page["nodes"]:
                    returned_ids.add(node["id"])
                    if self._crawl_node(repo, node, updated_through=updated_through):
                        crawled += 1
            # At the cap GitHub never returns every match, and
            # _crawl_updated_window has logged that already.
            if issue_count >= SEARCH_RESULT_CAP or len(returned_ids) >= issue_count:
                return crawled
        logger.warning(
            "Search [%s] counted %d pull requests but returned %d in each of %d "
            "passes; some may be missed",
            query_string,
            issue_count,
            len(returned_ids),
            _MAX_SEARCH_PASSES,
        )
        return crawled

    def _crawl_node(
        self, repo: str, node: dict[str, Any], *, updated_through: datetime
    ) -> bool:
        """Buffers the rows for one pull request node and returns True, or returns
        False without buffering anything for a pull request this run has crawled
        already or one updated after updated_through."""
        if node["id"] in self._crawled_pr_ids:
            logger.debug(
                "Skipping %s #%s, already crawled in this run", repo, node["number"]
            )
            return False
        updated_at = datetime.fromisoformat(node["updatedAt"])
        # Search can return a pull request updated after the window ends, because
        # its index lags the live data. Its row would move the repo's watermark
        # past windows this run has not crawled, so it waits for the later window
        # or run that covers its update time.
        if updated_at > updated_through:
            logger.debug(
                "Skipping %s #%s, updated at %s after the window ends at %s",
                repo,
                node["number"],
                updated_at.isoformat(),
                updated_through.isoformat(),
            )
            return False
        self._crawled_pr_ids.add(node["id"])
        self._complete_node(repo, node)
        for address, rows in rows_for_pull_request(node, repo, self._ctx).items():
            self._buffer[address].extend(rows)
        self._buffered_prs += 1
        if self._buffered_prs >= FLUSH_EVERY_PRS:
            self._flush()
        return True

    def _complete_node(self, repo: str, node: dict[str, Any]) -> None:
        """Reads the remaining pages of every truncated connection on the pull
        request node that has an overflow query, and merges them in.

        It loops until nothing readable is truncated, because a review read by the
        reviews follow-up can itself have more comments to read.
        """
        owner, name = repo.split("/", 1)
        while readable := [
            c for c in truncated_connections(node) if c.overflow_query is not None
        ]:
            connection = readable[0]
            if connection.is_nested_in_reviews:
                for review in node["reviews"]["nodes"]:
                    page_info = review[connection.field]["pageInfo"]
                    if not page_info["hasNextPage"]:
                        continue
                    extra_nodes = self._read_overflow(
                        connection,
                        {"reviewId": review["id"], "after": page_info["endCursor"]},
                    )
                    merge_overflow(review[connection.field], extra_nodes)
                continue
            extra_nodes = self._read_overflow(
                connection,
                {
                    "owner": owner,
                    "name": name,
                    "number": node["number"],
                    "after": node[connection.field]["pageInfo"]["endCursor"],
                },
            )
            merge_overflow(node[connection.field], extra_nodes)

    def _read_overflow(
        self, connection: NestedConnection, variables: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Returns every node of the connection's overflow query, starting after
        the cursor in variables."""
        nodes: list[dict[str, Any]] = []
        for page in self._client.paginate(
            assert_type(connection.overflow_query, str),
            variables,
            connection.connection_path,
        ):
            nodes.extend(page["nodes"])
        return nodes

    def _flush(self) -> None:
        """Writes the buffered rows, the six child tables first and pull_requests
        last, so a pull_requests row never lands before its child rows."""
        if self._buffered_prs == 0:
            return
        for address in [*CHILD_TABLE_ADDRESSES, PULL_REQUESTS_ADDRESS]:
            self._sink.write(address, self._buffer[address])
            self._buffer[address] = []
        self._buffered_prs = 0
