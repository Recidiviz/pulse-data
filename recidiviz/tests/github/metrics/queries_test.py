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
"""Tests for the GraphQL query strings used by the pull request crawl."""
import re

from recidiviz.github.metrics.queries import (
    NESTED_CONNECTIONS,
    PR_SEARCH_COUNT_QUERY,
    PR_SEARCH_QUERY,
    REPOSITORY_ACCESS_QUERY,
)


def _declared_variables(query: str) -> set[str]:
    header = query[: query.index("{")]
    return set(re.findall(r"\$(\w+)\s*:", header))


def _used_variables(query: str) -> set[str]:
    body = query[query.index("{") :]
    return set(re.findall(r"\$(\w+)", body))


def _assert_balanced_braces(query: str) -> None:
    depth = 0
    for char in query:
        depth += {"{": 1, "}": -1}.get(char, 0)
        assert depth >= 0
    assert depth == 0


def test_overflow_queries_cover_every_connection() -> None:
    assert [c.name for c in NESTED_CONNECTIONS] == [
        "reviews",
        "review_comments",
        "issue_comments",
        "commits",
        "files",
        "timeline_items",
        "labels",
    ]
    assert [c.name for c in NESTED_CONNECTIONS if c.overflow_query is None] == [
        "labels"
    ]


def test_every_declared_variable_is_used() -> None:
    # GitHub rejects a query that declares a variable it never uses.
    for query in [
        REPOSITORY_ACCESS_QUERY,
        PR_SEARCH_COUNT_QUERY,
        PR_SEARCH_QUERY,
        *(c.overflow_query for c in NESTED_CONNECTIONS if c.overflow_query),
    ]:
        assert _declared_variables(query) == _used_variables(query)
        _assert_balanced_braces(query)
