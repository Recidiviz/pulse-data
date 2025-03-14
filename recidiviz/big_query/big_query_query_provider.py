# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines an interface that can be used to access a valid BigQuery query."""
import abc
import re
from functools import lru_cache
from typing import Set, Union

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode

REFERENCED_BQ_ADDRESS_REGEX = re.compile(
    r"`(?P<project_id_clause>[\w-]*)\.(?P<dataset_id>[\w-]*)\.(?P<table_id>[\w-]*)`"
)


class BigQueryQueryProvider:
    """An interface that can be used to access a valid BigQuery query."""

    @abc.abstractmethod
    def get_query(self) -> str:
        """Returns a SQL query that can be run in BigQuery."""

    @property
    def parent_tables(self) -> Set[BigQueryAddress]:
        """Returns a set of addresses for tables/views referenced in query."""
        return self._parent_tables(self.get_query())

    @classmethod
    @lru_cache(maxsize=None)
    def _parent_tables(cls, query: str) -> Set[BigQueryAddress]:
        return {
            BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
            for _project_id, dataset_id, table_id in re.findall(
                REFERENCED_BQ_ADDRESS_REGEX, query
            )
        }

    @classmethod
    def resolve(cls, query_str_or_provider: Union[str, "BigQueryQueryProvider"]) -> str:
        """Given a query string or query provider, returns a query string."""
        if isinstance(query_str_or_provider, BigQueryQueryProvider):
            return query_str_or_provider.get_query()
        return query_str_or_provider

    @classmethod
    def strip_semicolon(
        cls, query_str_or_provider: Union[str, "BigQueryQueryProvider"]
    ) -> str:
        """Given a query string or query provider, returns a query string with the
        trailing semicolon stripped.
        """
        return cls.resolve(query_str_or_provider).rstrip().rstrip(";")


@attr.define
class SimpleBigQueryQueryProvider(BigQueryQueryProvider):
    """A simple implementation of BigQueryQueryProvider which just wraps a plain query
    string.
    """

    query: str

    def get_query(self) -> str:
        return self.query


@attr.define(frozen=True, kw_only=True)
class StateFilteredQueryProvider(BigQueryQueryProvider):
    """A query provider that wraps the original query in a WHERE clause that filters
    down to only rows for the given |state_code|.
    """

    original_query: str | BigQueryQueryProvider
    state_code_filter: StateCode

    def get_query(self) -> str:
        return f"""SELECT * FROM (
{self.strip_semicolon(self.original_query)}
) WHERE state_code = '{self.state_code_filter.value}';
"""
