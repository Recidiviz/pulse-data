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
from typing import Union

import attr

from recidiviz.common.constants.states import StateCode


class BigQueryQueryProvider:
    """An interface that can be used to access a valid BigQuery query."""

    @abc.abstractmethod
    def get_query(self) -> str:
        """Returns a SQL query that can be run in BigQuery."""

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
