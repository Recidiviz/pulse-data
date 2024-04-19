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
"""Query provider classes / helpers for use querying reference queries in Dataflow
pipelines.
"""
from typing import Dict, Iterable, Optional, Set, Type

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    StateFilteredQueryProvider,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state import entities as state_entities


@attr.define(frozen=True, kw_only=True)
class RootEntityIdFilteredQueryProvider(BigQueryQueryProvider):
    """A query provider that wraps the original query in a WHERE clause that filters
    down to only rows for the root entities specified by |root_entity_id_filter_set|.
    The original query must have a column that matches the |root_entity_cls| primary key
    name (e.g. a person_id or staff_id column).
    """

    original_query: str | BigQueryQueryProvider
    root_entity_cls: (
        Type[state_entities.StatePerson] | Type[state_entities.StateStaff]
    )
    root_entity_id_filter_set: Set[int] = attr.ib(default=None)

    def get_query(self) -> str:
        root_entity_id_field = self.root_entity_cls.get_class_id_name()

        id_str_set = {
            str(root_entity_id)
            for root_entity_id in self.root_entity_id_filter_set
            if str(root_entity_id)
        }

        return f"""SELECT * FROM (
{self.strip_semicolon(self.original_query)}
) WHERE {root_entity_id_field} IN {tuple(sorted(id_str_set))};
"""


def view_builders_as_state_filtered_query_providers(
    view_builders: Iterable[BigQueryViewBuilder],
    state_code: StateCode,
    address_overrides: Optional[BigQueryAddressOverrides],
) -> Dict[str, StateFilteredQueryProvider]:
    """For the given list of view builders, returns a mapping of view id to query
    provider, where the query provider returns the results of the view query, filtered
    down to only rows for the given |state_code|.
    """
    views = [vb.build(address_overrides=address_overrides) for vb in view_builders]
    return {
        v.view_id: StateFilteredQueryProvider(
            original_query=v.view_query, state_code_filter=state_code
        )
        for v in views
    }
