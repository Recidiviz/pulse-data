# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Builder for a view that unions the results from a list of parent views together."""

import logging
from typing import Callable, Dict, Optional, Sequence, Set

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)
from recidiviz.utils import metadata


def default_builder_to_select_statement(
    view_builder: BigQueryViewBuilder,  # pylint: disable=unused-argument
) -> str:
    return "SELECT *"


class UnionAllBigQueryViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """Builder for a view that unions the results from a list of parent views together."""

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        parent_view_builders: Sequence[BigQueryViewBuilderType],
        builder_to_select_statement: Callable[
            [BigQueryViewBuilderType], str
        ] = default_builder_to_select_statement,
    ):
        """
        Args:
            dataset_id: The view address dataset_id
            view_id: The view address table_id
            description: Description for this view
            parent_view_builders: The list of view builders of views to select from.
            builder_to_select_statement: A function that, for a given parent view
                builder, returns a string SELECT statement that should be used to query
                the provided view.
        """
        if not parent_view_builders:
            raise ValueError(
                f"Found no view builders to union for view `{dataset_id}.{view_id}`"
            )
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.description = description
        self.parent_view_builders = parent_view_builders
        self.builder_to_select_statement = builder_to_select_statement
        self.materialized_address = self._build_materialized_address(
            dataset_id=dataset_id,
            view_id=view_id,
            materialized_address_override=None,
            should_materialize=True,
        )
        self.projects_to_deploy = None
        self.parent_address_filter: Optional[Set[BigQueryAddress]] = None

    def set_parent_address_filter(
        self, parent_address_filter: Set[BigQueryAddress]
    ) -> None:
        self.parent_address_filter = parent_address_filter

    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> BigQueryView:
        """
        Builds a view that unions the results from a list of parent views together.
        If parent_address_filter has been set via set_parent_address_filter(), then
        the resulting view will only union results from the tables in that filter set.
        """

        if self.parent_address_filter and not address_overrides:
            raise ValueError(
                "Cannot set a UNION ALL query filter unless loading views into a "
                "sandbox."
            )

        select_queries = []
        query_format_args: Dict[str, str] = {}

        parent_view_builders_in_project = [
            vb
            for vb in self.parent_view_builders
            if vb.should_deploy_in_project(metadata.project_id())
        ]

        if not parent_view_builders_in_project:
            raise ValueError(
                f"Found no valid parent views for UNION ALL composite view "
                f"[{self.address.to_str()}]"
            )

        filtered_parent_view_builders = [
            vb
            for vb in parent_view_builders_in_project
            if self.parent_address_filter is None
            or vb.address in self.parent_address_filter
        ]

        if not filtered_parent_view_builders:
            logging.warning(
                "No parent views for UNION ALL composite view [%s.%s] in the "
                "list of view filters. Selecting ALL parents.",
                self.address.dataset_id,
                self.address.table_id,
            )
            filtered_parent_view_builders = list(self.parent_view_builders)

        # Sort view builders by address so we produce a query with deterministic order
        for vb in sorted(filtered_parent_view_builders, key=lambda vb: vb.address):
            select_statement = self.builder_to_select_statement(vb)
            if not vb.materialized_address:
                raise ValueError(f"Expected view [{vb.address}] to be materialized.")
            select_queries.append(
                vb.materialized_address.select_query_template(
                    select_statement=select_statement
                )
            )

        view_query_template = "\nUNION ALL\n".join(select_queries)

        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            bq_description=self.description,
            view_query_template=view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=None,
            address_overrides=address_overrides,
            should_deploy_predicate=None,
            **query_format_args,
        )
