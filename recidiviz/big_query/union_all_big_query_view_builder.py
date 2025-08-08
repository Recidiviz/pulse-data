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
"""Builder for a view that unions the results from a list of parent views or source
tables together.
"""
import logging
from typing import Callable, Dict, Optional, Sequence

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.utils import metadata


class UnionAllBigQueryViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """Builder for a view that unions the results from a list of parent views or source
    tables together.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        bq_description: str | None = None,
        parents: Sequence[BigQueryViewBuilderType] | Sequence[BigQueryAddress],
        clustering_fields: list[str],
        custom_select_statement: Optional[str] = None,
        parent_to_select_statement: Optional[
            Callable[[BigQueryViewBuilderType], str]
        ] = None,
        materialized_address_override: Optional[BigQueryAddress] = None,
    ):
        """
        Args:
            dataset_id: The view address dataset_id
            view_id: The view address table_id
            description: Description for this view
            bq_description: Description for this view, truncated to get around BQ limits
            parents: The list of view builders or tables to select from.
            clustering_fields: Columns by which to cluster this view's materialized
                table.
            custom_select_statement: A custom SELECT statement to be used for each
                individual view / address query. May only be set if
                |parent_to_select_statement| is null.
            parent_to_select_statement: A function that, for a given parent view
                builder or table address, returns a string SELECT statement that should
                be used to query the provided view. May only be set if
                |custom_select_statement| is not null.
            materialized_address_override: If set, this view will be materialized to
                this address rather than the default materialization address.
        """
        if not parents:
            raise ValueError(
                f"Found no view builders / tables to union for view "
                f"`{dataset_id}.{view_id}`"
            )

        if not clustering_fields:
            raise ValueError(
                f"Found no clustering fields for UnionAllBigQueryViewBuilder view "
                f"[{dataset_id}.{view_id}]."
            )
        if custom_select_statement and parent_to_select_statement:
            raise ValueError(
                f"May only set one of |custom_select_statement| and "
                f"|parent_to_select_statement|. Found both set for view "
                f"{dataset_id}.{view_id}"
            )
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.description = description
        self.bq_description = bq_description
        self.parents = parents
        self.clustering_fields = clustering_fields
        self.custom_select_statement = custom_select_statement
        self.parent_to_select_statement = parent_to_select_statement
        self.materialized_address = self._build_materialized_address(
            dataset_id=dataset_id,
            view_id=view_id,
            materialized_address_override=materialized_address_override,
            should_materialize=True,
        )
        self.projects_to_deploy = None

    @staticmethod
    def _get_address(
        parent: BigQueryViewBuilderType | BigQueryAddress,
    ) -> BigQueryAddress:
        if isinstance(parent, BigQueryAddress):
            return parent
        return parent.address

    @staticmethod
    def _get_table_for_query(
        parent: BigQueryViewBuilderType | BigQueryAddress,
    ) -> BigQueryAddress:
        if isinstance(parent, BigQueryAddress):
            return parent
        return parent.table_for_query

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryView:
        """
        Builds a view that unions the results from a list of parent views together.
        If parent_address_filter has been set via set_parent_address_filter(), then
        the resulting view will only union results from the tables in that filter set.
        """
        select_queries = []
        query_format_args: Dict[str, str] = {}

        parents_in_project = [
            parent
            for parent in self.parents
            if isinstance(parent, BigQueryAddress)
            or parent.should_deploy_in_project(metadata.project_id())
        ]

        if not parents_in_project:
            raise ValueError(
                f"Found no valid parent views for UNION ALL composite view "
                f"[{self.address.to_str()}]"
            )

        if sandbox_context is None or sandbox_context.state_code_filter is None:
            filtered_parents = parents_in_project
        else:
            filtered_parents = [
                parent
                for parent in parents_in_project
                if self._get_address(parent).state_code_for_address()
                == sandbox_context.state_code_filter
            ]

        if not filtered_parents:
            logging.warning(
                "No parent views for UNION ALL composite view [%s] in the "
                "list of view filters. Selecting ALL parents.",
                self.address.to_str(),
            )
            filtered_parents = parents_in_project

        # Sort view builders by address so we produce a query with deterministic order
        for parent in sorted(filtered_parents, key=self._get_address):
            table_for_query = self._get_table_for_query(parent)

            if self.custom_select_statement:
                custom_select_statement = self.custom_select_statement
            elif self.parent_to_select_statement:
                if isinstance(parent, BigQueryAddress):
                    raise ValueError(
                        "Can only use a custom select statement for a "
                        "BigQueryViewBuilder type parent"
                    )
                custom_select_statement = self.parent_to_select_statement(parent)
            else:
                custom_select_statement = None

            if custom_select_statement:
                query = table_for_query.select_query_template(
                    select_statement=custom_select_statement
                )
            else:
                query = table_for_query.select_query_template()

            select_queries.append(query)

        view_query_template = "\nUNION ALL\n".join(select_queries)

        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            bq_description=self.bq_description
            if self.bq_description
            else self.description,
            view_query_template=view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            time_partitioning=None,
            sandbox_context=sandbox_context,
            materialized_table_schema=None,
            **query_format_args,
        )
