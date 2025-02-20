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
"""Defines a BigQueryView that delegates to a separate BigQueryView and contains an additional
metadata query."""

from typing import Generic

from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)


class WithMetadataQueryBigQueryView(BigQueryView):
    """An extension of BigQueryView that delegates its view query to a separate BigQueryView.
    It adds an additional `metadata_query` and format kwargs, which enable a
    WithMetadataQueryBigQueryViewExporter to export the query results to GCS object metadata.
    """

    def __init__(
        self,
        *,
        metadata_query: str,
        delegate: BigQueryView,
        sandbox_context: BigQueryViewSandboxContext | None,
        **metadata_query_format_kwargs: str,
    ):
        super().__init__(
            dataset_id=delegate.dataset_id,
            view_id=delegate.view_id,
            description=delegate.description,
            bq_description=delegate.bq_description,
            view_query_template=delegate.view_query_template,
            materialized_address=delegate.materialized_address,
            sandbox_context=sandbox_context,
            clustering_fields=delegate.clustering_fields,
            **delegate.query_format_kwargs,
        )

        try:
            self.metadata_query = self.query_builder.build_query(
                project_id=self.project,
                query_template=metadata_query,
                query_format_kwargs=metadata_query_format_kwargs,
            )
        except ValueError as e:
            raise ValueError(
                f"Unable to format metadata query for {self.address}"
            ) from e

        self.delegate = delegate

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"metadata_query={self.metadata_query}, "
            f"delegate={self.delegate.__repr__}"
        )


class WithMetadataQueryBigQueryViewBuilder(
    Generic[BigQueryViewBuilderType], BigQueryViewBuilder[WithMetadataQueryBigQueryView]
):
    """Class that builds a WithMetadataQueryBigQueryView."""

    def __init__(
        self,
        *,
        metadata_query: str,
        delegate: BigQueryViewBuilderType,
        **metadata_query_format_kwargs: str,
    ):
        self.metadata_query = metadata_query
        self.delegate = delegate
        self.metadata_query_format_kwargs = metadata_query_format_kwargs

        self.projects_to_deploy = delegate.projects_to_deploy
        self.dataset_id = delegate.dataset_id
        self.view_id = delegate.view_id
        self.materialized_address = delegate.materialized_address

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> WithMetadataQueryBigQueryView:
        return WithMetadataQueryBigQueryView(
            metadata_query=self.metadata_query,
            delegate=self.delegate.build(),
            sandbox_context=sandbox_context,
            **self.metadata_query_format_kwargs,
        )
