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

from typing import Generic, Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    BigQueryViewBuilderType,
)


class WithMetadataQueryBigQueryView(BigQueryView):
    """An extension of BigQueryView that delegates its view query to a separate BigQueryView.
    It adds an additional `metadata_query` and format kwargs, which enable a
    WithMetadataQueryBigQueryViewExporter to export the query results to GCS object metadata."""

    def __init__(
        self,
        *,
        metadata_query: str,
        delegate: BigQueryView,
        address_overrides: Optional[BigQueryAddressOverrides],
        **metadata_query_format_kwargs: str,
    ):
        # "description" gets added to the BigQueryView kwargs when the view is built;
        # remove it here so it doesn't conflict with the parameter
        # TODO(#14545): Remove this once description no longer is added to the kwargs.
        delegate_query_format_kwargs = delegate.query_format_kwargs.copy()
        del delegate_query_format_kwargs["description"]

        super().__init__(
            dataset_id=delegate.dataset_id,
            view_id=delegate.view_id,
            description=delegate.description,
            view_query_template=delegate.view_query_template,
            materialized_address=delegate.materialized_address,
            address_overrides=address_overrides,
            clustering_fields=delegate.clustering_fields,
            should_deploy_predicate=None,
            **delegate_query_format_kwargs,
        )

        metadata_query_no_overrides = self._format_view_query(
            metadata_query, inject_project_id=True, **metadata_query_format_kwargs
        )
        self.metadata_query = (
            self._apply_overrides_to_view_query(
                metadata_query_no_overrides, address_overrides
            )
            if address_overrides
            else metadata_query_no_overrides
        )
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
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> WithMetadataQueryBigQueryView:
        return WithMetadataQueryBigQueryView(
            metadata_query=self.metadata_query,
            delegate=self.delegate.build(),
            address_overrides=address_overrides,
            **self.metadata_query_format_kwargs,
        )
