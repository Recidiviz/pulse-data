# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""An extension of BigQueryView with extra functionality related to metric views specifically."""

from typing import List, Optional, Tuple

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.utils.types import assert_type


class MetricBigQueryView(BigQueryView):
    """An extension of BigQueryView with extra functionality related to metric views specifically."""

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        dimensions: Tuple[str, ...],
        materialized_address: BigQueryAddress,
        sandbox_context: BigQueryViewSandboxContext | None,
        clustering_fields: Optional[List[str]] = None,
        **query_format_kwargs: str,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=view_id,
            description=description,
            bq_description=description,
            view_query_template=view_query_template,
            materialized_address=materialized_address,
            sandbox_context=sandbox_context,
            clustering_fields=clustering_fields,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.dimensions = dimensions

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"view={self.project}.{self.dataset_id}.{self.view_id}, "
            f"view_query='{self.view_query}', "
            f"dimensions={self.dimensions})"
        )


class MetricBigQueryViewBuilder(BigQueryViewBuilder[MetricBigQueryView]):
    """Class that builds a MetricBigQueryView. Can be instantiated as a top-level variable, unlike a BigQueryView, whose
    constructor requires that a project_id has been properly set in the metadata package - something that often happens
    after a file is imported."""

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        dimensions: Tuple[str, ...],
        materialized_address_override: Optional[BigQueryAddress] = None,
        clustering_fields: Optional[List[str]] = None,
        # All keyword args must have string values
        **query_format_kwargs: str,
    ):
        self.dataset_id = dataset_id
        self.view_id = view_id
        self.projects_to_deploy = None
        self.description = description
        self.view_query_template = view_query_template
        self.dimensions = dimensions
        self.materialized_address: BigQueryAddress = assert_type(
            self._build_materialized_address(
                dataset_id=dataset_id,
                view_id=view_id,
                materialized_address_override=materialized_address_override,
                # We materialize all MetricBigQueryViewBuilder
                should_materialize=True,
            ),
            BigQueryAddress,
        )
        self.clustering_fields = clustering_fields
        self.query_format_kwargs = query_format_kwargs

    def _build(
        self,
        *,
        sandbox_context: BigQueryViewSandboxContext | None,
    ) -> MetricBigQueryView:
        return MetricBigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self.view_query_template,
            dimensions=self.dimensions,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            sandbox_context=sandbox_context,
            **self.query_format_kwargs,
        )
