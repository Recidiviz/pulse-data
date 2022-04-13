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

"""An extension of MetricBigQueryViewBuilder with extra functionality related to pathways views specifically."""

from typing import List, Optional, Tuple

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder


class PathwaysMetricBigQueryViewBuilder(MetricBigQueryViewBuilder):
    """
    This class includes functionality for filtering metric aggregates with null/unknown dimensions. Aggregates
    containing null/unknown dimensions are not retrievable in the Pathways user interface and are removed to decrease
    our file size.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        view_id: str,
        description: str,
        view_query_template: str,
        dimensions: Tuple[str, ...],
        should_materialize: bool = False,
        materialized_address_override: Optional[BigQueryAddress] = None,
        clustering_fields: Optional[List[str]] = None,
        # All keyword args must have string values
        **query_format_kwargs: str,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=view_id,
            description=description,
            view_query_template=self._view_query_template_with_filtered_dimensions(
                view_query_template=view_query_template, dimensions=dimensions
            ),
            dimensions=dimensions,
            should_materialize=should_materialize,
            materialized_address_override=materialized_address_override,
            clustering_fields=clustering_fields,
            **query_format_kwargs,
        )

    @classmethod
    def _has_all_dimensions_clause(cls, dimensions: Tuple[str, ...]) -> str:
        clauses = [
            f"COALESCE(UPPER(CAST({dimension} AS STRING)), 'EXTERNAL_UNKNOWN') NOT IN ('UNKNOWN', 'EXTERNAL_UNKNOWN', 'INTERNAL_UNKNOWN')"
            for dimension in dimensions
        ]

        return "\n AND ".join(clauses)

    @classmethod
    def _view_query_template_with_filtered_dimensions(
        cls, view_query_template: str, dimensions: Tuple[str, ...]
    ) -> str:
        return f"""
            WITH pathways_view AS ( {view_query_template} )
            SELECT * FROM pathways_view
            WHERE {cls._has_all_dimensions_clause(dimensions)}
        """
