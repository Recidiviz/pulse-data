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

from typing import Dict, Optional

from recidiviz.metrics.metric_big_query_view import (
    MetricBigQueryView,
    MetricBigQueryViewBuilder,
)


class PathwaysMetricBigQueryViewBuilder(MetricBigQueryViewBuilder):
    """
    This class includes functionality for filtering metric aggregates with null/unknown dimensions. Aggregates
    containing null/unknown dimensions are not retrievable in the Pathways user interface and are removed to decrease
    our file size.
    """

    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> MetricBigQueryView:
        return MetricBigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            view_query_template=self._view_query_template_with_filtered_dimensions(),
            dimensions=self.dimensions,
            should_materialize=self.should_materialize,
            materialized_address_override=self.materialized_address_override,
            clustering_fields=self.clustering_fields,
            dataset_overrides=dataset_overrides,
            **self.query_format_kwargs,
        )

    @property
    def _has_all_dimensions_clause(self) -> str:
        clauses = [
            f"COALESCE(UPPER(CAST({dimension} AS STRING)), 'EXTERNAL_UNKNOWN') NOT IN ('UNKNOWN', 'EXTERNAL_UNKNOWN', 'INTERNAL_UNKNOWN')"
            for dimension in self.dimensions
        ]

        return "\n AND ".join(clauses)

    def _view_query_template_with_filtered_dimensions(self) -> str:
        return f"""
            WITH pathways_view AS ( {self.view_query_template} )
            SELECT * FROM pathways_view
            WHERE {self._has_all_dimensions_clause}
        """
