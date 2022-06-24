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

from typing import List, Optional, Tuple, Union

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder

NON_STRING_DIMENSIONS = ("year", "month")


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
            view_query_template=view_query_template,
            dimensions=dimensions,
            should_materialize=should_materialize,
            materialized_address_override=materialized_address_override,
            clustering_fields=clustering_fields,
            dimensions_clause=self.replace_unknowns(dimensions),
            **query_format_kwargs,
        )

    @classmethod
    def replace_unknowns(cls, dimensions: Union[List[str], Tuple[str, ...]]) -> str:
        clauses = []
        for dimension in dimensions:
            if dimension in NON_STRING_DIMENSIONS:
                clauses.append(dimension)
            else:
                clauses.append(
                    f"""
                    CASE COALESCE(UPPER(CAST({dimension} AS STRING)), 'EXTERNAL_UNKNOWN')
                        WHEN 'EXTERNAL_UNKNOWN' THEN 'UNKNOWN'
                        WHEN 'INTERNAL_UNKNOWN' THEN 'OTHER'
                        ELSE CAST({dimension} AS STRING)
                        END
                    AS {dimension}"""
                )

        return ",\n".join(clauses)
