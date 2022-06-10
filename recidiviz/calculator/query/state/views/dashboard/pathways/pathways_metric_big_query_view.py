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

from typing import List, Literal, Mapping, Optional, Tuple

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.string import StrictStringFormatter

MetricStat = Literal[
    "aggregating_location_id",
    "last_updated",
    "event_count",
    "full_name",
    "person_count",
]


# Contains aggregate functions to collapse disjointed metrics into one row
METRIC_STAT_AGGS: Mapping[MetricStat, str] = {
    "event_count": "SUM(event_count) AS event_count,",
    "last_updated": "MAX(last_updated) AS last_updated,",
    "person_count": "SUM(person_count) AS person_count,",
}


DIMENSIONS_WITHOUT_UNKNOWNS = {"state_code", "year", "month"}

MetricMetadata = Literal["age", "aggregating_location_id", "caseload", "full_name"]
# Metadata values are assumed to be one-to-one with a given aggregate row. i.e. for a person level view, a person's age
# does not change as the aggregate only contains one row.
METADATA_TEMPLATE = "ANY_VALUE({metadata_column}) AS {metadata_column},"


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
        metric_stats: Optional[Tuple[MetricStat, ...]] = None,
        metric_metadata: Optional[Tuple[MetricMetadata, ...]] = None,
        materialized_address_override: Optional[BigQueryAddress] = None,
        clustering_fields: Optional[List[str]] = None,
        # All keyword args must have string values
        **query_format_kwargs: str,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=view_id,
            description=description,
            view_query_template=self._view_query_template_with_updated_dimensions(
                view_query_template=view_query_template,
                dimensions=dimensions,
                metric_metadata=metric_metadata,
                metric_stats=metric_stats,
            ),
            dimensions=dimensions,
            should_materialize=should_materialize,
            materialized_address_override=materialized_address_override,
            clustering_fields=clustering_fields,
            **query_format_kwargs,
        )

    @classmethod
    def _replace_unknowns(cls, dimensions: Tuple[str, ...]) -> str:
        clauses = [
            f"""
            CASE COALESCE(UPPER(CAST({dimension} AS STRING)), 'EXTERNAL_UNKNOWN')
                WHEN 'EXTERNAL_UNKNOWN' THEN 'UNKNOWN'
                WHEN 'INTERNAL_UNKNOWN' THEN 'OTHER'
                ELSE CAST({dimension} AS STRING)
                END
            AS {dimension}"""
            for dimension in dimensions
            if dimension not in DIMENSIONS_WITHOUT_UNKNOWNS
        ]
        return ",\n".join(clauses)

    @classmethod
    def _view_query_template_with_updated_dimensions(
        cls,
        view_query_template: str,
        dimensions: Tuple[str, ...],
        metric_stats: Optional[Tuple[MetricStat, ...]] = None,
        metric_metadata: Optional[Tuple[MetricMetadata, ...]] = None,
    ) -> str:
        """Given a `view_query_template`, pessimistically re-aggregate rows by their dimensions,
        apply aggregate functions against `metric_stats` columns,
        and include any additional `metric_metadata` columns as specified by the view
        """
        dimensions_clause = ",\n".join(dimensions)
        stats_clause = "\n".join(
            METRIC_STAT_AGGS[stat] for stat in tuple(metric_stats or ())
        )
        metadata_clause = "\n".join(
            StrictStringFormatter().format(
                METADATA_TEMPLATE, metadata_column=metadata_column
            )
            for metadata_column in tuple(metric_metadata or ())
        )

        return f"""
            WITH pathways_view AS ( {view_query_template} ),
            coalesced_dimensions AS ( 
                SELECT *
                REPLACE (
                    {cls._replace_unknowns(dimensions)}
                )
                FROM pathways_view
            )
            SELECT
                {dimensions_clause},
                {metadata_clause}
                {stats_clause}
            FROM coalesced_dimensions
            GROUP BY {dimensions_clause}
            ORDER BY {dimensions_clause}
        """
