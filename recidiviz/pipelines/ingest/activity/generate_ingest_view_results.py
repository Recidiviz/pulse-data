# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A PTransform that generates ingest view results for a given ingest view."""
import datetime
import logging
from typing import Any

import apache_beam as beam
from apache_beam.pvalue import PBegin
from dateutil import parser

from recidiviz.big_query.big_query_utils import datetime_clause
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import ReadFromBigQuery
from recidiviz.utils.string import StrictStringFormatter

INGEST_VIEW_OUTPUT_QUERY_TEMPLATE = f"""
WITH view_results AS (
    {{view_query}}
)
SELECT *,
    CURRENT_DATETIME('UTC') AS {MATERIALIZATION_TIME_COL_NAME},
    {{upper_bound_datetime_inclusive}} AS {UPPER_BOUND_DATETIME_COL_NAME}
FROM view_results;
"""


class GenerateIngestViewResults(beam.PTransform):
    """Generates ingest view results for a given ingest view based on provided parameters."""

    def __init__(
        self,
        ingest_view_builder: DirectIngestViewQueryBuilder,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        resource_labels: dict[str, str],
    ) -> None:
        super().__init__()

        if not resource_labels:
            raise ValueError("Received no resource_labels in GenerateIngestViewResults")

        self.ingest_view_builder = ingest_view_builder
        self.ingest_view_name = ingest_view_builder.ingest_view_name
        # If we're using a raw data table that has no data, that
        # is bad and we want to yell.
        self.upper_bound_datetime_inclusive = self.get_upper_bound_datetime_inclusive(
            raw_data_tables_to_upperbound_dates
        )
        self.raw_data_source_instance = raw_data_source_instance
        self.resource_labels = resource_labels

    def get_upper_bound_datetime_inclusive(
        self, upperbounds_by_table: dict[str, str]
    ) -> datetime.datetime:
        """Returns the max upper bound datetime across raw data dependencies. Throws if any are missing"""
        tables_with_missing_upperbound = set()
        all_upper_bounds = set()
        for table in self.ingest_view_builder.raw_data_table_dependency_file_tags:
            if upperbound := upperbounds_by_table.get(table):
                all_upper_bounds.add(parser.isoparse(upperbound))
            else:
                tables_with_missing_upperbound.add(table)
        if tables_with_missing_upperbound:
            raise ValueError(
                f"Ingest View [{self.ingest_view_name}] has raw data dependencies "
                f"with missing upper bound datetimes: {','.join(tables_with_missing_upperbound)}"
            )
        return max(all_upper_bounds)

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[dict[str, Any]]:
        query = self.generate_ingest_view_query(
            view_builder=self.ingest_view_builder,
            raw_data_source_instance=self.raw_data_source_instance,
            upper_bound_datetime_inclusive=self.upper_bound_datetime_inclusive,
        )

        logging.info("Ingest view query for [%s]: %s", self.ingest_view_name, query)

        return (
            input_or_inputs
            | f"Read {self.ingest_view_name} ingest view results from BigQuery."
            >> ReadFromBigQuery(
                query=query,
                resource_labels=self.resource_labels,
            )
        )

    @staticmethod
    def generate_ingest_view_query(
        view_builder: DirectIngestViewQueryBuilder,
        raw_data_source_instance: DirectIngestInstance,
        upper_bound_datetime_inclusive: datetime.datetime,
    ) -> str:
        """
        Returns a version of the ingest view query for the provided args that can
        be run in Dataflow. Augments the ingest view query with metadata columns that
        will be output to materialized ingest view results tables.

        A note that this query for Dataflow cannot use materialized tables or temporary
        tables.
        """
        view_query = view_builder.build_query(
            query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=raw_data_source_instance,
                raw_data_datetime_upper_bound=upper_bound_datetime_inclusive,
            )
        ).rstrip(" ;")
        return StrictStringFormatter().format(
            INGEST_VIEW_OUTPUT_QUERY_TEMPLATE,
            view_query=view_query,
            upper_bound_datetime_inclusive=datetime_clause(
                upper_bound_datetime_inclusive
            ),
        )
