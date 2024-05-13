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
from typing import Any, Dict

import apache_beam as beam
from apache_beam.pvalue import PBegin
from dateutil import parser
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import datetime_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
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

INGEST_VIEW_LATEST_DATE_QUERY_TEMPLATE = f"""
SELECT
    MAX(update_datetime) AS {UPPER_BOUND_DATETIME_COL_NAME}
FROM (
        {{raw_data_tables}}
);"""

INDIVIDUAL_TABLE_QUERY_TEMPLATE = """SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `{project_id}.{raw_data_dataset}.{file_tag}`"""
INDIVIDUAL_TABLE_QUERY_WHERE_CLAUSE = " WHERE update_datetime <= '{upper_bound_date}'"
INDIVIDUAL_TABLE_LIMIT_ZERO_CLAUSE = " LIMIT 0"
INDIVIDUAL_TABLE_QUERY_WITH_WHERE_CLAUSE_TEMPLATE = (
    f"{INDIVIDUAL_TABLE_QUERY_TEMPLATE}{INDIVIDUAL_TABLE_QUERY_WHERE_CLAUSE}"
)
INDIVIDUAL_TABLE_QUERY_LIMIT_ZERO_TEMPLATE = (
    f"{INDIVIDUAL_TABLE_QUERY_TEMPLATE}{INDIVIDUAL_TABLE_LIMIT_ZERO_CLAUSE}"
)

ADDITIONAL_SCHEMA_COLUMNS = [
    bigquery.SchemaField(
        UPPER_BOUND_DATETIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        MATERIALIZATION_TIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
]


class GenerateIngestViewResults(beam.PTransform):
    """Generates ingest view results for a given ingest view based on provided parameters."""

    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: Dict[str, str],
        ingest_instance: DirectIngestInstance,
    ) -> None:
        super().__init__()

        if not raw_data_tables_to_upperbound_dates:
            raise ValueError(
                f"Must define raw_data_tables_to_upperbound_dates for view "
                f"[{ingest_view_name}]"
            )

        self.project_id = project_id
        self.state_code = state_code
        self.region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value
        )
        self.ingest_view_name = ingest_view_name
        self.view_builder: DirectIngestViewQueryBuilder = (
            DirectIngestViewQueryBuilderCollector(
                self.region, [ingest_view_name]
            ).get_query_builder_by_view_name(ingest_view_name=ingest_view_name)
        )
        parsed_upperbound_dates = {
            parser.isoparse(upper_bound_date_str)
            for upper_bound_date_str in raw_data_tables_to_upperbound_dates.values()
        }
        self.upper_bound_datetime_inclusive: datetime.datetime = max(
            parsed_upperbound_dates
        )
        self.ingest_instance = ingest_instance

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[Dict[str, Any]]:
        # If upper_bound_datetime_inclusive is None, that means that none of the raw table dependencies of this view
        # have imported any data, so we skip the view.
        if not self.upper_bound_datetime_inclusive:
            return (
                input_or_inputs
                | f"Skip querying {self.ingest_view_name} - no raw data"
                >> beam.Create([])
            )

        query = self.generate_ingest_view_query(
            view_builder=self.view_builder,
            raw_data_source_instance=self.ingest_instance,
            upper_bound_datetime_inclusive=self.upper_bound_datetime_inclusive,
        )

        logging.info("Ingest view query for [%s]: %s", self.ingest_view_name, query)

        return (
            input_or_inputs
            | f"Read {self.ingest_view_name} ingest view results from BigQuery."
            >> ReadFromBigQuery(query=query)
        )

    @staticmethod
    def generate_ingest_view_query(
        view_builder: DirectIngestViewQueryBuilder,
        raw_data_source_instance: DirectIngestInstance,
        upper_bound_datetime_inclusive: datetime.datetime,
    ) -> str:
        """Returns a version of the ingest view query for the provided args that can
        be run in Dataflow. Augments the ingest view query with metadata columns that
        will be output to materialized ingest view results tables.

        A note that this query for Dataflow cannot use materialized tables or temporary
        tables."""
        view_query = (
            view_builder.build_query(
                config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=raw_data_source_instance,
                    raw_data_datetime_upper_bound=upper_bound_datetime_inclusive,
                )
            )
            .rstrip()
            .rstrip(";")
        )

        upper_bound_datetime_inclusive_clause = datetime_clause(
            upper_bound_datetime_inclusive
        )

        return StrictStringFormatter().format(
            INGEST_VIEW_OUTPUT_QUERY_TEMPLATE,
            view_query=view_query,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_clause,
        )
