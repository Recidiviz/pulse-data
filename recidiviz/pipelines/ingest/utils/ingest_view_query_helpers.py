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
"""Utility functions for working with date pairs."""
import logging
from typing import Any, Dict, List

import apache_beam as beam
from dateutil import parser
from google.cloud import bigquery

from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    LOWER_BOUND_DATETIME_COL_NAME,
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.pipelines.ingest.pipeline_parameters import MaterializationMethod
from recidiviz.utils import environment
from recidiviz.utils.string import StrictStringFormatter

INGEST_VIEW_DATE_BOUND_TUPLES_QUERY_TEMPLATE = f"""
SELECT
    LAG(max_dt_on_date) OVER (
        ORDER BY update_date
    ) AS {LOWER_BOUND_DATETIME_COL_NAME},
    max_dt_on_date AS {UPPER_BOUND_DATETIME_COL_NAME},
FROM (
    SELECT
        update_date AS update_date,
        MAX(update_datetime) AS max_dt_on_date
    FROM (
        {{raw_data_tables}}
    )
    GROUP BY update_date
)
ORDER BY 1;"""

INGEST_VIEW_LATEST_DATE_QUERY_TEMPLATE = f"""
SELECT
    MAX(update_datetime) AS {UPPER_BOUND_DATETIME_COL_NAME},
    CAST(NULL AS DATETIME) AS {LOWER_BOUND_DATETIME_COL_NAME}
FROM (
        {{raw_data_tables}}
);"""

INDIVIDUAL_TABLE_QUERY_TEMPLATE = """SELECT DISTINCT update_datetime, CAST(update_datetime AS DATE) AS update_date
        FROM `{project_id}.{state_code}_raw_data.{file_tag}`"""


def generate_date_bound_tuples_query(
    project_id: str,
    state_code: str,
    raw_data_tables: List[str],
    materialization_method: MaterializationMethod = MaterializationMethod.ORIGINAL,
) -> str:
    """Returns a SQL query that will return a list of upper and lower bound date tuples
    which can each be used to generate an individual ingest view query."""
    raw_data_table_sql_statements = [
        StrictStringFormatter().format(
            INDIVIDUAL_TABLE_QUERY_TEMPLATE,
            project_id=project_id,
            state_code=state_code.lower(),
            file_tag=table,
        )
        for table in raw_data_tables
    ]
    raw_data_tables_sql = "\nUNION ALL\n        ".join(raw_data_table_sql_statements)
    raw_date_pairs_query = StrictStringFormatter().format(
        INGEST_VIEW_DATE_BOUND_TUPLES_QUERY_TEMPLATE
        if materialization_method == MaterializationMethod.ORIGINAL
        else INGEST_VIEW_LATEST_DATE_QUERY_TEMPLATE,
        raw_data_tables=raw_data_tables_sql,
    )
    return raw_date_pairs_query


ADDITIONAL_SCHEMA_COLUMNS = [
    bigquery.SchemaField(
        UPPER_BOUND_DATETIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        LOWER_BOUND_DATETIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="NULLABLE",
    ),
    bigquery.SchemaField(
        MATERIALIZATION_TIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
]


def get_ingest_view_date_diff_query(
    date_pair: Dict[str, Any],
    project_id: str,
    state_code: str,
    ingest_view_name: str,
    ingest_instance: DirectIngestInstance,
) -> beam.io.ReadFromBigQueryRequest:
    """Returns a query that calculates the date diff between the upper and lower bound dates."""
    # TODO(#22164) Automatically set up environment for Dataflow workers.
    if not environment.in_test() and not environment.in_ci():
        environment.setup_environment(project_id)
    region = direct_ingest_regions.get_direct_ingest_region(region_code=state_code)
    view_builder: DirectIngestViewQueryBuilder = DirectIngestViewQueryBuilderCollector(
        region, [ingest_view_name]
    ).get_query_builder_by_view_name(ingest_view_name=ingest_view_name)
    ingest_view_materialization_args = IngestViewMaterializationArgs(
        ingest_view_name=ingest_view_name,
        lower_bound_datetime_exclusive=parser.isoparse(
            date_pair[LOWER_BOUND_DATETIME_COL_NAME]
        )
        if date_pair[LOWER_BOUND_DATETIME_COL_NAME]
        else None,
        upper_bound_datetime_inclusive=parser.isoparse(
            date_pair[UPPER_BOUND_DATETIME_COL_NAME]
        ),
        ingest_instance=ingest_instance,
    )

    query = IngestViewMaterializerImpl.dataflow_query_for_args(
        view_builder=view_builder,
        raw_data_source_instance=ingest_instance,
        ingest_view_materialization_args=ingest_view_materialization_args,
    )
    logging.info(query)

    return beam.io.ReadFromBigQueryRequest(query=query, use_standard_sql=True)
