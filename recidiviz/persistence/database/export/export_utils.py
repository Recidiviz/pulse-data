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

"""Exports data from the state dataset in BigQuery to another dataset in BigQuery."""

import datetime
import logging
from typing import List, Optional, cast

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import ExportQueryConfig, BigQueryClient
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.persistence.database.base_schema import StateBase

# TODO(#4302): Remove unused schema module imports
# We need to import this to ensure that the Sql Alchemy table metadata referenced by StateBase is accessible later
# pylint:disable=unused-import
from recidiviz.persistence.database.schema.state import schema


def copy_table_to_dataset(target_dataset: str,
                          target_table: str,
                          export_query: str,
                          bq_client: BigQueryClient) -> None:
    """Copies the results of the given query to the target table and dataset, overwriting what lives there if the
    table already exists."""
    bq_client.create_table_from_query_async(target_dataset, target_table, export_query, [], True)


def export_tables_to_cloud_storage(export_configs: List[ExportQueryConfig], bq_client: BigQueryClient) -> None:
    """Exports tables with the given export configurations to Google Cloud Storage."""
    bq_client.export_query_results_to_cloud_storage(export_configs)


def gcs_export_directory(bucket_name: str, today: datetime.date, state_code: str) -> GcsfsDirectoryPath:
    """Returns a GCS directory to export files into, of the format:
    gs://{bucket_name}/ingested_state_data/{state_code}/{YYYY}/{MM}/{DD}
    """
    path = GcsfsDirectoryPath.from_bucket_and_blob_name(
        bucket_name=bucket_name,
        blob_name=f'ingested_state_data/{state_code}/{today.year:04}/{today.month:02}/{today.day:02}/')
    return cast(GcsfsDirectoryPath, path)


def format_columns_for_sql(columns: List[str], table_prefix: Optional[str] = None) -> str:
    if table_prefix:
        return ','.join(map(lambda col: f'{table_prefix}.{col}', columns))
    return ','.join(columns)


def format_region_codes_for_sql(region_codes: List[str]) -> str:
    """Format a list of region codes to use in a SQL string
        format_region_codes_for_sql(['US_ND']) --> "'US_ND'"
        format_region_codes_for_sql(['US_ND', 'US_PA']) --> "'US_ND', 'US_PA'"
    """
    return ','.join([f"\'{region_code.upper()}\'" for region_code in region_codes])


def state_code_in_clause(state_codes: List[str]) -> str:
    """Converts a list of state codes into a well-formatted SQL clause, e.g.
    state_code_in_clause(['US_MO', 'US_PA']) -> "state_code in ('US_MO', 'US_PA')"
    state_code_in_clause(['US_ID']) --> "state_code in ('US_ID')"
    """
    formatted_region_codes = format_region_codes_for_sql(state_codes)
    return f'state_code in ({formatted_region_codes})'


def state_table_export_query_str(table: bigquery.table.TableListItem, state_codes: List[str]) -> Optional[str]:
    """Returns a query string that can retrieve the data from the given table for the given states."""
    source_table = table.table_id
    source_dataset = table.dataset_id
    project_id = table.project

    if source_table.endswith('_history'):
        logging.info('Skipping export of table [%s]', source_table)
        return None

    if source_dataset != STATE_BASE_DATASET:
        raise ValueError('Received export request for a table not in the `state` dataset, which is required. '
                         f'Was in dataset [{source_dataset}] instead')

    sqlalchemy_table = None

    for t in StateBase.metadata.sorted_tables:
        if t.name == source_table:
            sqlalchemy_table = t

    if sqlalchemy_table is None:
        raise EnvironmentError('Unexpectedly did not find a table in SQLAlchemy metadata '
                               f'matching source table [{source_table}]')

    state_code_clause = state_code_in_clause(state_codes)

    select_query = f'SELECT * FROM {project_id}.{source_dataset}.{source_table}'

    filter_clause = f'WHERE {state_code_clause}'

    return f'{select_query} {filter_clause};'
