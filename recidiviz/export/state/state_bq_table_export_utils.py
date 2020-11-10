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

import logging
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import \
    BigQuerySchemaTableRegionFilteredQueryBuilder


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

    column_names = [column.name for column in sqlalchemy_table.columns]
    query_builder = BigQuerySchemaTableRegionFilteredQueryBuilder(
        project_id=project_id,
        dataset_id=table.dataset_id,
        metadata_base=StateBase,
        table=sqlalchemy_table,
        columns_to_include=column_names,
        region_codes_to_include=state_codes
    )

    return f'{query_builder.full_query()};'
