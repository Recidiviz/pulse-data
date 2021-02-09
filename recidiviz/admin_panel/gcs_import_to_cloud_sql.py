# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements admin panel route for importing GCS to Cloud SQL."""
import logging
from typing import List

from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.sqlalchemy_engine_manager import SQLAlchemyEngineManager, SchemaType


def import_gcs_csv_to_cloud_sql(destination_table: str,
                                gcs_uri: GcsfsFilePath,
                                columns: List[str]) -> None:
    """Implements the import of GCS CSV to Cloud SQL by creating a temporary table, uploading the
    results to the temporary table, and then swapping the contents of the table."""
    # Create temporary table
    engine = SQLAlchemyEngineManager.get_engine_for_schema_base(
        SQLAlchemyEngineManager.declarative_method_for_schema(SchemaType.CASE_TRIAGE)
    )
    if engine is None:
        raise RuntimeError('Could not create postgres sqlalchemy engine')

    with engine.connect() as conn:
        conn.execute(f'CREATE TABLE tmp__{destination_table} AS TABLE {destination_table} WITH NO DATA')

    try:
        # Start actual Cloud SQL import
        logging.info('Starting import from GCS URI: %s', gcs_uri)
        logging.info('Starting import to destination table: %s', destination_table)
        logging.info('Starting import using columns: %s', columns)
        cloud_sql_client = CloudSQLClientImpl()
        instance_name = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(SchemaType.CASE_TRIAGE)
        if instance_name is None:
            raise ValueError('Could not find instance name.')
        operation_id = cloud_sql_client.import_gcs_csv(
            instance_name=instance_name,
            table_name=f'tmp__{destination_table}',
            gcs_uri=gcs_uri,
            columns=columns,
        )
        if operation_id is None:
            raise RuntimeError('Cloud SQL import operation was not started successfully.')

        operation_succeeded = cloud_sql_client.wait_until_operation_completed(operation_id)

        if not operation_succeeded:
            raise RuntimeError('Cloud SQL import failed.')
    except Exception as e:
        logging.warning('Dropping newly created table due to raised exception.')
        conn.execute(f'DROP TABLE tmp__{destination_table}')
        raise e

    # Swap in new table
    with engine.begin() as conn:
        conn.execute(f'ALTER TABLE {destination_table} RENAME TO old__{destination_table}')
        conn.execute(f'ALTER TABLE tmp__{destination_table} RENAME TO {destination_table}')
        conn.execute(f'DROP TABLE old__{destination_table}')
