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
from typing import List, Optional

from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def _import_csv_to_temp_table(
    database_key: SQLAlchemyDatabaseKey,
    schema_type: SchemaType,
    destination_table: str,
    tmp_table_name: str,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    seconds_to_wait: int,
) -> None:
    """Imports a GCS CSV file to a temp table that is created with the destination table as a template."""
    with SessionFactory.using_database(database_key=database_key) as session:
        # Drop old temp table if exists, Create new temp table
        session.execute(f"DROP TABLE IF EXISTS {tmp_table_name}")
        session.execute(
            f"CREATE TABLE {tmp_table_name} AS TABLE {destination_table} WITH NO DATA"
        )

    # Import CSV to temp table
    logging.info("Starting import from GCS URI: %s", gcs_uri)
    logging.info("Starting import to tmp destination table: %s", tmp_table_name)
    logging.info("Starting import using columns: %s", columns)

    cloud_sql_client = CloudSQLClientImpl()
    instance_name = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(
        schema_type=schema_type
    )
    if instance_name is None:
        raise ValueError("Could not find instance name.")

    # Create temp table with CSV file
    operation_id = cloud_sql_client.import_gcs_csv(
        instance_name=instance_name,
        table_name=tmp_table_name,
        gcs_uri=gcs_uri,
        columns=columns,
    )

    if operation_id is None:
        raise RuntimeError("Cloud SQL import operation was not started successfully.")

    operation_succeeded = cloud_sql_client.wait_until_operation_completed(
        operation_id=operation_id,
        seconds_to_wait=seconds_to_wait,
    )

    if not operation_succeeded:
        raise RuntimeError(f"Cloud SQL import to {tmp_table_name} failed.")


def import_gcs_csv_to_cloud_sql(
    schema_type: SchemaType,
    destination_table: str,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    region_code: Optional[str] = None,
    seconds_to_wait: int = 60 * 5,  # 5 minutes
) -> None:
    """Implements the import of GCS CSV to Cloud SQL by creating a temporary table, uploading the
    results to the temporary table, and then swapping the contents of the table.

    If a region_code is provided, selects all rows in the destination_table that do not equal the region_code and
    inserts them into the temp table before swapping.
    """
    tmp_table_name = f"tmp__{destination_table}"
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=schema_type)
    _import_csv_to_temp_table(
        database_key=database_key,
        schema_type=schema_type,
        destination_table=destination_table,
        tmp_table_name=tmp_table_name,
        gcs_uri=gcs_uri,
        columns=columns,
        seconds_to_wait=seconds_to_wait,
    )

    with SessionFactory.using_database(database_key=database_key) as session:
        if region_code is not None:
            # Import the rest of the regions into temp table
            session.execute(
                f"INSERT INTO {tmp_table_name} SELECT * FROM {destination_table} "
                f"WHERE state_code != '{region_code}'"
            )

        # Swap the temp table and destination table, dropping the old destination table at the end.
        old_table_name = f"old__{destination_table}"
        session.execute(f"ALTER TABLE {destination_table} RENAME TO {old_table_name}")
        session.execute(f"ALTER TABLE {tmp_table_name} RENAME TO {destination_table}")
        session.execute(f"DROP TABLE {old_table_name}")
