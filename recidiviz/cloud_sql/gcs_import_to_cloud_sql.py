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
from typing import Any, Dict, List, Optional

import attr
from sqlalchemy import Table, create_mock_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import (
    CreateIndex,
    CreateTable,
    DDLElement,
    DropTable,
    SetColumnComment,
)

from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOpportunity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def get_temporary_table_name(table: Table) -> str:
    return f"tmp__{table.name}"


temporary_etl_clients_name = get_temporary_table_name(ETLClient.__table__)
temporary_etl_opps_name = get_temporary_table_name(ETLOpportunity.__table__)

ADDITIONAL_DDL_QUERIES_BY_MODEL = {
    # TODO(#8579): Remove when the duplicates in `etl_clients` have been removed
    ETLClient: [
        f"ALTER TABLE {temporary_etl_clients_name} DROP CONSTRAINT {temporary_etl_clients_name}_pkey;",
        f"ALTER TABLE {temporary_etl_clients_name} DROP CONSTRAINT {temporary_etl_clients_name}_state_code_person_external_id_key;",
    ],
    # TODO(#9292): Remove when the duplicates in `etl_opportunities` have been removed
    ETLOpportunity: [
        f"ALTER TABLE {temporary_etl_opps_name} DROP CONSTRAINT {temporary_etl_opps_name}_pkey;"
    ],
}


def build_temporary_sqlalchemy_table(table: Table) -> Table:
    # Create a throwaway Base to map the model to
    base = declarative_base()
    return table.to_metadata(
        base.metadata,
        # Replace our model's table name with the temporary table's name
        name=get_temporary_table_name(table),
    )


@attr.s
class ModelSQL:
    """Given a SQLAlchemy table, captures the DDL statements necessary to create the table"""

    table: Table = attr.ib()
    ddl_statements: List[DDLElement] = attr.ib(factory=list)

    def __attrs_post_init__(self) -> None:
        # Create a mock engine which captures the DDL statements
        engine = create_mock_engine("postgresql://", self._capture_query)

        # Creates the temporary table in the mock engine
        self.table.create(engine)

    def _capture_query(
        self, sql: DDLElement, *_args: List[Any], **_kwargs: Dict[str, Any]
    ) -> None:
        self.ddl_statements.append(sql)

    def build_rename_ddl_queries(self, new_base_name: str) -> List[str]:
        """Builds queries for renaming a table and its indexes"""
        queries = []

        for ddl_statement in self.ddl_statements:
            resource_name = ddl_statement.element.name

            if isinstance(ddl_statement, CreateTable):
                queries.append(f"ALTER TABLE {resource_name} RENAME TO {new_base_name}")
            elif isinstance(ddl_statement, CreateIndex):
                if self.table.name not in resource_name:
                    raise NotImplementedError(
                        f"Cannot rename indexes that do not contain the table's name ({self.table.name})"
                    )

                # SQLAlchemy includes the table name inside the index name by default;
                # Map the index to the new base name
                new_index_name = resource_name.replace(self.table.name, new_base_name)
                queries.append(
                    f"ALTER INDEX {resource_name} RENAME TO {new_index_name}"
                )
            elif isinstance(ddl_statement, SetColumnComment):
                # Column comments will automatically be moved when renaming the table
                continue
            else:
                raise ValueError(
                    f"Unsupported DDL statement: {ddl_statement.__class__}"
                )

        return queries

    @classmethod
    def from_model(cls, model: SQLAlchemyModelType) -> "ModelSQL":
        return cls(table=model.__table__)


def _recreate_table(database_key: SQLAlchemyDatabaseKey, model_sql: ModelSQL) -> None:
    """Drops the table if it exists then recreates the table with the provided schema"""
    with SessionFactory.using_database(database_key=database_key) as session:
        drop_table = DropTable(model_sql.table, if_exists=True)
        session.execute(drop_table)

        for ddl_statement in model_sql.ddl_statements:
            session.execute(ddl_statement)


def _import_csv_to_temp_table(
    schema_type: SchemaType,
    tmp_table_name: str,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    seconds_to_wait: int,
) -> None:
    """Imports a GCS CSV file to a temp table that is created with the destination table as a template."""

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
    model: SQLAlchemyModelType,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    region_code: Optional[str] = None,
    seconds_to_wait: int = 60 * 5,  # 5 minutes
    db_name: Optional[str] = None,
) -> None:
    """Implements the import of GCS CSV to Cloud SQL by creating a temporary table, uploading the
    results to the temporary table, and then swapping the contents of the table.

    If a region_code is provided, selects all rows in the destination_table that do not equal the region_code and
    inserts them into the temp table before swapping.
    """
    database_key = (
        SQLAlchemyDatabaseKey.for_schema(schema_type=schema_type)
        if db_name is None
        else SQLAlchemyDatabaseKey(schema_type=schema_type, db_name=db_name)
    )

    destination_table = model.__tablename__

    # Generate DDL statements for the temporary table
    temporary_table = build_temporary_sqlalchemy_table(model.__table__)
    temporary_table_model_sql = ModelSQL(table=temporary_table)

    with SessionFactory.using_database(database_key=database_key) as session:
        _recreate_table(database_key, temporary_table_model_sql)

        additional_ddl_queries = ADDITIONAL_DDL_QUERIES_BY_MODEL.get(model, [])

        for query in additional_ddl_queries:
            session.execute(query)

    _import_csv_to_temp_table(
        schema_type=schema_type,
        tmp_table_name=temporary_table.name,
        gcs_uri=gcs_uri,
        columns=columns,
        seconds_to_wait=seconds_to_wait,
    )

    with SessionFactory.using_database(database_key=database_key) as session:
        if region_code is not None:
            # Import the rest of the regions into temp table
            session.execute(
                f"INSERT INTO {temporary_table.name} SELECT * FROM {destination_table} "
                f"WHERE state_code != '{region_code}'"
            )

        # Drop the destination table
        session.execute(f"DROP TABLE {destination_table}")

        rename_queries = temporary_table_model_sql.build_rename_ddl_queries(
            destination_table
        )

        # Rename temporary table and all indexes / constraint on the temporary table
        for query in rename_queries:
            session.execute(query)
