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
import json
import logging
from collections import defaultdict
from http import HTTPStatus
from typing import Any, Dict, List, Optional

import attr
from google.api_core import retry
from googleapiclient import errors
from sqlalchemy import Table, create_mock_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import (
    CreateIndex,
    CreateTable,
    DDLElement,
    DropTable,
    SetColumnComment,
)

from recidiviz.cloud_sql.cloud_sql_client import CloudSQLClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def get_temporary_table_name(table: Table) -> str:
    return f"tmp__{table.name}"


CONSTRAINTS_TO_DROP: dict[str, list[str]] = defaultdict(list)

ADDITIONAL_DDL_QUERIES_BY_TABLE_NAME: dict[str, list[str]] = {}


def retry_predicate(exception: BaseException) -> bool:
    """A function that will determine whether we should retry a given Google exception."""
    return (
        isinstance(exception, errors.HttpError)
        and exception.status_code == HTTPStatus.CONFLICT
    )


def build_temporary_sqlalchemy_table(table: Table) -> Table:
    # Create a throwaway Base to map the model to
    base = declarative_base()
    temporary_table = table.to_metadata(
        base.metadata,
        # Replace our model's table name with the temporary table's name
        name=get_temporary_table_name(table),
    )

    for index in temporary_table.indexes:
        index.name = get_renamed_ddl_name(
            index.name, table_name=table.name, new_base_name=temporary_table.name
        )

    for constraint in temporary_table.constraints:
        if constraint.name:
            constraint.name = get_renamed_ddl_name(
                constraint.name,
                table_name=table.name,
                new_base_name=temporary_table.name,
            )

    return temporary_table


def get_renamed_ddl_name(index_name: str, table_name: str, new_base_name: str) -> str:
    if table_name not in index_name:
        raise NotImplementedError(
            f"Cannot rename index ({index_name}) that does not contain the table's name ({table_name})"
        )

    # SQLAlchemy includes the table name inside the index name by default;
    # Map the index to the new base name
    return index_name.replace(table_name, new_base_name)


def build_constraint_rename_query(
    resource_name: str, table_name: str, new_base_name: str
) -> str:
    new_constraint_name = get_renamed_ddl_name(
        resource_name,
        table_name=table_name,
        new_base_name=new_base_name,
    )

    return f"ALTER TABLE {new_base_name} RENAME CONSTRAINT {resource_name} TO {new_constraint_name}"


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

                for constraint in ddl_statement.element.constraints:
                    if (
                        constraint.name
                        and constraint.name not in CONSTRAINTS_TO_DROP[self.table.name]
                    ):
                        queries.append(
                            build_constraint_rename_query(
                                resource_name=constraint.name,
                                table_name=self.table.name,
                                new_base_name=new_base_name,
                            )
                        )
            elif isinstance(ddl_statement, CreateIndex):
                new_index_name = get_renamed_ddl_name(
                    resource_name,
                    table_name=self.table.name,
                    new_base_name=new_base_name,
                )
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


@retry.Retry(predicate=retry_predicate)
def _import_csv_to_temp_table(
    database_key: SQLAlchemyDatabaseKey,
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
        schema_type=database_key.schema_type
    )
    if instance_name is None:
        raise ValueError("Could not find instance name.")

    # Create temp table with CSV file
    operation_id = cloud_sql_client.import_gcs_csv(
        instance_name=instance_name,
        db_name=database_key.db_name,
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
    database_key: SQLAlchemyDatabaseKey,
    model: SQLAlchemyModelType,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    region_code: Optional[str] = None,
    seconds_to_wait: int = 60 * 5,  # 5 minutes
) -> None:
    """Implements the import of GCS CSV to Cloud SQL by creating a temporary table, uploading the
    results to the temporary table, and then swapping the contents of the table.

    If a region_code is provided, selects all rows in the destination_table that do not equal the region_code and
    inserts them into the temp table before swapping.
    TODO(#29762): Deprecate in favor of import_gcs_file_to_cloud_sql
    """
    destination_table = model.__table__
    destination_table_name = model.__tablename__

    # Generate DDL statements for the temporary table
    temporary_table = build_temporary_sqlalchemy_table(destination_table)
    temporary_table_model_sql = ModelSQL(table=temporary_table)

    with SessionFactory.using_database(database_key=database_key) as session:
        _recreate_table(database_key, temporary_table_model_sql)

        additional_ddl_queries = ADDITIONAL_DDL_QUERIES_BY_TABLE_NAME.get(
            temporary_table.name, []
        )

        for query in additional_ddl_queries:
            session.execute(query)

    _import_csv_to_temp_table(
        database_key=database_key,
        tmp_table_name=temporary_table.name,
        gcs_uri=gcs_uri,
        columns=columns,
        seconds_to_wait=seconds_to_wait,
    )

    with SessionFactory.using_database(database_key=database_key) as session:
        if region_code is not None:
            # Import the rest of the regions into temp table
            session.execute(
                f"INSERT INTO {temporary_table.name} SELECT * FROM {destination_table_name} "
                f"WHERE state_code != '{region_code}'"
            )

        # Drop the destination table
        session.execute(DropTable(destination_table, if_exists=True))

        rename_queries = temporary_table_model_sql.build_rename_ddl_queries(
            destination_table_name
        )

        # Rename temporary table and all indexes / constraint on the temporary table
        for query in rename_queries:
            session.execute(query)


def parse_exported_json_row_from_bigquery(
    data: Any, model: SQLAlchemyModelType
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            result[key] = None
        elif isinstance(model.__table__.c[key].type, JSONB):
            result[key] = json.loads(value)
        elif value == "true":
            result[key] = True
        elif value == "false":
            result[key] = False
        else:
            result[key] = value
    return result


def _import_gcs_json_to_cloud_sql(
    database_key: SQLAlchemyDatabaseKey,
    gcs_uri: GcsfsFilePath,
    destination_table: Table,
    model: SQLAlchemyModelType,
) -> None:
    """
    Expects a newline delimited JSON file at the specified GCS URI path and
    reads each line to add new entities to the specified destination table.
    """
    gcsfs = GcsfsFactory.build()
    added_entities = 0
    with gcsfs.open(gcs_uri) as f, SessionFactory.using_database(
        database_key=database_key
    ) as session:
        for line in f:
            try:
                json_entity = json.loads(line)
                flattened_json_entity = parse_exported_json_row_from_bigquery(
                    json_entity, model
                )
                session.execute(
                    destination_table.insert().values(**flattened_json_entity)
                )
                added_entities += 1
            except Exception as e:
                # TODO(#29761): Consider counting number of failed rows and alerting based on threshold of failed row
                logging.error(
                    "Added %s entities before encountering exception: %s",
                    added_entities,
                    str(e),
                )
                raise e

            session.commit()

    logging.info(
        "Added %s entities in import to %s",
        added_entities,
        destination_table.name,
    )


def import_gcs_file_to_cloud_sql(
    database_key: SQLAlchemyDatabaseKey,
    model: SQLAlchemyModelType,
    gcs_uri: GcsfsFilePath,
    columns: List[str],
    region_code: Optional[str] = None,
    seconds_to_wait: int = 60 * 5,  # 5 minutes
) -> None:
    """
    Implements the import of GCS file to Cloud SQL by creating a temporary table, uploading the
    results to the temporary table, and then swapping the contents of the table.

    If a region_code is provided, selects all rows in the destination_table that do not equal the region_code and
    inserts them into the temp table before swapping.
    """

    destination_table = model.__table__
    destination_table_name = model.__tablename__

    # Generate DDL statements for the temporary table
    temporary_table = build_temporary_sqlalchemy_table(destination_table)
    temporary_table_model_sql = ModelSQL(table=temporary_table)

    with SessionFactory.using_database(database_key=database_key) as session:
        # Drop (if it exists) and create temporary table for the given model
        _recreate_table(database_key, temporary_table_model_sql)

        additional_ddl_queries = ADDITIONAL_DDL_QUERIES_BY_TABLE_NAME.get(
            temporary_table.name, []
        )

        for query in additional_ddl_queries:
            session.execute(query)

    _, file_type = gcs_uri.blob_name.split(".")

    # Import file to destination table table
    logging.info("Starting import from GCS URI: %s", gcs_uri)
    logging.info("Starting import to destination table: %s", destination_table.name)
    logging.info("Starting import using columns: %s", destination_table.columns)

    if file_type == "csv":
        _import_csv_to_temp_table(
            database_key=database_key,
            tmp_table_name=temporary_table.name,
            gcs_uri=gcs_uri,
            columns=columns,
            seconds_to_wait=seconds_to_wait,
        )
    elif file_type == "json":
        # Imports JSON data to the temporary table
        _import_gcs_json_to_cloud_sql(
            database_key=database_key,
            gcs_uri=gcs_uri,
            destination_table=temporary_table,
            model=model,
        )
    else:
        raise ValueError(f"Unexpected file type ({file_type}): {gcs_uri.abs_path()}")

    with SessionFactory.using_database(database_key=database_key) as session:
        if region_code is not None:
            # Import the rest of the regions into temp table
            session.execute(
                f"INSERT INTO {temporary_table.name} SELECT * FROM {destination_table_name} "
                f"WHERE state_code != '{region_code}'"
            )

        # Drop the destination table
        session.execute(DropTable(destination_table, if_exists=True))

        rename_queries = temporary_table_model_sql.build_rename_ddl_queries(
            destination_table_name
        )

        # Rename temporary table and all indexes / constraint on the temporary table
        for query in rename_queries:
            session.execute(query)
