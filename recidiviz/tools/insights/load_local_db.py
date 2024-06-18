# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Tool for loading data into the Insights development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables and then re-add them from the
specified source, i.e. staging GCS or fixture files.

Usage against default development database (docker-compose v1):
docker exec pulse-data_import_service_1 pipenv run python -m recidiviz.tools.insights.load_local_db \
    --data_type FIXTURE \
    --state_codes US_PA

Usage against default development database (docker-compose v2):
docker exec pulse-data-import_service-1 pipenv run python -m recidiviz.tools.insights.load_local_db \
    --data_type FIXTURE \
    --state_codes US_PA
"""
import argparse
import json
import logging
import os
import sys
from typing import IO, List, Tuple

from sqlalchemy.engine import Engine
from sqlalchemy.sql.ddl import DropTable

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    ModelSQL,
    _recreate_table,
    build_temporary_sqlalchemy_table,
    get_non_identity_columns_from_model,
    parse_exported_json_row_from_bigquery,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights import schema
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.insights import fixtures
from recidiviz.tools.utils.fixture_helpers import create_dbs
from recidiviz.utils.environment import in_development


def get_table_columns(table: SQLAlchemyModelType) -> List[str]:
    view_builder = None
    for builder in OUTLIERS_VIEW_BUILDERS:
        if builder.view_id == table.__tablename__:
            view_builder = builder
    if not view_builder:
        raise ValueError(f"missing view builder {table.__tablename__}")
    return (
        view_builder.columns
        if isinstance(view_builder, SelectedColumnsBigQueryViewBuilder)
        else []
    )


def _reset_insights_table(
    model: SQLAlchemyModelType, database_key: SQLAlchemyDatabaseKey, file: IO
) -> None:
    """
    Mimics logic from recidiviz.cloud_sql.gcs_import_to_cloud_sql.import_gcs_file_to_cloud_sql
    """
    destination_table = model.__table__
    destination_table_name = model.__tablename__

    # Generate DDL statements for the temporary table
    temporary_table = build_temporary_sqlalchemy_table(destination_table)
    temporary_table_model_sql = ModelSQL(table=temporary_table)

    with SessionFactory.using_database(database_key=database_key) as session:
        logging.info("Recreating table %s", temporary_table.name)
        _recreate_table(database_key, temporary_table_model_sql)

    model_columns = get_non_identity_columns_from_model(model)
    with SessionFactory.using_database(database_key) as session:
        entities = [
            parse_exported_json_row_from_bigquery(json.loads(row), model, model_columns)
            for row in file.readlines()
        ]

        session.execute(temporary_table.insert(), entities)
        session.commit()

    logging.info(
        "Dropping existing %s to replace with temporary table", destination_table_name
    )

    with SessionFactory.using_database(database_key=database_key) as session:
        # Drop the destination table
        session.execute(DropTable(destination_table, if_exists=True))

        rename_queries = temporary_table_model_sql.build_rename_ddl_queries(
            destination_table_name
        )

        # Rename temporary table and all indexes / constraint on the temporary table
        for query in rename_queries:
            session.execute(query)
        session.commit()


def reset_insights_fixtures(
    engine: Engine,
    database_key: SQLAlchemyDatabaseKey,
    tables: List[SQLAlchemyModelType],
    state: str,
    data_type: str,
    gcs_bucket: str,
) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    logging.info("dropping all tables for %s", state)
    InsightsBase.metadata.drop_all(engine)
    for table in tables:
        logging.info("creating table %s.%s", state, table.__tablename__)
        InsightsBase.metadata.create_all(engine, tables=[table.__table__])

    logging.info("Done creating tables")

    for table in tables:
        table_name = table.__tablename__
        logging.info("Hydrating table %s with %s data", table_name, data_type)
        if data_type == "FIXTURE":
            filename = f"{table_name}.json"
            fixture_path = os.path.join(os.path.dirname(fixtures.__file__), filename)
            with open(fixture_path, "r", encoding="UTF-8") as file:
                _reset_insights_table(table, database_key, file)

        elif data_type == "GCS":
            gcsfs = GcsfsFactory.build()
            gcs_path = f"gs://{gcs_bucket}/{state}/{table_name}.json"
            gcsfs_path = GcsfsFilePath.from_absolute_path(gcs_path)
            with gcsfs.open(gcsfs_path) as file:
                _reset_insights_table(table, database_key, file)


def main(
    data_type: str,
    state_codes: List[str],
    tables: List[SQLAlchemyModelType],
    gcs_bucket: str,
) -> None:
    create_dbs(state_codes, SchemaType.INSIGHTS)

    for state in state_codes:
        database_manager = StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.INSIGHTS
        )
        database_key = database_manager.database_key_for_state(state)
        insights_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)

        reset_insights_fixtures(
            insights_engine, database_key, tables, state, data_type, gcs_bucket
        )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_type",
        help="Type of data to load, defaults to FIXTURE.",
        type=str,
        choices=["FIXTURE", "GCS"],
        default="FIXTURE",
    )

    parser.add_argument(
        "--state_codes",
        help="Space-separated state codes to load data for. If unset, will load data for all states.",
        type=str,
        nargs="*",
        choices=get_outliers_enabled_states(),
        default=get_outliers_enabled_states(),
    )

    parser.add_argument(
        "--tables",
        help="Space-separated tables to load data into. If unset, loads data into all tables.",
        type=str,
        nargs="*",
        choices=[
            table.name
            for table in get_all_table_classes_in_schema(SchemaType.INSIGHTS)
            if table.name not in ["configurations", "user_metadata"]
        ],
        default=[
            table.name
            for table in get_all_table_classes_in_schema(SchemaType.INSIGHTS)
            if table.name not in ["configurations", "user_metadata"]
        ],
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to read GCS data from. If empty and data_type=GCS, reads from the staging insights-etl-data bucket.",
        default="recidiviz-staging-insights-etl-data",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    args, _ = parse_arguments(sys.argv)
    table_classes = [
        get_database_entity_by_table_name(schema, table) for table in args.tables
    ]
    main(args.data_type, args.state_codes, table_classes, args.gcs_bucket)
