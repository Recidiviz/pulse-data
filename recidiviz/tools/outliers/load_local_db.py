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
"""
Tool for loading data into the Outliers development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables and then re-add them from the
specified source, i.e. staging GCS or fixture files.

Usage against default development database (docker-compose v1):
docker exec pulse-data_import_service_1 pipenv run python -m recidiviz.tools.outliers.load_local_db \
    --data_type FIXTURE \
    --state_codes US_PA

Usage against default development database (docker-compose v2):
docker exec pulse-data-import_service-1 pipenv run python -m recidiviz.tools.outliers.load_local_db \
    --data_type FIXTURE \
    --state_codes US_PA
"""
import argparse
import logging
import os
import sys
from typing import List, Tuple

from sqlalchemy.engine import Engine

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.outliers import schema
from recidiviz.persistence.database.schema.outliers.schema import OutliersBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import create_dbs, reset_fixtures
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


def import_outliers_from_gcs(
    engine: Engine,
    tables: List[SQLAlchemyModelType],
    gcs_bucket: str,
    state: str,
    gcsfs: GCSFileSystem,
) -> None:
    """Imports data from a given GCS bucket into the specified tables using the provided SQLALchemy Engine"""
    connection = engine.raw_connection()

    logging.info("dropping all tables for %s", state)
    OutliersBase.metadata.drop_all(engine)

    for table in tables:
        table_name = table.__tablename__

        # Recreate table
        logging.info("creating table %s.%s", state, table_name)
        OutliersBase.metadata.create_all(engine, tables=[table.__table__])

        # Import CSV from GCS
        gcs_path = f"gs://{gcs_bucket}/{state}/{table_name}.csv"
        logging.info("importing into %s.%s from %s", state, table_name, gcs_path)
        gcsfs_path = GcsfsFilePath.from_absolute_path(gcs_path)
        with gcsfs.open(gcsfs_path) as fp:
            cursor = connection.cursor()
            cursor.copy_expert(
                f"COPY {table_name} ({','.join(get_table_columns(table))}) FROM STDIN CSV",
                fp,
            )
            cursor.close()
            connection.commit()

    connection.close()


def reset_outliers_fixtures(
    engine: Engine, tables: List[SQLAlchemyModelType], state: str
) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    logging.info("dropping all tables for %s", state)
    OutliersBase.metadata.drop_all(engine)
    for table in tables:
        logging.info("creating table %s.%s", state, table.__tablename__)
        OutliersBase.metadata.create_all(engine, tables=[table.__table__])

    reset_fixtures(
        engine=engine,
        tables=tables,
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tools/outliers/fixtures",
        ),
        csv_headers=True,
    )


def main(
    data_type: str,
    state_codes: List[str],
    tables: List[SQLAlchemyModelType],
    gcs_bucket: str,
) -> None:
    create_dbs(state_codes, SchemaType.OUTLIERS)

    for state in state_codes:
        database_manager = StateSegmentedDatabaseManager(
            get_outliers_enabled_states(), SchemaType.OUTLIERS
        )
        database_key = database_manager.database_key_for_state(state)
        outliers_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)

        if data_type == "FIXTURE":
            reset_outliers_fixtures(outliers_engine, tables, state)
        elif data_type == "GCS":
            import_outliers_from_gcs(
                outliers_engine, tables, gcs_bucket, state, GcsfsFactory.build()
            )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_type",
        help="Type of data to load, either GCS or FIXTURE. Defaults to FIXTURE.",
        type=str,
        choices=["GCS", "FIXTURE"],
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
            table.name for table in get_all_table_classes_in_schema(SchemaType.OUTLIERS)
        ],
        default=[
            table.name for table in get_all_table_classes_in_schema(SchemaType.OUTLIERS)
        ],
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to read GCS data from. If empty and data_type=GCS, reads from the staging outliers-etl-data bucket.",
        default="recidiviz-staging-outliers-etl-data",
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
