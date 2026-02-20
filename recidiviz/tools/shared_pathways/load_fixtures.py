# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
Tool for loading fixture data into our Pathways development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables, re-add them from the fixture files or a GCS bucket, and
update the local cache.

Usage against default development database (docker-compose v2)

PATHWAYS:
docker exec pulse-data-case_triage_backend-1 uv run python -m recidiviz.tools.shared_pathways.load_fixtures \
    --data_type GCS \
    --state_codes US_TN US_ID \
    --tables liberty_to_prison_transitions supervision_to_prison_transitions \
    --database PATHWAYS \
    --gcs_bucket recidiviz-staging-dashboard-event-level-data

PUBLIC_PATHWAYS:
docker exec pulse-data-public_pathways_backend-1 uv run python -m recidiviz.tools.shared_pathways.load_fixtures \
    --data_type FIXTURE \
    --state_codes US_NY \
    --database PUBLIC_PATHWAYS

Note that when running with FIXTURE data and the --tables parameter, metric_metadata will need to be
explicitly specified if you'd like to load it, whereas with GCS it is updated automatically for each
table.

WARNING: These tables take up multiple GB of space, and loading all data for all states is likely to
impact your computer's performance. Use the `state_codes` and `tables` parameters to only load the
data you need for development at a given moment, and reset tables you don't need the data for
anymore by overwriting them with fixture data.
"""
import argparse
import logging
import os
import sys
from types import ModuleType
from typing import List, Optional, Tuple

from sqlalchemy.engine import Engine

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_local_development,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.public_pathways.public_pathways_views import (
    PUBLIC_PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.case_triage.shared_pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.shared_pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.case_triage.shared_pathways.utils import get_metrics_for_entity
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.public_pathways import (
    schema as public_pathways_schema,
)
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


def get_table_columns(table: SQLAlchemyModelType, schema_type: SchemaType) -> List[str]:
    view_builder_columns = None
    view_builders = (
        PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS
        if schema_type == SchemaType.PATHWAYS
        else PUBLIC_PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS
    )

    for pathways_view_builder in view_builders:
        if pathways_view_builder.view_id == table.__tablename__:
            view_builder_columns = pathways_view_builder.delegate.columns
    if not view_builder_columns:
        raise ValueError(f"missing view builder {table.__tablename__}")
    return view_builder_columns


def import_pathways_from_gcs(
    engine: Engine,
    tables: List[SQLAlchemyModelType],
    gcs_bucket: str,
    state: str,
    gcsfs: GCSFileSystem,
    schema: ModuleType,
    schema_type: SchemaType,
) -> None:
    """Imports data from a given GCS bucket into the specified tables using the provided SQLALchemy Engine"""
    metadata_model = schema.MetricMetadata
    connection = engine.raw_connection()
    metadata_model.metadata.create_all(engine, tables=[metadata_model.__table__])

    for table in tables:
        if table == metadata_model:
            # We'll import into MetricMetadata for each metric we import
            continue
        table_name = table.__tablename__

        # Recreate table
        logging.info("dropping table %s.%s", state, table_name)
        metadata_model.metadata.drop_all(engine, tables=[table.__table__])
        logging.info("creating table %s.%s", state, table_name)
        metadata_model.metadata.create_all(engine, tables=[table.__table__])

        # Import CSV from GCS
        gcs_path = f"gs://{gcs_bucket}/{state}/{table_name}.csv"
        logging.info("importing into %s.%s from %s", state, table_name, gcs_path)
        gcsfs_path = GcsfsFilePath.from_absolute_path(gcs_path)
        with gcsfs.open(gcsfs_path) as fp:
            cursor = connection.cursor()
            cursor.copy_expert(
                f"COPY {table_name} ({','.join(get_table_columns(table, schema_type))}) FROM STDIN CSV",
                fp,
            )
            cursor.close()
            connection.commit()
        object_metadata = gcsfs.get_metadata(gcsfs_path) or {}
        last_updated = object_metadata.get("last_updated", None)
        dynamic_filter_options = object_metadata.get("dynamic_filter_options", None)
        # facility_id_name_map is nullable, so we will add it whether or not it exists
        if last_updated:
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT INTO {metadata_model.__tablename__} (metric, last_updated, dynamic_filter_options)
                VALUES(%s, %s, %s)
                ON CONFLICT (metric) DO UPDATE SET last_updated=EXCLUDED.last_updated, dynamic_filter_options=EXCLUDED.dynamic_filter_options""",
                (table.__name__, last_updated, dynamic_filter_options),
            )
            cursor.close()
            connection.commit()

    connection.close()


def reset_pathways_fixtures(
    engine: Engine,
    tables: List[SQLAlchemyModelType],
    state: str,
    schema: ModuleType,
    fixture_dir: str,
) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    metadata = schema.MetricMetadata.metadata

    # Reset_fixtures doesn't handle schema changes, so drop and recreate the tables
    for table in tables:
        logging.info("dropping table %s.%s", state, table.__tablename__)
        metadata.drop_all(engine, tables=[table.__table__])
        logging.info("creating table %s.%s", state, table.__tablename__)
        metadata.create_all(engine, tables=[table.__table__])

    reset_fixtures(
        engine=engine,
        tables=tables,
        fixture_directory=fixture_dir,
        csv_headers=True,
    )


def main(
    data_type: str,
    state_codes: List[str],
    tables: Optional[List[str]],
    gcs_bucket: str,
    schema_type: SchemaType,
) -> None:
    """Creates and initializes databases, then resets the fixtures or imports data from GCS"""
    create_dbs(state_codes, schema_type)

    if tables is None:
        tables = [table.name for table in get_all_table_classes_in_schema(schema_type)]
    if schema_type == SchemaType.PATHWAYS:
        schema_file: ModuleType = pathways_schema
        fixture_dir = os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tests/case_triage/pathways/fixtures",
        )
    else:
        schema_file = public_pathways_schema
        fixture_dir = os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tests/public_pathways/fixtures",
        )

    table_classes = [
        get_database_entity_by_table_name(schema_file, table) for table in tables
    ]

    for state in state_codes:
        database_key = PathwaysDatabaseManager(
            state_codes, schema_type
        ).database_key_for_state(state)
        pathways_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)
        if pathways_engine is None:
            pathways_engine = SQLAlchemyEngineManager.init_engine(database_key)

        if data_type == "FIXTURE":
            reset_pathways_fixtures(
                pathways_engine, table_classes, state, schema_file, fixture_dir
            )
        elif data_type == "GCS":
            import_pathways_from_gcs(
                pathways_engine,
                table_classes,
                gcs_bucket,
                state,
                GcsfsFactory.build(),
                schema_file,
                schema_type,
            )

    # Reset cache after all fixtures have been added because PathwaysMetricCache will initialize
    # a DB engine, and we can't import the metrics if the engine has already been initialized.
    for state in state_codes:
        metric_cache = PathwaysMetricCache.build(
            StateCode(state),
            schema_type=schema_type,
            metadata_model=schema_file.MetricMetadata,
            enabled_states=state_codes,
        )
        for table in table_classes:
            for metric in get_metrics_for_entity(table):
                logging.info("resetting cache for %s %s", state, metric.name)
                metric_cache.reset_cache(metric)


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
        choices=get_pathways_enabled_states_for_local_development(),
        default=get_pathways_enabled_states_for_local_development(),
    )

    parser.add_argument(
        "--tables",
        help="Space-separated tables to load data into. If unset, loads data into all tables.",
        type=str,
        nargs="*",
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to read GCS data from. If empty and data_type=GCS, reads from the staging dashboard-event-level-data bucket.",
        default="recidiviz-staging-dashboard-event-level-data",
    )

    parser.add_argument(
        "--database",
        help="The database to load data into. Either PATHWAYS or PUBLIC_PATHWAYS",
        choices=[
            "PATHWAYS",
            "PUBLIC_PATHWAYS",
        ],
        default="PATHWAYS",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    args, _ = parse_arguments(sys.argv)

    main(
        args.data_type,
        args.state_codes,
        args.tables,
        args.gcs_bucket,
        schema_type=SchemaType[args.database],
    )
