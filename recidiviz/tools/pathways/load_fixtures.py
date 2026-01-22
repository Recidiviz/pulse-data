# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

Usage against default development database (docker-compose v2):
docker exec pulse-data-case_triage_backend-1 uv run python -m recidiviz.tools.pathways.load_fixtures \
    --data_type GCS \
    --state_codes US_TN US_ID \
    --tables liberty_to_prison_transitions supervision_to_prison_transitions \
    --gcs_bucket recidiviz-staging-dashboard-event-level-data

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
from typing import List, Tuple

from sqlalchemy.engine import Engine

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_local_development,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.case_triage.pathways.enabled_metrics import get_metrics_for_entity
from recidiviz.case_triage.pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.shared_pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.pathways.schema import (
    MetricMetadata,
    PathwaysBase,
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


def get_table_columns(table: SQLAlchemyModelType) -> List[str]:
    view_builder_columns = None
    for pathways_view_builder in PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS:
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
) -> None:
    """Imports data from a given GCS bucket into the specified tables using the provided SQLALchemy Engine"""
    connection = engine.raw_connection()
    PathwaysBase.metadata.create_all(engine, tables=[MetricMetadata.__table__])

    for table in tables:
        if table == MetricMetadata:
            # We'll import into MetricMetadata for each metric we import
            continue
        table_name = table.__tablename__

        # Recreate table
        logging.info("dropping table %s.%s", state, table_name)
        PathwaysBase.metadata.drop_all(engine, tables=[table.__table__])
        logging.info("creating table %s.%s", state, table_name)
        PathwaysBase.metadata.create_all(engine, tables=[table.__table__])

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
        object_metadata = gcsfs.get_metadata(gcsfs_path) or {}
        last_updated = object_metadata.get("last_updated", None)
        facility_id_name_map = object_metadata.get("facility_id_name_map", None)
        # facility_id_name_map is nullable, so we will add it whether or not it exists
        if last_updated:
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT INTO {MetricMetadata.__tablename__} (metric, last_updated, facility_id_name_map)
                VALUES(%s, %s, %s)
                ON CONFLICT (metric) DO UPDATE SET last_updated=EXCLUDED.last_updated, facility_id_name_map=EXCLUDED.facility_id_name_map""",
                (table.__name__, last_updated, facility_id_name_map),
            )
            cursor.close()
            connection.commit()

    connection.close()


def reset_pathways_fixtures(
    engine: Engine, tables: List[SQLAlchemyModelType], state: str
) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    # Reset_fixtures doesn't handle schema changes, so drop and recreate the tables
    for table in tables:
        logging.info("dropping table %s.%s", state, table.__tablename__)
        PathwaysBase.metadata.drop_all(engine, tables=[table.__table__])
        logging.info("creating table %s.%s", state, table.__tablename__)
        PathwaysBase.metadata.create_all(engine, tables=[table.__table__])

    reset_fixtures(
        engine=engine,
        tables=tables,
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tests/case_triage/pathways/fixtures",
        ),
        csv_headers=True,
    )


def main(
    data_type: str,
    state_codes: List[str],
    tables: List[SQLAlchemyModelType],
    gcs_bucket: str,
) -> None:
    create_dbs(state_codes, SchemaType.PATHWAYS)

    for state in state_codes:
        database_key = PathwaysDatabaseManager(
            state_codes, SchemaType.PATHWAYS
        ).database_key_for_state(state)
        pathways_engine = SQLAlchemyEngineManager.init_engine(database_key)

        if data_type == "FIXTURE":
            reset_pathways_fixtures(pathways_engine, tables, state)
        elif data_type == "GCS":
            import_pathways_from_gcs(
                pathways_engine, tables, gcs_bucket, state, GcsfsFactory.build()
            )

    # Reset cache after all fixtures have been added because PathwaysMetricCache will initialize
    # a DB engine, and we can't import the metrics if the engine has already been initialized.
    for state in state_codes:
        metric_cache = PathwaysMetricCache.build(StateCode(state))
        for table in tables:
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
        choices=[
            table.name for table in get_all_table_classes_in_schema(SchemaType.PATHWAYS)
        ],
        default=[
            table.name for table in get_all_table_classes_in_schema(SchemaType.PATHWAYS)
        ],
    )

    parser.add_argument(
        "--gcs_bucket",
        help="The bucket to read GCS data from. If empty and data_type=GCS, reads from the staging dashboard-event-level-data bucket.",
        default="recidiviz-staging-dashboard-event-level-data",
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
        get_database_entity_by_table_name(pathways_schema, table)
        for table in args.tables
    ]
    main(args.data_type, args.state_codes, table_classes, args.gcs_bucket)
