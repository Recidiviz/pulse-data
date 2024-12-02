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
"""A script for converting legacy to new raw data metadata.


Usage: 

python -m recidiviz.tools.ingest.one_offs.backfill_new_raw_data_infra_from_legacy \
    --project-id recidiviz-staging \
    --state-code US_ID \
    --raw-data-instance PRIMARY \
    --dry-run True


"""
import argparse
import logging
import sys

import sqlalchemy

from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import is_raw_data_import_dag_enabled
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
    DirectIngestRawDataResourceLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.cloud_sql_connection_mixin import (
    CloudSqlConnectionMixin,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

LOCK_DESCRIPTION = "LOCK ACQUIRED FOR BACKFILLING NEW RAW DATA OPERATIONS DB"


# TODO(#28239) remove once new raw data import infra is rolled out
class BackfillNewRawDataInfraFromLegacy(CloudSqlConnectionMixin):
    """Class that converts legacy to new raw data metadata."""

    def __init__(
        self,
        *,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        project_id: str,
        with_proxy: bool,
    ) -> None:
        self.state_code = state_code
        self.raw_data_instance = raw_data_instance
        self.project_id = project_id
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.with_proxy = with_proxy

    def ensure_no_conflicting_ids(
        self,
        *,
        session: Session,
    ) -> None:
        query = f"""
        WITH legacy_pks AS (
            SELECT file_id as pk
            FROM direct_ingest_raw_file_metadata
            WHERE 
                region_code = '{self.state_code.value.upper()}'
                AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
                AND is_invalidated IS FALSE
        )
        SELECT l.pk as colliding_pk
        FROM legacy_pks l
        INNER JOIN direct_ingest_raw_big_query_file_metadata b
        ON l.pk = b.file_id
        """
        results = session.execute(sqlalchemy.text(query))
        if len(conflicting_ids := list(results)) != 0:
            raise ValueError(f"Found [{len(conflicting_ids)}] conflicting ids")
        logging.info("Found no conflicting ids; proceeding...")

    def preview_backfill(
        self,
        *,
        session: Session,
    ) -> None:

        command = f"""
        SELECT file_tag, count(*)
        FROM direct_ingest_raw_file_metadata
        WHERE 
        region_code = '{self.state_code.value.upper()}'
        AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
        AND is_invalidated IS FALSE
        GROUP BY file_tag
        """
        results = session.execute(sqlalchemy.text(command))
        formatted_tags = "\n".join(
            [f"\t-{file_tag}: {count}" for file_tag, count in results]
        )
        logging.info("Found the following rows to copy over: \n %s", formatted_tags)

    def insert_gcs_files(
        self,
        *,
        session: Session,
    ) -> None:
        command = f"""
        INSERT INTO direct_ingest_raw_gcs_file_metadata (file_id, region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time)
        SELECT file_id, region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time 
        FROM direct_ingest_raw_file_metadata
        WHERE 
        region_code = '{self.state_code.value.upper()}'
        AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
        AND is_invalidated IS FALSE
        """
        result = session.execute(sqlalchemy.text(command))
        logging.info("Added [%s] rows to gcs metadata table", result.rowcount)

    def insert_bq_files(
        self,
        *,
        session: Session,
    ) -> None:
        command = f"""
        INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, is_invalidated, file_tag, update_datetime, file_processed_time)
        SELECT file_id, region_code, raw_data_instance, is_invalidated, file_tag, update_datetime, file_processed_time 
        FROM direct_ingest_raw_file_metadata
        WHERE 
        region_code = '{self.state_code.value.upper()}'
        AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
        AND is_invalidated IS FALSE
        """
        result = session.execute(sqlalchemy.text(command))
        logging.info("Added [%s] rows to big query metadata table", result.rowcount)

    def backfill(
        self,
        *,
        dry_run: bool,
    ) -> None:
        """Backfills new raw data metadata db tables using the legacy metadata table."""
        lock_manager = DirectIngestRawDataResourceLockManager(
            region_code=self.state_code.value,
            raw_data_source_instance=self.raw_data_instance,
            with_proxy=self.with_proxy,
        )

        locks = lock_manager.acquire_lock_for_resources(
            resources=[DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE],
            actor=DirectIngestRawDataLockActor.ADHOC,
            description=LOCK_DESCRIPTION,
            ttl_seconds=60 * 60,
        )

        if not dry_run and not is_raw_data_import_dag_enabled(
            state_code=self.state_code,
            raw_data_instance=self.raw_data_instance,
            project_id=self.project_id,
        ):
            raise ValueError(
                "Cannot execute backfill metadata db until new raw data infra has been turned on"
            )

        try:
            with self.get_session(
                database_key=self.database_key, with_proxy=self.with_proxy
            ) as session:
                self.ensure_no_conflicting_ids(session=session)

                self.preview_backfill(session=session)

                prompt_for_confirmation(
                    f"Proceed with backfill? [dry run mode: {dry_run}]"
                )

                if not dry_run:
                    logging.info("Executing backfill")
                    self.insert_bq_files(session=session)
                    self.insert_gcs_files(session=session)
                else:
                    logging.info("Dry run, skipping backfill")
        finally:
            for lock in locks:
                lock_manager.release_lock_by_id(lock.lock_id)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument("--state-code", type=StateCode, required=True)

    parser.add_argument("--raw-data-instance", type=DirectIngestInstance, required=True)

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = create_parser().parse_args()
    if not args.dry_run:
        prompt_for_confirmation(
            f"Have you validated that raw data import can successfully import in the new infra in [{args.state_code.value}] and [{args.raw_data_instance.value}]"
        )

    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            manager = BackfillNewRawDataInfraFromLegacy(
                state_code=args.state_code,
                raw_data_instance=args.raw_data_instance,
                project_id=args.project_id,
                with_proxy=True,
            )

            manager.backfill(dry_run=args.dry_run)
