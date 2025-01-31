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
from itertools import groupby
from typing import Iterator

import attr
import sqlalchemy

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
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


@attr.define(frozen=True)
class PossiblyConflictingId:
    file_id: int
    file_tag: str


@attr.define(frozen=True)
class ReplacedFileId:
    old_file_id: int
    new_file_id: int
    file_tag: str

    def as_db_tuple(self) -> str:
        return f"({self.old_file_id}, {self.new_file_id})"

    @property
    def requires_update_in_bq(self) -> bool:
        return self.old_file_id != self.new_file_id


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

    def find_conflicting_ids(self, *, session: Session) -> set[PossiblyConflictingId]:
        """Identifies all ids that are possibly conflicting between the new and
        legacy dbs.
        """
        query = f"""
        WITH legacy_pks AS (
            SELECT file_id as pk
            FROM direct_ingest_raw_file_metadata
            WHERE 
                region_code = '{self.state_code.value.upper()}'
                AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
                AND is_invalidated IS FALSE
        )
        SELECT l.pk as colliding_pk, b.file_tag as colliding_file_tag
        FROM legacy_pks l
        INNER JOIN direct_ingest_raw_big_query_file_metadata b
        ON l.pk = b.file_id
        """
        return {
            PossiblyConflictingId(file_id=int(result[0]), file_tag=result[1])
            for result in session.execute(sqlalchemy.text(query))
        }

    def _build_update_query(
        self, *, file_tag: str, replaced_ids: Iterator[ReplacedFileId]
    ) -> str:
        table_name = f"{self.project_id}.{raw_tables_dataset_for_region(state_code=self.state_code, instance=self.raw_data_instance)}.{file_tag}"

        return f"""
            UPDATE `{table_name}` SET file_id = new_file_id
            FROM (
                SELECT * FROM UNNEST([
                    STRUCT<`old_file_id` INT64, `new_file_id` INT64>
                    {','.join(replaced.as_db_tuple() for replaced in replaced_ids)}
                ])
            ) n
            WHERE old_file_id = file_id;"""

    def update_conflicting_ids_in_bq(
        self, *, replaced_file_ids: list[ReplacedFileId]
    ) -> None:
        """Updates all file_id references in BigQuery, using the mapping in each
        |replaced_file_ids| objet.
        """
        requires_update_in_bq = [
            replaced for replaced in replaced_file_ids if replaced.requires_update_in_bq
        ]

        if not requires_update_in_bq:
            logging.info("found nothing to update in bq... skipping")
            return

        updates_by_file_tag = groupby(
            sorted(requires_update_in_bq, key=lambda x: x.file_tag),
            key=lambda x: x.file_tag,
        )

        queries = [
            self._build_update_query(file_tag=file_tag, replaced_ids=file_ids_to_update)
            for file_tag, file_ids_to_update in updates_by_file_tag
        ]

        bq_client = BigQueryClientImpl(project_id=self.project_id)

        logging.info("updating bq tables")

        for q in queries:
            job = bq_client.run_query_async(query_str=q, use_query_cache=False)
            try:
                job.result()
            except Exception as e:
                logging.exception(
                    "QUERY %s failed to run: %s \n\n\n %s", q, job.errors, e
                )
                raise e

    def ensure_no_unhandled_conflicting_ids(
        self, *, session: Session, should_replace_file_ids: bool
    ) -> None:
        """Ensures that all conflicting ids will be properly handled; that is,
        if there are conflicting ids then |should_replace_file_ids| must be True.
        """
        num_conflicting_ids = len(list(self.find_conflicting_ids(session=session)))
        if should_replace_file_ids:
            if num_conflicting_ids != 0:
                logging.info(
                    "Found [%s], but proceeding as should_replace_file_ids is True",
                    num_conflicting_ids,
                )
            else:
                raise ValueError(
                    "Found no conflicting ids but should_replace_file_ids was set to True"
                )
        elif num_conflicting_ids != 0:
            raise ValueError(f"Found [{num_conflicting_ids}] conflicting ids")
        else:
            logging.info("Found no conflicting ids; proceeding...")

    def preview_backfill(
        self,
        *,
        session: Session,
    ) -> None:
        """Previews backfill by determining how many rows of each file tag need to be
        copied over.
        """

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
        replaced_file_ids: list[ReplacedFileId],
    ) -> None:
        """Builds and executes a sql query to populate the gcs file metadata table
        using the mapping in |replaced_file_ids|.
        """

        command = f"""
        WITH file_id_mapping AS (
            SELECT v.old_file_id, v.new_file_id
            FROM (
                VALUES {', '.join(replaced_file_id.as_db_tuple() for replaced_file_id in replaced_file_ids)} 
            ) AS v(old_file_id, new_file_id)
        ), rows_to_backfill AS (
            SELECT file_id, region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time 
            FROM direct_ingest_raw_file_metadata
            WHERE 
            region_code = '{self.state_code.value.upper()}'
            AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
            AND is_invalidated IS FALSE
        ), mapped_rows_to_backfill AS (
            SELECT 
                new_file_id as file_id,
                region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time 
            FROM rows_to_backfill r
            INNER JOIN file_id_mapping m
            ON r.file_id = m.old_file_id
        )
        INSERT INTO direct_ingest_raw_gcs_file_metadata (file_id, region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time)
        SELECT file_id, region_code, raw_data_instance, is_invalidated, file_tag, normalized_file_name, update_datetime, file_discovery_time 
        FROM mapped_rows_to_backfill
        """
        result = session.execute(sqlalchemy.text(command))
        logging.info("Added [%s] rows to gcs metadata table", result.rowcount)

    def insert_bq_files(
        self,
        *,
        session: Session,
        should_replace_file_ids: bool,
    ) -> list[ReplacedFileId]:
        """Builds and executes a sql query to populate the bq file metadata table,
        building a mapping of old_file_id to new_file_id.
        """

        new_file_id_stmt = (
            "nextval('direct_ingest_raw_big_query_file_metadata_file_id_seq')"
            if should_replace_file_ids
            else "file_id"
        )

        command = f"""
        WITH legacy_rows_to_backfill AS (
            SELECT 
                file_id as old_file_id, 
                {new_file_id_stmt} as new_file_id,
                region_code, 
                raw_data_instance, 
                is_invalidated, 
                file_tag, 
                normalized_file_name, 
                update_datetime, 
                file_processed_time 
            FROM direct_ingest_raw_file_metadata
            WHERE 
                region_code = '{self.state_code.value.upper()}'
                AND raw_data_instance = '{self.raw_data_instance.value.upper()}'
                AND is_invalidated IS FALSE
        ), new_backfilled_rows AS (
            INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, is_invalidated, file_tag, update_datetime, file_processed_time)
            SELECT new_file_id as file_id, region_code, raw_data_instance, is_invalidated, file_tag, update_datetime, file_processed_time 
            FROM legacy_rows_to_backfill
        )
        SELECT 
            old_file_id,
            new_file_id,
            file_tag
        FROM legacy_rows_to_backfill
        """
        results = session.execute(sqlalchemy.text(command))
        logging.info("Added [%s] rows to big query metadata table", results.rowcount)
        return [
            ReplacedFileId(
                old_file_id=int(result[0]),
                new_file_id=int(result[1]),
                file_tag=result[2],
            )
            for result in results
        ]

    def backfill(
        self,
        *,
        dry_run: bool,
        should_replace_file_ids: bool,
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

                self.ensure_no_unhandled_conflicting_ids(
                    session=session,
                    should_replace_file_ids=should_replace_file_ids,
                )

                self.preview_backfill(session=session)

                prompt_for_confirmation(
                    f"Proceed with backfill? [dry run mode: {dry_run}]"
                )

                if not dry_run:
                    logging.info("Executing backfill")
                    maybe_replaced_file_ids = self.insert_bq_files(
                        session=session,
                        should_replace_file_ids=should_replace_file_ids,
                    )
                    self.insert_gcs_files(
                        session=session,
                        replaced_file_ids=maybe_replaced_file_ids,
                    )
                    self.update_conflicting_ids_in_bq(
                        replaced_file_ids=maybe_replaced_file_ids
                    )
                else:
                    logging.info("Dry run, skipping backfill")
        finally:
            for lock in locks:
                lock_manager.release_lock_by_id(lock.lock_id)


def _create_parser() -> argparse.ArgumentParser:
    """Builds an argument parser for this script."""
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

    parser.add_argument(
        "--should_replace_file_ids",
        default=False,
        type=str_to_bool,
        help="Replaces file_ids in legacy operations that conflict with new file_ids.",
    )

    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = _create_parser().parse_args()
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

            manager.backfill(
                dry_run=args.dry_run,
                should_replace_file_ids=args.should_replace_file_ids,
            )
