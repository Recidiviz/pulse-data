# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""An implementation for a class that handles writing metadata about each direct ingest file to the operations
Postgres table.
"""
import datetime
from typing import Dict, List, Optional

import pytz
import sqlalchemy
from sqlalchemy import and_, func

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DIRECT_INGEST_UNPROCESSED_PREFIX,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
    DirectIngestRawFileMetadataSummary,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import dao, schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.utils import environment


class PostgresDirectIngestRawFileMetadataManager(DirectIngestRawFileMetadataManager):
    """An implementation for a class that handles writing metadata about each raw data
    direct ingest file to the operations Postgres table.
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
    ) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.raw_data_instance = raw_data_instance

    def has_raw_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        try:
            _ = self.get_raw_file_metadata(path)
        except ValueError:
            return False

        return True

    def mark_raw_file_as_discovered(self, path: GcsfsFilePath) -> None:
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        parts = filename_parts_from_path(path)
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestRawFileMetadata(
                    region_code=self.region_code,
                    file_tag=parts.file_tag,
                    normalized_file_name=path.file_name,
                    file_discovery_time=datetime.datetime.now(tz=pytz.UTC),
                    file_processed_time=None,
                    update_datetime=parts.utc_upload_datetime,
                    raw_data_instance=self.raw_data_instance.value,
                    is_invalidated=False,
                )
            )

    def get_raw_file_metadata(self, path: GcsfsFilePath) -> DirectIngestRawFileMetadata:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path, self.raw_data_instance
            )
            return convert_schema_object_to_entity(
                metadata, DirectIngestRawFileMetadata
            )

    def has_raw_file_been_processed(self, path: GcsfsFilePath) -> bool:
        try:
            metadata = self.get_raw_file_metadata(path)
        except ValueError:
            # For raw data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        return metadata.file_processed_time is not None

    def mark_raw_file_as_processed(self, path: GcsfsFilePath) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path, self.raw_data_instance
            )

            metadata.file_processed_time = datetime.datetime.now(tz=pytz.UTC)

    def get_metadata_for_raw_files_discovered_after_datetime(
        self,
        raw_file_tag: str,
        discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
    ) -> List[DirectIngestRawFileMetadata]:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = dao.get_metadata_for_raw_files_discovered_after_datetime(
                session=session,
                region_code=self.region_code,
                raw_file_tag=raw_file_tag,
                discovery_time_lower_bound_exclusive=discovery_time_lower_bound_exclusive,
                raw_data_instance=self.raw_data_instance,
            )

            return [
                convert_schema_object_to_entity(metadata, DirectIngestRawFileMetadata)
                for metadata in results
            ]

    def get_metadata_for_all_raw_files_in_region(
        self,
    ) -> List[DirectIngestRawFileMetadataSummary]:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            return dao.get_all_raw_file_metadata_rows_for_region(
                session=session,
                region_code=self.region_code,
                raw_data_instance=self.raw_data_instance,
            )

    def get_unprocessed_raw_files_eligible_for_import(
        self,
    ) -> List[DirectIngestRawFileMetadata]:
        """Returns the metadata in the operations table for unprocessed raw files that are eligible for import for
        this region. In order to be eligible for processing, a given file has to have a null `file_processed_time`
        and the lowest `update_datetime` relative to other files with the same file_tag that are queued to process.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # There is no `SELECT * EXCEPT recency_rank` syntax in postgresql, so select all of the columns in the
            # table.
            query = f"""
              SELECT
                file_id,
                region_code,
                file_tag,
                normalized_file_name,
                raw_data_instance,
                is_invalidated,
                file_discovery_time,
                file_processed_time,
                update_datetime
              FROM (
                SELECT
                  *,
                  ROW_NUMBER() OVER (PARTITION BY file_tag ORDER BY update_datetime) as recency_rank
                FROM direct_ingest_raw_file_metadata
                WHERE region_code = '{self.region_code}'
                AND is_invalidated = False
                AND raw_data_instance = '{self.raw_data_instance.value}'
                AND file_processed_time is NULL
              ) a
              WHERE recency_rank = 1
            """
            results = session.execute(sqlalchemy.text(query))

            return [
                convert_schema_object_to_entity(
                    schema.DirectIngestRawFileMetadata(**result),
                    DirectIngestRawFileMetadata,
                )
                for result in results
            ]

    def get_non_invalidated_files(self) -> List[DirectIngestRawFileMetadata]:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(schema.DirectIngestRawFileMetadata).filter_by(
                region_code=self.region_code,
                is_invalidated=False,
                raw_data_instance=self.raw_data_instance.value,
            )
            results = query.all()

            return [
                convert_schema_object_to_entity(metadata, DirectIngestRawFileMetadata)
                for metadata in results
            ]

    def mark_instance_data_invalidated(self) -> None:
        """Sets the is_invalidated on all rows for the state/instance"""
        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestRawFileMetadata
            update_query = (
                table_cls.__table__.update()
                .where(
                    and_(
                        table_cls.region_code == self.region_code.upper(),
                        table_cls.raw_data_instance == self.raw_data_instance.value,
                    )
                )
                .values(is_invalidated=True)
            )
            session.execute(update_query)

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "PostgresDirectIngestRawFileMetadataManager",
    ) -> None:
        """Take all rows where `is_invalidated=False` and transfer to the instance associated with
        the new_instance_manager
        """
        if (
            new_instance_manager.raw_data_instance == self.raw_data_instance
            or new_instance_manager.region_code != self.region_code
        ):
            raise ValueError(
                "Either state codes are not the same or new instance is same as origin."
            )

        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestRawFileMetadata
            # check destination instance does not have any valid metadata rows
            check_query = (
                session.query(schema.DirectIngestRawFileMetadata)
                .filter_by(
                    region_code=self.region_code.upper(),
                    raw_data_instance=new_instance_manager.raw_data_instance.value,
                    is_invalidated=False,
                )
                .all()
            )
            if check_query:
                raise ValueError(
                    "Destination instance should not have any valid raw file metadata rows."
                )

            update_query = (
                table_cls.__table__.update()
                .where(
                    and_(
                        table_cls.region_code == self.region_code.upper(),
                        table_cls.raw_data_instance == self.raw_data_instance.value,
                        # pylint: disable=singleton-comparison
                        table_cls.is_invalidated == False,
                    )
                )
                .values(
                    raw_data_instance=new_instance_manager.raw_data_instance.value,
                )
            )
            session.execute(update_query)

    def get_max_update_datetimes(
        self, session: Session
    ) -> Dict[str, datetime.datetime]:
        """Returns the max update datetime for all processed file tags from direct_ingest_raw_file_metadata."""
        results = (
            session.query(
                schema.DirectIngestRawFileMetadata.file_tag,
                func.max(schema.DirectIngestRawFileMetadata.update_datetime).label(
                    "max_update_datetime"
                ),
            )
            .filter(
                schema.DirectIngestRawFileMetadata.region_code == self.region_code,
                schema.DirectIngestRawFileMetadata.raw_data_instance
                == self.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                schema.DirectIngestRawFileMetadata.is_invalidated == False,
                schema.DirectIngestRawFileMetadata.file_processed_time.is_not(None),
            )
            .group_by(schema.DirectIngestRawFileMetadata.file_tag)
            .all()
        )
        return {result.file_tag: result.max_update_datetime for result in results}

    @environment.test_only
    def mark_file_as_invalidated(self, path: GcsfsFilePath) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path, self.raw_data_instance
            )
            metadata.is_invalidated = True
