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
from typing import List, Optional

import pytz

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DIRECT_INGEST_UNPROCESSED_PREFIX,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestViewExportArgs,
    filename_parts_from_path,
)
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestFileMetadataManager,
    DirectIngestIngestFileMetadataManager,
    DirectIngestRawFileMetadataManager,
    DirectIngestSftpFileMetadataManager,
)
from recidiviz.ingest.direct.types.errors import DirectIngestInstanceError
from recidiviz.persistence.database.schema.operations import dao, schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestIngestFileMetadata,
    DirectIngestRawFileMetadata,
    DirectIngestSftpFileMetadata,
)
from recidiviz.utils import environment


class PostgresDirectIngestSftpFileMetadataManager(DirectIngestSftpFileMetadataManager):
    """An implementation for a class that handles writing metadata about each sftp
    direct ingest file to the operations Postgres table."""

    def __init__(self, region_code: str, ingest_database_name: str) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = ingest_database_name

    def has_sftp_file_been_discovered(self, remote_file_path: str) -> bool:
        try:
            _ = self.get_sftp_file_metadata(remote_file_path)
        except ValueError:
            return False

        return True

    def mark_sftp_file_as_discovered(self, remote_file_path: str) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestSftpFileMetadata(
                    region_code=self.region_code,
                    remote_file_path=remote_file_path,
                    discovery_time=datetime.datetime.now(tz=pytz.UTC),
                    processed_time=None,
                )
            )

    def get_sftp_file_metadata(
        self, remote_file_path: str
    ) -> DirectIngestSftpFileMetadata:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_sftp_file_metadata_row_for_path(
                session, self.region_code, remote_file_path
            )
            return self._sftp_file_schema_metadata_as_entity(metadata)

    def has_sftp_file_been_processed(self, remote_file_path: str) -> bool:
        try:
            metadata = self.get_sftp_file_metadata(remote_file_path)
        except ValueError:
            # For sftp data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        return metadata.processed_time is not None

    def mark_sftp_file_as_processed(self, remote_file_path: str) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_sftp_file_metadata_row_for_path(
                session, self.region_code, remote_file_path
            )
            metadata.processed_time = datetime.datetime.now(tz=pytz.UTC)

    @staticmethod
    def _sftp_file_schema_metadata_as_entity(
        schema_metadata: schema.DirectIngestRawFileMetadata,
    ) -> DirectIngestSftpFileMetadata:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestSftpFileMetadata):
            raise ValueError(
                f"Unexpected metadata entity type: {type(entity_metadata)}"
            )

        return entity_metadata


class PostgresDirectIngestRawFileMetadataManager(DirectIngestRawFileMetadataManager):
    """An implementation for a class that handles writing metadata about each raw data
    direct ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str, ingest_database_name: str) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = ingest_database_name

    def has_raw_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        self._check_is_raw_file_path(path)

        try:
            _ = self.get_raw_file_metadata(path)
        except ValueError:
            return False

        return True

    def mark_raw_file_as_discovered(self, path: GcsfsFilePath) -> None:
        self._check_is_raw_file_path(path)
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        parts = filename_parts_from_path(path)
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestRawFileMetadata(
                    region_code=self.region_code,
                    file_tag=parts.file_tag,
                    normalized_file_name=path.file_name,
                    discovery_time=datetime.datetime.now(tz=pytz.UTC),
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime,
                )
            )

    def get_raw_file_metadata(self, path: GcsfsFilePath) -> DirectIngestRawFileMetadata:
        self._check_is_raw_file_path(path)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path
            )
            return self._raw_file_schema_metadata_as_entity(metadata)

    def has_raw_file_been_processed(self, path: GcsfsFilePath) -> bool:
        self._check_is_raw_file_path(path)

        try:
            metadata = self.get_raw_file_metadata(path)
        except ValueError:
            # For raw data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        return metadata.processed_time is not None

    def mark_raw_file_as_processed(self, path: GcsfsFilePath) -> None:
        self._check_is_raw_file_path(path)
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path
            )

            metadata.processed_time = datetime.datetime.now(tz=pytz.UTC)

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
            )

            return [
                self._raw_file_schema_metadata_as_entity(metadata)
                for metadata in results
            ]

    @staticmethod
    def _raw_file_schema_metadata_as_entity(
        schema_metadata: schema.DirectIngestRawFileMetadata,
    ) -> DirectIngestRawFileMetadata:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestRawFileMetadata):
            raise ValueError(
                f"Unexpected metadata entity type: {type(entity_metadata)}"
            )

        return entity_metadata

    @staticmethod
    def _check_is_raw_file_path(path: GcsfsFilePath) -> None:
        parts = filename_parts_from_path(path)
        if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(
                f"Unexpected files type [{parts.file_type}] for path [{path.abs_path()}]"
            )

    def get_num_unprocessed_raw_files(self) -> int:
        """Returns the number of unprocessed raw files in the operations table for this region"""
        if "secondary" in self.ingest_database_name:
            raise DirectIngestInstanceError(
                f"Invalid ingest database name [{self.ingest_database_name}] provided."
                f"Raw files should only be processed in a primary ingest instance,"
                f"not the secondary instance. "
            )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            unprocessed_raw_files = (
                dao.get_date_sorted_unprocessed_raw_files_for_region(
                    session, self.region_code
                )
            )
            return len(unprocessed_raw_files)


class PostgresDirectIngestIngestFileMetadataManager(
    DirectIngestIngestFileMetadataManager
):
    """An implementation for a class that handles writing metadata about each ingest
    view direct ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str, ingest_database_name: str):
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = ingest_database_name

    def register_ingest_file_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                region_code=self.region_code,
                file_tag=ingest_view_job_args.ingest_view_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=datetime.datetime.now(tz=pytz.UTC),
                datetimes_contained_lower_bound_exclusive=ingest_view_job_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=ingest_view_job_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            session.add(metadata)
            session.commit()
            return self._ingest_file_schema_metadata_as_entity(metadata)

    def register_ingest_file_split(
        self,
        original_file_metadata: DirectIngestIngestFileMetadata,
        path: GcsfsFilePath,
    ) -> DirectIngestIngestFileMetadata:
        self._check_is_ingest_view_file_path(path)

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                region_code=self.region_code,
                file_tag=original_file_metadata.file_tag,
                is_invalidated=False,
                is_file_split=True,
                job_creation_time=datetime.datetime.now(tz=pytz.UTC),
                normalized_file_name=path.file_name,
                datetimes_contained_lower_bound_exclusive=original_file_metadata.datetimes_contained_lower_bound_exclusive,
                datetimes_contained_upper_bound_inclusive=original_file_metadata.datetimes_contained_upper_bound_inclusive,
                ingest_database_name=original_file_metadata.ingest_database_name,
            )
            session.add(metadata)
            session.commit()
            return self._ingest_file_schema_metadata_as_entity(metadata)

    def has_ingest_view_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        self._check_is_ingest_view_file_path(path)

        metadata = self.get_ingest_view_file_metadata(path)

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        if not isinstance(metadata, DirectIngestIngestFileMetadata):
            raise ValueError(
                f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
            )

        return metadata.discovery_time is not None

    def mark_ingest_view_file_as_discovered(self, path: GcsfsFilePath) -> None:
        self._check_is_ingest_view_file_path(path)
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_ingest_file_metadata_row_for_path(
                session,
                self.region_code,
                path,
                ingest_database_name=self.ingest_database_name,
            )
            dt = datetime.datetime.now(tz=pytz.UTC)
            if not metadata.export_time:
                metadata.export_time = dt
            metadata.discovery_time = dt

    def get_ingest_view_file_metadata(
        self, path: GcsfsFilePath
    ) -> DirectIngestIngestFileMetadata:
        self._check_is_ingest_view_file_path(path)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_ingest_file_metadata_row_for_path(
                session,
                self.region_code,
                path,
                ingest_database_name=self.ingest_database_name,
            )
            return self._ingest_file_schema_metadata_as_entity(metadata)

    def has_ingest_view_file_been_processed(self, path: GcsfsFilePath) -> bool:
        self._check_is_ingest_view_file_path(path)

        metadata = self.get_ingest_view_file_metadata(path)

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        if not isinstance(metadata, DirectIngestIngestFileMetadata):
            raise ValueError(
                f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
            )

        return metadata.processed_time is not None

    def mark_ingest_view_file_as_processed(self, path: GcsfsFilePath) -> None:
        self._check_is_ingest_view_file_path(path)
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_ingest_file_metadata_row_for_path(
                session,
                self.region_code,
                path,
                ingest_database_name=self.ingest_database_name,
            )
            metadata.processed_time = datetime.datetime.now(tz=pytz.UTC)

    def get_ingest_view_metadata_for_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_ingest_view_metadata_for_export_job(
                session=session,
                region_code=self.region_code,
                file_tag=ingest_view_job_args.ingest_view_name,
                datetimes_contained_lower_bound_exclusive=ingest_view_job_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=ingest_view_job_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )

            if not metadata:
                raise ValueError(
                    f"No metadata found for export job args [{ingest_view_job_args}]"
                )

            return self._ingest_file_schema_metadata_as_entity(metadata)

    def register_ingest_view_export_file_name(
        self,
        metadata_entity: DirectIngestIngestFileMetadata,
        exported_path: GcsfsFilePath,
    ) -> None:
        self._check_is_ingest_view_file_path(exported_path)
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_ingest_file_metadata_row(
                session=session,
                file_id=metadata_entity.file_id,
                ingest_database_name=metadata_entity.ingest_database_name,
            )

            if metadata.normalized_file_name:
                raise ValueError(
                    f"Normalized file name already set to [{metadata.normalized_file_name}] for file id "
                    f"[{metadata.file_id}]"
                )

            metadata.normalized_file_name = exported_path.file_name

    def mark_ingest_view_exported(
        self, metadata_entity: DirectIngestIngestFileMetadata
    ) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = dao.get_ingest_file_metadata_row(
                session=session,
                file_id=metadata_entity.file_id,
                ingest_database_name=metadata_entity.ingest_database_name,
            )
            metadata.export_time = datetime.datetime.now(tz=pytz.UTC)

    def get_ingest_view_metadata_for_most_recent_valid_job(
        self, ingest_view_tag: str
    ) -> Optional[DirectIngestIngestFileMetadata]:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = dao.get_ingest_view_metadata_for_most_recent_valid_job(
                session=session,
                region_code=self.region_code,
                file_tag=ingest_view_tag,
                ingest_database_name=self.ingest_database_name,
            )

            return (
                self._ingest_file_schema_metadata_as_entity(metadata)
                if metadata
                else None
            )

    def get_ingest_view_metadata_pending_export(
        self,
    ) -> List[DirectIngestIngestFileMetadata]:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = dao.get_ingest_view_metadata_pending_export(
                session=session,
                region_code=self.region_code,
                ingest_database_name=self.ingest_database_name,
            )

            return [
                self._ingest_file_schema_metadata_as_entity(metadata)
                for metadata in results
            ]

    @staticmethod
    def _ingest_file_schema_metadata_as_entity(
        schema_metadata: schema.DirectIngestIngestFileMetadata,
    ) -> DirectIngestIngestFileMetadata:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestIngestFileMetadata):
            raise ValueError(
                f"Unexpected metadata entity type: {type(entity_metadata)}"
            )

        return entity_metadata

    @staticmethod
    def _check_is_ingest_view_file_path(path: GcsfsFilePath) -> None:
        parts = filename_parts_from_path(path)
        if parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
            raise ValueError(
                f"Unexpected files type [{parts.file_type}] for path [{path.abs_path()}]"
            )

    def get_num_unprocessed_ingest_files(self) -> int:
        """Returns the number of unprocessed ingest files in the operations table for this region"""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            unprocessed_ingest_files = (
                dao.get_date_sorted_unprocessed_ingest_view_files_for_region(
                    session, self.region_code, self.ingest_database_name
                )
            )
            return len(unprocessed_ingest_files)

    def get_date_of_earliest_unprocessed_ingest_file(
        self,
    ) -> Optional[datetime.datetime]:
        """Returns the earliest unprocessed ingest file in the operations table for this region"""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # The unprocessed ingest files are returned from earliest to latest
            unprocessed_ingest_files = (
                dao.get_date_sorted_unprocessed_ingest_view_files_for_region(
                    session, self.region_code, self.ingest_database_name
                )
            )

            if not unprocessed_ingest_files:
                return None

            earliest_unprocessed_ingest_file: schema.DirectIngestIngestFileMetadata = (
                unprocessed_ingest_files[0]
            )
            return earliest_unprocessed_ingest_file.job_creation_time

    @environment.test_only
    def clear_ingest_file_metadata(
        self,
    ) -> None:
        """Deletes all metadata rows for this metadata manager's region and ingest
        instance.
        """
        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestIngestFileMetadata
            delete_query = table_cls.__table__.delete().where(
                table_cls.region_code == self.region_code
                and table_cls.ingest_database_name == self.database_key.db_name
            )
            session.execute(delete_query)


class PostgresDirectIngestFileMetadataManager(DirectIngestFileMetadataManager):
    """An implementation for a class that handles writing metadata about each direct
    ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str, ingest_database_name: str):
        self.region_code = region_code.upper()
        self.sftp_file_manager = PostgresDirectIngestSftpFileMetadataManager(
            self.region_code, ingest_database_name
        )
        self.raw_file_manager = PostgresDirectIngestRawFileMetadataManager(
            self.region_code, ingest_database_name
        )
        self.ingest_file_manager = PostgresDirectIngestIngestFileMetadataManager(
            self.region_code, ingest_database_name
        )

    def has_sftp_file_been_discovered(self, remote_file_path: str) -> bool:
        return self.sftp_file_manager.has_sftp_file_been_discovered(remote_file_path)

    def mark_sftp_file_as_discovered(self, remote_file_path: str) -> None:
        return self.sftp_file_manager.mark_sftp_file_as_discovered(remote_file_path)

    def get_sftp_file_metadata(
        self, remote_file_path: str
    ) -> DirectIngestSftpFileMetadata:
        return self.sftp_file_manager.get_sftp_file_metadata(remote_file_path)

    def has_sftp_file_been_processed(self, remote_file_path: str) -> bool:
        return self.sftp_file_manager.has_sftp_file_been_processed(remote_file_path)

    def mark_sftp_file_as_processed(self, remote_file_path: str) -> None:
        return self.sftp_file_manager.mark_sftp_file_as_processed(remote_file_path)

    def has_raw_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        return self.raw_file_manager.has_raw_file_been_discovered(path)

    def mark_raw_file_as_discovered(self, path: GcsfsFilePath) -> None:
        return self.raw_file_manager.mark_raw_file_as_discovered(path)

    def get_raw_file_metadata(self, path: GcsfsFilePath) -> DirectIngestRawFileMetadata:
        return self.raw_file_manager.get_raw_file_metadata(path)

    def has_raw_file_been_processed(self, path: GcsfsFilePath) -> bool:
        return self.raw_file_manager.has_raw_file_been_processed(path)

    def mark_raw_file_as_processed(self, path: GcsfsFilePath) -> None:
        return self.raw_file_manager.mark_raw_file_as_processed(path)

    def get_metadata_for_raw_files_discovered_after_datetime(
        self,
        raw_file_tag: str,
        discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
    ) -> List[DirectIngestRawFileMetadata]:
        return self.raw_file_manager.get_metadata_for_raw_files_discovered_after_datetime(
            raw_file_tag=raw_file_tag,
            discovery_time_lower_bound_exclusive=discovery_time_lower_bound_exclusive,
        )

    def register_ingest_file_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:
        return self.ingest_file_manager.register_ingest_file_export_job(
            ingest_view_job_args=ingest_view_job_args
        )

    def register_ingest_file_split(
        self,
        original_file_metadata: DirectIngestIngestFileMetadata,
        path: GcsfsFilePath,
    ) -> DirectIngestIngestFileMetadata:
        return self.ingest_file_manager.register_ingest_file_split(
            original_file_metadata=original_file_metadata, path=path
        )

    def has_ingest_view_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        return self.ingest_file_manager.has_ingest_view_file_been_discovered(path)

    def mark_ingest_view_file_as_discovered(self, path: GcsfsFilePath) -> None:
        return self.ingest_file_manager.mark_ingest_view_file_as_discovered(path)

    def get_ingest_view_file_metadata(
        self, path: GcsfsFilePath
    ) -> DirectIngestIngestFileMetadata:
        return self.ingest_file_manager.get_ingest_view_file_metadata(path)

    def has_ingest_view_file_been_processed(self, path: GcsfsFilePath) -> bool:
        return self.ingest_file_manager.has_ingest_view_file_been_processed(path)

    def mark_ingest_view_file_as_processed(self, path: GcsfsFilePath) -> None:
        return self.ingest_file_manager.mark_ingest_view_file_as_processed(path)

    def get_ingest_view_metadata_for_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:
        return self.ingest_file_manager.get_ingest_view_metadata_for_export_job(
            ingest_view_job_args=ingest_view_job_args
        )

    def register_ingest_view_export_file_name(
        self,
        metadata_entity: DirectIngestIngestFileMetadata,
        exported_path: GcsfsFilePath,
    ) -> None:
        return self.ingest_file_manager.register_ingest_view_export_file_name(
            metadata_entity=metadata_entity, exported_path=exported_path
        )

    def mark_ingest_view_exported(
        self, metadata_entity: DirectIngestIngestFileMetadata
    ) -> None:
        return self.ingest_file_manager.mark_ingest_view_exported(
            metadata_entity=metadata_entity
        )

    def get_ingest_view_metadata_for_most_recent_valid_job(
        self, ingest_view_tag: str
    ) -> Optional[DirectIngestIngestFileMetadata]:
        return (
            self.ingest_file_manager.get_ingest_view_metadata_for_most_recent_valid_job(
                ingest_view_tag=ingest_view_tag
            )
        )

    def get_ingest_view_metadata_pending_export(
        self,
    ) -> List[DirectIngestIngestFileMetadata]:
        return self.ingest_file_manager.get_ingest_view_metadata_pending_export()

    def get_num_unprocessed_raw_files(self) -> int:
        return self.raw_file_manager.get_num_unprocessed_raw_files()

    def get_num_unprocessed_ingest_files(self) -> int:
        return self.ingest_file_manager.get_num_unprocessed_ingest_files()

    def get_date_of_earliest_unprocessed_ingest_file(
        self,
    ) -> Optional[datetime.datetime]:
        return self.ingest_file_manager.get_date_of_earliest_unprocessed_ingest_file()
