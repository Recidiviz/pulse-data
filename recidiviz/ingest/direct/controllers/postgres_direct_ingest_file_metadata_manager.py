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
from typing import Optional, List

import pytz

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import (
    DirectIngestFileMetadataManager,
    DirectIngestRawFileMetadataManager,
    DirectIngestIngestFileMetadataManager,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DIRECT_INGEST_UNPROCESSED_PREFIX,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestViewExportArgs,
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
)
from recidiviz.persistence.database.schema.operations import schema, dao
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
)
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestIngestFileMetadata,
)


class PostgresDirectIngestRawFileMetadataManager(DirectIngestRawFileMetadataManager):
    """An implementation for a class that handles writing metadata about each raw data
    direct ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str):
        self.region_code = region_code
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def has_raw_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        self._check_is_raw_file_path(path)

        try:
            metadata = self.get_raw_file_metadata(path)
        except ValueError:
            return False

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        if not isinstance(metadata, DirectIngestRawFileMetadata):
            raise ValueError(
                f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
            )

        # TODO(#3020): Design/handle/write tests for case where this is a file we've moved from storage for a
        #  rerun. How do we accurately detect when this is happening?
        return True

    def mark_raw_file_as_discovered(self, path: GcsfsFilePath) -> None:
        self._check_is_raw_file_path(path)
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        parts = filename_parts_from_path(path)
        session = SessionFactory.for_database(self.database_key)

        try:
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
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_raw_file_metadata(self, path: GcsfsFilePath) -> DirectIngestRawFileMetadata:
        self._check_is_raw_file_path(path)
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path
            )
            metadata_entity = self._raw_file_schema_metadata_as_entity(metadata)
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def has_raw_file_been_processed(self, path: GcsfsFilePath) -> bool:
        self._check_is_raw_file_path(path)

        try:
            metadata = self.get_raw_file_metadata(path)
        except ValueError:
            # For raw data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        if not isinstance(metadata, DirectIngestRawFileMetadata):
            raise ValueError(
                f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
            )

        return metadata.processed_time is not None

    def mark_raw_file_as_processed(self, path: GcsfsFilePath) -> None:
        self._check_is_raw_file_path(path)
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_raw_file_metadata_row_for_path(
                session, self.region_code, path
            )

            metadata.processed_time = datetime.datetime.now(tz=pytz.UTC)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_metadata_for_raw_files_discovered_after_datetime(
        self,
        raw_file_tag: str,
        discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
    ) -> List[DirectIngestRawFileMetadata]:
        session = SessionFactory.for_database(self.database_key)

        try:
            results = dao.get_metadata_for_raw_files_discovered_after_datetime(
                session=session,
                region_code=self.region_code,
                raw_file_tag=raw_file_tag,
                discovery_time_lower_bound_exclusive=discovery_time_lower_bound_exclusive,
            )

            metadata_entities = [
                self._raw_file_schema_metadata_as_entity(metadata)
                for metadata in results
            ]
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entities

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


class PostgresDirectIngestIngestFileMetadataManager(
    DirectIngestIngestFileMetadataManager
):
    """An implementation for a class that handles writing metadata about each ingest
    view direct ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str, ingest_database_name: str):
        self.region_code = region_code
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = ingest_database_name

    def register_ingest_file_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:
        session = SessionFactory.for_database(self.database_key)

        try:
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
            metadata_entity = self._ingest_file_schema_metadata_as_entity(metadata)
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def register_ingest_file_split(
        self,
        original_file_metadata: DirectIngestIngestFileMetadata,
        path: GcsfsFilePath,
    ) -> DirectIngestIngestFileMetadata:
        self._check_is_ingest_view_file_path(path)

        session = SessionFactory.for_database(self.database_key)

        try:
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
            metadata_entity = self._ingest_file_schema_metadata_as_entity(metadata)
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def has_ingest_view_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        self._check_is_ingest_view_file_path(path)

        metadata = self.get_ingest_view_file_metadata(path)

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        if not isinstance(metadata, DirectIngestIngestFileMetadata):
            raise ValueError(
                f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
            )

        # TODO(#3020): Design/handle/write tests for case where this is a file we've moved from storage for a
        #  rerun. How do we accurately detect when this is happening?
        return metadata.discovery_time is not None

    def mark_ingest_view_file_as_discovered(self, path: GcsfsFilePath) -> None:
        self._check_is_ingest_view_file_path(path)
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        session = SessionFactory.for_database(self.database_key)

        try:
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
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_ingest_view_file_metadata(
        self, path: GcsfsFilePath
    ) -> DirectIngestIngestFileMetadata:
        self._check_is_ingest_view_file_path(path)
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_ingest_file_metadata_row_for_path(
                session,
                self.region_code,
                path,
                ingest_database_name=self.ingest_database_name,
            )
            metadata_entity = self._ingest_file_schema_metadata_as_entity(metadata)
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

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
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_ingest_file_metadata_row_for_path(
                session,
                self.region_code,
                path,
                ingest_database_name=self.ingest_database_name,
            )

            metadata.processed_time = datetime.datetime.now(tz=pytz.UTC)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_ingest_view_metadata_for_export_job(
        self, ingest_view_job_args: GcsfsIngestViewExportArgs
    ) -> DirectIngestIngestFileMetadata:

        session = SessionFactory.for_database(self.database_key)

        try:
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

            metadata_entity = self._ingest_file_schema_metadata_as_entity(metadata)
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def register_ingest_view_export_file_name(
        self,
        metadata_entity: DirectIngestIngestFileMetadata,
        exported_path: GcsfsFilePath,
    ) -> None:
        self._check_is_ingest_view_file_path(exported_path)

        session = SessionFactory.for_database(self.database_key)

        try:
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
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def mark_ingest_view_exported(
        self, metadata_entity: DirectIngestIngestFileMetadata
    ) -> None:
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_ingest_file_metadata_row(
                session=session,
                file_id=metadata_entity.file_id,
                ingest_database_name=metadata_entity.ingest_database_name,
            )
            metadata.export_time = datetime.datetime.now(tz=pytz.UTC)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_ingest_view_metadata_for_most_recent_valid_job(
        self, ingest_view_tag: str
    ) -> Optional[DirectIngestIngestFileMetadata]:
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_ingest_view_metadata_for_most_recent_valid_job(
                session=session,
                region_code=self.region_code,
                file_tag=ingest_view_tag,
                ingest_database_name=self.ingest_database_name,
            )

            metadata_entity = (
                self._ingest_file_schema_metadata_as_entity(metadata)
                if metadata
                else None
            )
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def get_ingest_view_metadata_pending_export(
        self,
    ) -> List[DirectIngestIngestFileMetadata]:
        session = SessionFactory.for_database(self.database_key)

        try:
            results = dao.get_ingest_view_metadata_pending_export(
                session=session,
                region_code=self.region_code,
                ingest_database_name=self.ingest_database_name,
            )

            metadata_entities = [
                self._ingest_file_schema_metadata_as_entity(metadata)
                for metadata in results
            ]
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entities

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


class PostgresDirectIngestFileMetadataManager(DirectIngestFileMetadataManager):
    """An implementation for a class that handles writing metadata about each direct
    ingest file to the operations Postgres table.
    """

    def __init__(self, region_code: str, ingest_database_name: str):
        self.region_code = region_code.upper()
        self.raw_file_manager = PostgresDirectIngestRawFileMetadataManager(
            self.region_code
        )
        self.ingest_file_manager = PostgresDirectIngestIngestFileMetadataManager(
            self.region_code, ingest_database_name
        )

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
