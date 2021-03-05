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

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import (
    DirectIngestFileMetadataManager,
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
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestIngestFileMetadata,
    DirectIngestFileMetadata,
)


class PostgresDirectIngestFileMetadataManager(DirectIngestFileMetadataManager):
    """An implementation for a class that handles writing metadata about each direct ingest file to the operations
    Postgres table.
    """

    def __init__(self, region_code: str):
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

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
                job_creation_time=datetime.datetime.utcnow(),
                datetimes_contained_lower_bound_exclusive=ingest_view_job_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=ingest_view_job_args.upper_bound_datetime_to_export,
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
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = schema.DirectIngestIngestFileMetadata(
                region_code=self.region_code,
                file_tag=original_file_metadata.file_tag,
                is_invalidated=False,
                is_file_split=True,
                job_creation_time=datetime.datetime.utcnow(),
                normalized_file_name=path.file_name,
                datetimes_contained_lower_bound_exclusive=original_file_metadata.datetimes_contained_lower_bound_exclusive,
                datetimes_contained_upper_bound_inclusive=original_file_metadata.datetimes_contained_upper_bound_inclusive,
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

    def has_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        parts = filename_parts_from_path(path)

        try:
            metadata = self.get_file_metadata(path)
        except ValueError as e:
            if parts.file_type != GcsfsDirectIngestFileType.RAW_DATA:
                raise e
            return False

        if not metadata:
            raise ValueError(f"Metadata unexpectedly None for path [{path.abs_path()}]")

        # TODO(#3020): Design/handle/write tests for case where this is a file we've moved from storage for a
        #  rerun. How do we accurately detect when this is happening?
        if isinstance(metadata, DirectIngestRawFileMetadata):
            return True

        if isinstance(metadata, DirectIngestIngestFileMetadata):
            if metadata.discovery_time is None:
                return False
            return True

        raise ValueError(
            f"Unexpected metadata type [{type(metadata)}] for path [{path.abs_path()}]"
        )

    def mark_file_as_discovered(self, path: GcsfsFilePath) -> None:
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        parts = filename_parts_from_path(path)
        session = SessionFactory.for_database(self.database_key)

        try:
            if parts.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
                metadata = dao.get_file_metadata_row_for_path(
                    session, self.region_code, path
                )
                dt = datetime.datetime.utcnow()
                if not metadata.export_time:
                    metadata.export_time = dt
                metadata.discovery_time = dt
            elif parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
                session.add(
                    schema.DirectIngestRawFileMetadata(
                        region_code=self.region_code,
                        file_tag=parts.file_tag,
                        normalized_file_name=path.file_name,
                        discovery_time=datetime.datetime.utcnow(),
                        processed_time=None,
                        datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime,
                    )
                )
            else:
                raise ValueError(f"Unexpected path type: {parts.file_type}")
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_file_metadata(self, path: GcsfsFilePath) -> DirectIngestFileMetadata:
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_file_metadata_row_for_path(
                session, self.region_code, path
            )

            if isinstance(metadata, schema.DirectIngestRawFileMetadata):
                metadata_entity: DirectIngestFileMetadata = (
                    self._raw_file_schema_metadata_as_entity(metadata)
                )
            elif isinstance(metadata, schema.DirectIngestIngestFileMetadata):
                metadata_entity = self._ingest_file_schema_metadata_as_entity(metadata)
            else:
                raise ValueError(f"Unexpected metadata type: {type(metadata)}")
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

        return metadata_entity

    def mark_file_as_processed(self, path: GcsfsFilePath) -> None:
        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_file_metadata_row_for_path(
                session, self.region_code, path
            )
            metadata.processed_time = datetime.datetime.utcnow()
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
        parts = filename_parts_from_path(exported_path)
        if parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
            raise ValueError(f"Exported path has unexpected type {parts.file_type}")

        session = SessionFactory.for_database(self.database_key)

        try:
            metadata = dao.get_file_metadata_row(
                session, GcsfsDirectIngestFileType.INGEST_VIEW, metadata_entity.file_id
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
            metadata = dao.get_file_metadata_row(
                session, GcsfsDirectIngestFileType.INGEST_VIEW, metadata_entity.file_id
            )
            metadata.export_time = datetime.datetime.utcnow()
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
                session=session, region_code=self.region_code, file_tag=ingest_view_tag
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
    def _ingest_file_schema_metadata_as_entity(
        schema_metadata: schema.DirectIngestIngestFileMetadata,
    ) -> DirectIngestIngestFileMetadata:
        entity_metadata = convert_schema_object_to_entity(schema_metadata)

        if not isinstance(entity_metadata, DirectIngestIngestFileMetadata):
            raise ValueError(
                f"Unexpected metadata entity type: {type(entity_metadata)}"
            )

        return entity_metadata
