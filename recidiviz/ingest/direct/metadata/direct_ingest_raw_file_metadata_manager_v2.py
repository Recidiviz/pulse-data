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
"""Handles writing to and from our file metadata tables"""
import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional

from more_itertools import one
from sqlalchemy import and_, asc, func, select

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DIRECT_INGEST_UNPROCESSED_PREFIX,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations import entities


# TODO(#28239) migrate this manager back to DirectIngestRawFileMetadataManager
class DirectIngestRawFileMetadataManagerV2:
    """Handles writing to and from our file metadata tables"""

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
    ) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.raw_data_instance = raw_data_instance

    # --- private helpers --------------------------------------------------------------

    def _get_gcs_raw_file_metadata_for_path(
        self, session: Session, path: GcsfsFilePath
    ) -> schema.DirectIngestRawGCSFileMetadata:
        """Returns metadata information for the provided path, throws if it doesn't
        exist.
        """
        results = (
            session.query(schema.DirectIngestRawGCSFileMetadata)
            .filter_by(
                region_code=self.region_code,
                normalized_file_name=path.file_name,
                raw_data_instance=self.raw_data_instance.value,
            )
            .all()
        )

        if len(results) != 1:
            raise ValueError(
                f"Unexpected number of metadata results for path {path.abs_path()}: "
                f"[{len(results)}]"
            )

        return one(results)

    def _get_raw_big_query_file_metadata_for_file_id(
        self, session: Session, file_id: int
    ) -> schema.DirectIngestRawBigQueryFileMetadata:
        """Returns metadata information for the provided file id, throws if it doesn't
        exist.
        """
        return (
            session.query(schema.DirectIngestRawBigQueryFileMetadata)
            .filter_by(
                region_code=self.region_code,
                file_id=file_id,
                raw_data_instance=self.raw_data_instance.value,
            )
            .one()
        )

    # --- row-level object retrieval ---------------------------------------------------

    def get_raw_big_query_file_metadata(
        self, file_id: int
    ) -> entities.DirectIngestRawBigQueryFileMetadata:
        """Given a |file_id|, returns the relevant DirectIngestRawBigQueryFileMetadata
        row. If no such file_id exists, an error will be thrown.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = self._get_raw_big_query_file_metadata_for_file_id(
                session, file_id
            )
            return convert_schema_object_to_entity(
                metadata, entities.DirectIngestRawBigQueryFileMetadata
            )

    def get_raw_gcs_file_metadata(
        self, path: GcsfsFilePath
    ) -> entities.DirectIngestRawGCSFileMetadata:
        """Returns metadata information for the provided path. If no such path exists,
        an error will be thrown.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = self._get_gcs_raw_file_metadata_for_path(session, path)
            return convert_schema_object_to_entity(
                metadata, entities.DirectIngestRawGCSFileMetadata
            )

    # --- file discovery logic ---------------------------------------------------------

    def has_raw_gcs_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        """Checks whether the file at this path is marked as discovered in the
        operations database.
        """
        try:
            _ = self.get_raw_gcs_file_metadata(path)
        except ValueError:
            return False

        return True

    def mark_raw_gcs_file_as_discovered(
        self, path: GcsfsFilePath, is_chunked_file: bool = False
    ) -> entities.DirectIngestRawGCSFileMetadata:
        """Writes a new row to the appropriate metadata table for a new, unprocessed raw
        file at |path|. If |is_chunked_file| is True, a `file_id` will not be written to
        the db.
        """
        if not path.file_name.startswith(DIRECT_INGEST_UNPROCESSED_PREFIX):
            raise ValueError("Expect only unprocessed paths in this function.")

        parts = filename_parts_from_path(path)
        with SessionFactory.using_database(self.database_key) as session:

            file_id: Optional[int] = None

            if not is_chunked_file:
                new_bq_file = schema.DirectIngestRawBigQueryFileMetadata(
                    region_code=self.region_code,
                    file_tag=parts.file_tag,
                    update_datetime=parts.utc_upload_datetime,
                    raw_data_instance=self.raw_data_instance.value,
                    is_invalidated=False,
                )

                session.add(new_bq_file)
                session.flush()
                file_id = new_bq_file.file_id

            new_gcs_file = schema.DirectIngestRawGCSFileMetadata(
                file_id=file_id,
                region_code=self.region_code,
                raw_data_instance=self.raw_data_instance.value,
                file_tag=parts.file_tag,
                normalized_file_name=path.file_name,
                update_datetime=parts.utc_upload_datetime,
                file_discovery_time=datetime.datetime.now(tz=datetime.UTC),
            )
            session.add(new_gcs_file)
            session.flush()

            return convert_schema_object_to_entity(
                new_gcs_file, entities.DirectIngestRawGCSFileMetadata
            )

    def regiester_raw_big_query_file_for_paths(
        self, paths: List[GcsfsFilePath]
    ) -> entities.DirectIngestRawBigQueryFileMetadata:
        """Given a list of |paths| that have already been marked as discovered in the
        GCS file metadata table, registers an entry in the BigQuery file metadata table
        that links all of the files together, setting the update_datetime to the
        most recent update_datetime of the batch of files. If all of the provided |paths|
        don't exist in the GCS file metadata table, are not of the same file tag, or
        already have a file_id associated with it, an error will be raised.
        """
        with SessionFactory.using_database(self.database_key) as session:
            metadata_rows = (
                session.query(
                    schema.DirectIngestRawGCSFileMetadata.file_tag.label("file_tag"),
                    func.count(1).label("num_files"),
                    func.max(
                        schema.DirectIngestRawGCSFileMetadata.update_datetime
                    ).label("max_update_datetime"),
                )
                .filter(
                    # pylint: disable=singleton-comparison
                    schema.DirectIngestRawGCSFileMetadata.file_id == None,
                    schema.DirectIngestRawGCSFileMetadata.region_code
                    == self.region_code,
                    schema.DirectIngestRawGCSFileMetadata.raw_data_instance
                    == self.raw_data_instance.value,
                    schema.DirectIngestRawGCSFileMetadata.normalized_file_name.in_(
                        list(map(lambda x: x.file_name, paths))
                    ),
                )
                .group_by(schema.DirectIngestRawGCSFileMetadata.file_tag)
                .all()
            )

            if len(metadata_rows) != 1:
                raise ValueError(
                    f"Found multiple file tags [{', '.join(map(lambda x: x.file_tag, metadata_rows))}], "
                    f"but only expected one."
                )

            metadata_row = metadata_rows[0]

            if metadata_row.num_files != len(paths):
                raise ValueError(
                    f"Found unexpected number of paths: expected [{len(paths)}] but "
                    f"found [{metadata_row.num_files}]"
                )

            new_bq_file = schema.DirectIngestRawBigQueryFileMetadata(
                region_code=self.region_code,
                file_tag=metadata_row.file_tag,
                update_datetime=metadata_row.max_update_datetime,
                raw_data_instance=self.raw_data_instance.value,
                is_invalidated=False,
            )

            session.add(new_bq_file)
            session.flush()

            update_query = (
                schema.DirectIngestRawGCSFileMetadata.__table__.update()
                .where(
                    and_(
                        # pylint: disable=singleton-comparison
                        schema.DirectIngestRawGCSFileMetadata.region_code
                        == self.region_code,
                        schema.DirectIngestRawGCSFileMetadata.raw_data_instance
                        == self.raw_data_instance.value,
                        schema.DirectIngestRawGCSFileMetadata.normalized_file_name.in_(
                            list(map(lambda x: x.file_name, paths))
                        ),
                    )
                )
                .values(file_id=new_bq_file.file_id)
            )
            session.execute(update_query)

            return convert_schema_object_to_entity(
                new_bq_file, entities.DirectIngestRawBigQueryFileMetadata
            )

    # --- file processed logic ---------------------------------------------------------

    def has_raw_gcs_file_been_processed(self, path: GcsfsFilePath) -> bool:
        """Checks whether the file at this path has already been marked as processed
        (i.e. this GCS path has finished being uploaded to BigQuery)
        """
        try:
            metadata = self.get_raw_gcs_file_metadata(path)
        except ValueError:
            # For raw data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        return (
            metadata.bq_file is not None
            and metadata.bq_file.file_processed_time is not None
        )

    def has_raw_biq_query_file_been_processed(self, file_id: int) -> bool:
        """Checks whether this concpetual file_id has already been marked as processed
        (i.e. all GCS paths associated with this conceptual file have finished being
        uploaded to BigQuery)
        """
        try:
            metadata = self.get_raw_big_query_file_metadata(file_id)
        except ValueError:
            # For raw data files, if a file's metadata is not present in the database,
            # then it is assumed to be not processed, as it is seen as not existing.
            return False

        return metadata.file_processed_time is not None

    def mark_raw_big_query_file_as_processed(self, file_id: int) -> None:
        """Marks the file represented by the |path| as processed in the appropriate
        metadata table.
        """
        with SessionFactory.using_database(self.database_key) as session:
            metadata = self._get_raw_big_query_file_metadata_for_file_id(
                session, file_id
            )

            if metadata.is_invalidated:
                raise ValueError(
                    f"Cannot mark [{metadata.file_id}] as processed as the file is invalidated"
                )

            metadata.file_processed_time = datetime.datetime.now(tz=datetime.UTC)

    # --- file invalidation logic -----------------------------------------------------

    def mark_raw_big_query_file_as_invalidated_by_file_id(
        self, session: Session, file_id: int
    ) -> None:
        """Marks the row associated with the |file_id| as invalidated=True."""
        metadata = self._get_raw_big_query_file_metadata_for_file_id(session, file_id)
        metadata.is_invalidated = True

    # --- aggregate file retrieval logic -----------------------------------------------

    def get_unprocessed_raw_big_query_files_eligible_for_import(
        self,
    ) -> Dict[str, List[entities.DirectIngestRawBigQueryFileMetadata]]:
        """Returns unprocessed raw bq files that are eligible for import. In order to be
        eligible for processing, a given bq file has a null `file_processed_time` and
        `is_invalidated=False`
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            table_cls = schema.DirectIngestRawBigQueryFileMetadata

            recency_window = (
                func.row_number()
                .over(
                    partition_by=table_cls.file_tag,
                    order_by=table_cls.update_datetime.asc(),
                )
                .label("recency_rank")
            )

            results = (
                session.query(schema.DirectIngestRawBigQueryFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    raw_data_instance=self.raw_data_instance.value,
                    file_processed_time=None,
                    is_invalidated=False,
                )
                .add_columns(recency_window)
                .order_by(asc(("recency_rank")))
                .all()
            )

            result_dict: Dict[
                str, List[entities.DirectIngestRawBigQueryFileMetadata]
            ] = defaultdict(list)

            for big_query_file, _ in results:
                result_dict[big_query_file.file_tag].append(
                    convert_schema_object_to_entity(
                        big_query_file, entities.DirectIngestRawBigQueryFileMetadata
                    )
                )

            return result_dict

    # TODO(#29133) re-implement this function once we deteremine what the new admin
    # panel will display
    def get_metadata_for_all_raw_files_in_region(
        self,
    ) -> List[Any]:
        raise NotImplementedError

    def get_non_invalidated_raw_big_query_files(
        self,
    ) -> List[entities.DirectIngestRawBigQueryFileMetadata]:
        """Get metadata for all files that are not invalidated."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = (
                session.query(schema.DirectIngestRawBigQueryFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    is_invalidated=False,
                    raw_data_instance=self.raw_data_instance.value,
                )
                .all()
            )

            return [
                convert_schema_object_to_entity(
                    result, entities.DirectIngestRawBigQueryFileMetadata
                )
                for result in results
            ]

    def get_max_update_datetimes(
        self, session: Session
    ) -> Dict[str, datetime.datetime]:
        """Returns the max update datetime for all processed big query file tags"""
        results = (
            session.query(
                schema.DirectIngestRawBigQueryFileMetadata.file_tag,
                func.max(
                    schema.DirectIngestRawBigQueryFileMetadata.update_datetime
                ).label("max_update_datetime"),
            )
            .filter(
                schema.DirectIngestRawBigQueryFileMetadata.region_code
                == self.region_code,
                schema.DirectIngestRawBigQueryFileMetadata.raw_data_instance
                == self.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                schema.DirectIngestRawBigQueryFileMetadata.is_invalidated == False,
                schema.DirectIngestRawBigQueryFileMetadata.file_processed_time.isnot(
                    None
                ),
            )
            .group_by(schema.DirectIngestRawBigQueryFileMetadata.file_tag)
            .all()
        )
        return {result.file_tag: result.max_update_datetime for result in results}

    # --- flashing logic ---------------------------------------------------------------

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "DirectIngestRawFileMetadataManagerV2",
        session: Session,
    ) -> None:
        """Take all rows where `is_invalidated=False` and transfer to the instance
        associated with the new_instance_manager
        """
        if (
            new_instance_manager.raw_data_instance == self.raw_data_instance
            or new_instance_manager.region_code != self.region_code
        ):
            raise ValueError(
                "Either state codes are not the same or new instance is same as origin."
            )

        bq_table_cls = schema.DirectIngestRawBigQueryFileMetadata
        gcs_table_cls = schema.DirectIngestRawGCSFileMetadata
        # check destination instance does not have any valid metadata rows
        check_query = (
            session.query(bq_table_cls)
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

        file_id_subquery = (
            select([bq_table_cls.file_id])
            .where(
                bq_table_cls.region_code == self.region_code.upper(),
                bq_table_cls.raw_data_instance == self.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                bq_table_cls.is_invalidated == False,
            )
            .scalar_subquery()
        )

        gcs_update_query = (
            gcs_table_cls.__table__.update()
            .where(
                and_(
                    gcs_table_cls.region_code == self.region_code.upper(),
                    gcs_table_cls.raw_data_instance == self.raw_data_instance.value,
                    gcs_table_cls.file_id is not None,
                    # pylint: disable=singleton-comparison
                    gcs_table_cls.file_id.in_(file_id_subquery),
                )
            )
            .values(
                raw_data_instance=new_instance_manager.raw_data_instance.value,
            )
        )

        bq_update_query = (
            bq_table_cls.__table__.update()
            .where(
                and_(
                    bq_table_cls.region_code == self.region_code.upper(),
                    bq_table_cls.raw_data_instance == self.raw_data_instance.value,
                    # pylint: disable=singleton-comparison
                    bq_table_cls.is_invalidated == False,
                )
            )
            .values(
                raw_data_instance=new_instance_manager.raw_data_instance.value,
            )
        )

        session.execute(gcs_update_query)
        session.execute(bq_update_query)

    def mark_instance_data_invalidated(self) -> None:
        """Sets the is_invalidated on all rows for the state/instance"""
        with SessionFactory.using_database(
            self.database_key,
        ) as session:
            table_cls = schema.DirectIngestRawBigQueryFileMetadata
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