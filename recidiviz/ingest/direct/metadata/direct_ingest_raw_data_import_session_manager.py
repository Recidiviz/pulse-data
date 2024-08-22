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
"""Class for managing interaction with DirectIngestRawDataImportSession"""
import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, select

from recidiviz.common.constants.operations.direct_ingest_raw_data_import_session import (
    DirectIngestRawDataImportSessionStatus,
)
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


class DirectIngestRawDataImportSessionManager:
    """Class for managing interaction with DirectIngestRawDataImportSession"""

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
    ) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.raw_data_instance = raw_data_instance

    def start_import_session(
        self,
        file_id: int,
        historical_diffs_active: bool,
        import_status: DirectIngestRawDataImportSessionStatus = DirectIngestRawDataImportSessionStatus.STARTED,
    ) -> entities.DirectIngestRawDataImportSession:
        """Given a |file_id|, create and return a new import session."""

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            new_import_session = schema.DirectIngestRawDataImportSession(
                file_id=file_id,
                import_status=import_status.value,
                import_start=datetime.datetime.now(tz=datetime.UTC),
                historical_diffs_active=historical_diffs_active,
                region_code=self.region_code,
                raw_data_instance=self.raw_data_instance.value,
            )

            session.add(new_import_session)

            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

            return convert_schema_object_to_entity(
                new_import_session,
                entities.DirectIngestRawDataImportSession,
                populate_direct_back_edges=True,
            )

    @staticmethod
    def _get_import_session_update_dict(
        import_status: Optional[DirectIngestRawDataImportSessionStatus],
        import_end: Optional[datetime.datetime],
        raw_rows: Optional[int],
        net_new_or_updated_rows: Optional[int],
        deleted_rows: Optional[int],
    ) -> Dict[str, Any]:
        """Filters and returns all non-null values as a dictionary."""
        import_session_kwargs = {
            "import_status": import_status.value if import_status else import_status,
            "import_end": import_end,
            "raw_rows": raw_rows,
            "net_new_or_updated_rows": net_new_or_updated_rows,
            "deleted_rows": deleted_rows,
        }

        return {
            field: value
            for field, value in import_session_kwargs.items()
            if value is not None
        }

    def update_import_session_by_id(
        self,
        import_session_id: int,
        import_status: Optional[DirectIngestRawDataImportSessionStatus] = None,
        import_end: Optional[datetime.datetime] = None,
        raw_rows: Optional[int] = None,
        net_new_or_updated_rows: Optional[int] = None,
        deleted_rows: Optional[int] = None,
    ) -> None:
        """Updates the import session related to the provided |import_session_id| with
        the provided import session information.
        """
        import_session_update_dict = self._get_import_session_update_dict(
            import_status, import_end, raw_rows, net_new_or_updated_rows, deleted_rows
        )

        with SessionFactory.using_database(self.database_key) as session:

            update_query = (
                schema.DirectIngestRawDataImportSession.__table__.update()
                .where(
                    schema.DirectIngestRawDataImportSession.import_session_id
                    == import_session_id
                )
                .values(**import_session_update_dict)
            )

            result = session.execute(update_query)

            if result.rowcount != 1:
                raise ValueError(
                    f"Unexpected updated row count; got {result.rowcount} but expected 1"
                )

    def update_most_recent_import_session_for_file_id(
        self,
        file_id: int,
        import_status: Optional[DirectIngestRawDataImportSessionStatus] = None,
        import_end: Optional[datetime.datetime] = None,
        raw_rows: Optional[int] = None,
        net_new_or_updated_rows: Optional[int] = None,
        deleted_rows: Optional[int] = None,
    ) -> None:
        """Updates the import session related to the provided |file_id| with the most
        recent import_start.

        Note: Use this with caution! Unless the operations db lock is held, there is a
        chance that the most recent import session associated with the file_id is not
        necessarily the import session you intend it to be.
        """
        import_session_update_dict = self._get_import_session_update_dict(
            import_status, import_end, raw_rows, net_new_or_updated_rows, deleted_rows
        )

        with SessionFactory.using_database(self.database_key) as session:
            session_id_scalar = (
                session.query(schema.DirectIngestRawDataImportSession.import_session_id)
                .filter(schema.DirectIngestRawDataImportSession.file_id == file_id)
                .order_by(schema.DirectIngestRawDataImportSession.import_start.desc())
                .limit(1)
                .scalar_subquery()
            )
            update_query = (
                schema.DirectIngestRawDataImportSession.__table__.update()
                .where(
                    schema.DirectIngestRawDataImportSession.import_session_id
                    == session_id_scalar
                )
                .values(**import_session_update_dict)
            )

            result = session.execute(update_query)

            if result.rowcount != 1:
                raise ValueError(
                    f"Unexpected updated row count; got {result.rowcount} but expected 1"
                )

    def get_import_session_by_id(
        self, import_session_id: int
    ) -> entities.DirectIngestRawDataImportSession:
        """Returns the import session associated with a given |import_session_id|,
        throwing an error if one doesn't exist.
        """

        with SessionFactory.using_database(self.database_key) as session:
            import_session = (
                session.query(schema.DirectIngestRawDataImportSession)
                .filter_by(import_session_id=import_session_id)
                .one()
            )

            return convert_schema_object_to_entity(
                import_session,
                entities.DirectIngestRawDataImportSession,
                populate_direct_back_edges=True,
            )

    def get_import_sesions_for_file_id(
        self, file_id: int
    ) -> List[entities.DirectIngestRawDataImportSession]:
        """Returns all import sessions associated with a given |file_id|."""

        with SessionFactory.using_database(self.database_key) as session:
            import_sessions = (
                session.query(schema.DirectIngestRawDataImportSession)
                .filter_by(file_id=file_id)
                .order_by(schema.DirectIngestRawDataImportSession.import_start)
                .all()
            )

            return [
                convert_schema_object_to_entity(
                    import_session,
                    entities.DirectIngestRawDataImportSession,
                    populate_direct_back_edges=True,
                )
                for import_session in import_sessions
            ]

    def get_n_most_recent_import_sessions_for_file_tag(
        self, file_tag: str, n: int = 10
    ) -> List[entities.DirectIngestRawDataImportSession]:
        """Given a |file_tag| and |n|, returns, at most, |n| of the most recent import
        sessions associated with |file_tag|.
        """
        with SessionFactory.using_database(self.database_key) as session:
            import_sessions = (
                session.query(schema.DirectIngestRawDataImportSession)
                .join(schema.DirectIngestRawBigQueryFileMetadata)
                .filter(
                    schema.DirectIngestRawBigQueryFileMetadata.file_tag == file_tag,
                    schema.DirectIngestRawDataImportSession.region_code
                    == self.region_code,
                    schema.DirectIngestRawDataImportSession.raw_data_instance
                    == self.raw_data_instance.value,
                )
                .order_by(schema.DirectIngestRawDataImportSession.import_start.desc())
                .limit(n)
                .all()
            )

            return [
                convert_schema_object_to_entity(
                    import_session,
                    entities.DirectIngestRawDataImportSession,
                    populate_direct_back_edges=True,
                )
                for import_session in import_sessions
            ]

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "DirectIngestRawDataImportSessionManager",
        session: Session,
    ) -> None:
        """Take all import session that have a file_id that has `is_invalidated=False`
        and transfer to the instance associated with the new_instance_manager
        """
        if (
            new_instance_manager.raw_data_instance == self.raw_data_instance
            or new_instance_manager.region_code != self.region_code
        ):
            raise ValueError(
                "Either state codes are not the same or new instance is same as origin."
            )

        import_table_cls = schema.DirectIngestRawDataImportSession
        bq_file_table_cls = schema.DirectIngestRawBigQueryFileMetadata
        # check destination instance does not have any valid metadata rows
        check_query = (
            session.query(import_table_cls)
            .join(bq_file_table_cls)
            .filter(
                import_table_cls.region_code == self.region_code.upper(),
                import_table_cls.raw_data_instance
                == new_instance_manager.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                bq_file_table_cls.is_invalidated == False,
            )
            .all()
        )
        if check_query:
            raise ValueError(
                f"Destination instance should not have any file_ids that reference "
                f"valid raw big query file metadata rows. Found [{len(check_query)}]."
            )

        file_id_subquery = (
            select([bq_file_table_cls.file_id])
            .where(
                bq_file_table_cls.region_code == self.region_code.upper(),
                bq_file_table_cls.raw_data_instance == self.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                bq_file_table_cls.is_invalidated == False,
            )
            .scalar_subquery()
        )

        update_query = (
            import_table_cls.__table__.update()
            .where(
                and_(
                    import_table_cls.region_code == self.region_code.upper(),
                    import_table_cls.raw_data_instance == self.raw_data_instance.value,
                    import_table_cls.file_id.in_(file_id_subquery),
                )
            )
            .values(
                raw_data_instance=new_instance_manager.raw_data_instance.value,
            )
        )
        session.execute(update_query)
