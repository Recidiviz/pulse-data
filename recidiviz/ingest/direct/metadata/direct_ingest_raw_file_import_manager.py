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
"""Class for managing interaction with DirectIngestRawFileImport and 
DirectIngestRawFileImportRun
"""
import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, select

from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
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


class DirectIngestRawFileImportManager:
    """Class for storing information about raw data import metadata. This class manages
    interaction with DirectIngestRawFileImport and DirectIngestRawFileImportRun.

    n.b. while this class manages interaction w/ our file metadata tables, they are also
    queried in the airflow context with raw sql. all relevant updates to the tables'
    logic must also be reflected in the raw data import dag's query logic.
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
    ) -> None:
        self.region_code = region_code.upper()
        self.raw_data_instance = raw_data_instance
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def start_import_run(
        self, dag_run_id: str, import_run_start: Optional[datetime.datetime] = None
    ) -> entities.DirectIngestRawFileImportRun:
        """Uses the provided |dag_run_id| to create and return a raw data import run
        object.
        """
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            new_import_run = schema.DirectIngestRawFileImportRun(
                dag_run_id=dag_run_id,
                import_run_start=import_run_start
                or datetime.datetime.now(tz=datetime.UTC),
                region_code=self.region_code,
                raw_data_instance=self.raw_data_instance.value,
            )

            session.add(new_import_run)

            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

            return convert_schema_object_to_entity(
                new_import_run,
                entities.DirectIngestRawFileImportRun,
                populate_direct_back_edges=False,
            )

    def start_file_import(
        self,
        file_id: int,
        import_run_id: int,
        historical_diffs_active: bool,
        import_status: DirectIngestRawFileImportStatus = DirectIngestRawFileImportStatus.STARTED,
    ) -> entities.DirectIngestRawFileImport:
        """Uses the provided |file_id| and |import_run_id| to create and return a raw
        file import object.
        """

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            new_import = schema.DirectIngestRawFileImport(
                file_id=file_id,
                import_run_id=import_run_id,
                import_status=import_status.value,
                historical_diffs_active=historical_diffs_active,
                region_code=self.region_code,
                raw_data_instance=self.raw_data_instance.value,
            )

            session.add(new_import)

            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

            return convert_schema_object_to_entity(
                new_import,
                entities.DirectIngestRawFileImport,
                populate_direct_back_edges=False,
            )

    @staticmethod
    def _get_import_update_dict(
        import_status: Optional[DirectIngestRawFileImportStatus],
        raw_rows: Optional[int],
        net_new_or_updated_rows: Optional[int],
        deleted_rows: Optional[int],
    ) -> Dict[str, Any]:
        """Filters and returns all non-null values as a dictionary."""
        import_update_kwargs = {
            "import_status": (import_status.value if import_status else import_status),
            "raw_rows": raw_rows,
            "net_new_or_updated_rows": net_new_or_updated_rows,
            "deleted_rows": deleted_rows,
        }

        return {
            field: value
            for field, value in import_update_kwargs.items()
            if value is not None
        }

    def update_file_import_by_id(
        self,
        file_import_id: int,
        import_status: Optional[DirectIngestRawFileImportStatus] = None,
        raw_rows: Optional[int] = None,
        net_new_or_updated_rows: Optional[int] = None,
        deleted_rows: Optional[int] = None,
    ) -> None:
        """Updates the import run info related to the provided |file_import_id| with
        the provided import run information.
        """
        import_update_dict = self._get_import_update_dict(
            import_status,
            raw_rows,
            net_new_or_updated_rows,
            deleted_rows,
        )

        with SessionFactory.using_database(self.database_key) as session:

            update_query = (
                schema.DirectIngestRawFileImport.__table__.update()
                .where(
                    schema.DirectIngestRawFileImport.file_import_id == file_import_id
                )
                .values(**import_update_dict)
            )

            result = session.execute(update_query)

            if result.rowcount != 1:
                raise ValueError(
                    f"Unexpected updated row count; got {result.rowcount} but expected 1"
                )

    def update_most_recent_file_import_for_file_id(
        self,
        file_id: int,
        import_status: Optional[DirectIngestRawFileImportStatus] = None,
        raw_rows: Optional[int] = None,
        net_new_or_updated_rows: Optional[int] = None,
        deleted_rows: Optional[int] = None,
    ) -> None:
        """Updates the import run related to the provided |file_id| with the most
        recent DirectIngestRawFileImportRun::import_run_start associated with

        Note: Use this with caution! Unless the operations db lock is held, there is a
        chance that the most recent import run associated with the file_id is not
        necessarily the import run you intend it to be.
        """
        import_update_dict = self._get_import_update_dict(
            import_status,
            raw_rows,
            net_new_or_updated_rows,
            deleted_rows,
        )

        with SessionFactory.using_database(self.database_key) as session:
            import_id_scalar = (
                session.query(schema.DirectIngestRawFileImport.file_import_id)
                .filter(schema.DirectIngestRawFileImport.file_id == file_id)
                .join(schema.DirectIngestRawFileImportRun)
                .order_by(schema.DirectIngestRawFileImportRun.import_run_start.desc())
                .limit(1)
                .scalar_subquery()
            )
            update_query = (
                schema.DirectIngestRawFileImport.__table__.update()
                .where(
                    schema.DirectIngestRawFileImport.file_import_id == import_id_scalar
                )
                .values(**import_update_dict)
            )

            result = session.execute(update_query)

            if result.rowcount != 1:
                raise ValueError(
                    f"Unexpected updated row count; got {result.rowcount} but expected 1"
                )

    def get_import_by_id(
        self, file_import_id: int
    ) -> entities.DirectIngestRawFileImport:
        """Returns the file import associated with a given |file_import_id|,
        throwing an error if one doesn't exist.
        """

        with SessionFactory.using_database(self.database_key) as session:
            file_import = (
                session.query(schema.DirectIngestRawFileImport)
                .filter_by(file_import_id=file_import_id)
                .one()
            )

            return convert_schema_object_to_entity(
                file_import,
                entities.DirectIngestRawFileImport,
                populate_direct_back_edges=False,
            )

    def get_import_run_by_id(
        self, import_run_id: int
    ) -> entities.DirectIngestRawFileImportRun:
        """Returns the import run associated with a given |import_run_id|,
        throwing an error if one doesn't exist.
        """

        with SessionFactory.using_database(self.database_key) as session:
            import_run = (
                session.query(schema.DirectIngestRawFileImportRun)
                .filter_by(import_run_id=import_run_id)
                .one()
            )

            return convert_schema_object_to_entity(
                import_run,
                entities.DirectIngestRawFileImportRun,
                populate_direct_back_edges=True,
            )

    def get_import_for_file_id(
        self, file_id: int
    ) -> List[entities.DirectIngestRawFileImport]:
        """Returns all import runs associated with a given |file_id|."""

        with SessionFactory.using_database(self.database_key) as session:
            import_runs = (
                session.query(schema.DirectIngestRawFileImport)
                .filter_by(file_id=file_id)
                .all()
            )

            return [
                convert_schema_object_to_entity(
                    import_run,
                    entities.DirectIngestRawFileImport,
                    populate_direct_back_edges=False,
                )
                for import_run in import_runs
            ]

    def get_n_most_recent_imports_for_file_tag(
        self, file_tag: str, n: int = 10
    ) -> List[entities.DirectIngestRawFileImport]:
        """Given a |file_tag| and |n|, returns, at most, |n| of the most recent import
        runs associated with |file_tag|.
        """
        with SessionFactory.using_database(self.database_key) as session:
            import_runs = (
                session.query(schema.DirectIngestRawFileImport)
                .join(schema.DirectIngestRawBigQueryFileMetadata)
                .filter(
                    schema.DirectIngestRawBigQueryFileMetadata.file_tag == file_tag,
                    schema.DirectIngestRawFileImport.region_code == self.region_code,
                    schema.DirectIngestRawFileImport.raw_data_instance
                    == self.raw_data_instance.value,
                )
                .join(schema.DirectIngestRawFileImportRun)
                .order_by(schema.DirectIngestRawFileImportRun.import_run_start.desc())
                .limit(n)
                .all()
            )

            return [
                convert_schema_object_to_entity(
                    import_run,
                    entities.DirectIngestRawFileImport,
                    populate_direct_back_edges=True,
                )
                for import_run in import_runs
            ]

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "DirectIngestRawFileImportManager",
        session: Session,
    ) -> None:
        """Take all import runs that have a file_id that has `is_invalidated=False`
        and transfer to the instance associated with the new_instance_manager
        """
        if (
            new_instance_manager.raw_data_instance == self.raw_data_instance
            or new_instance_manager.region_code != self.region_code
        ):
            raise ValueError(
                "Either state codes are not the same or new instance is same as origin."
            )

        file_import_table_cls = schema.DirectIngestRawFileImport
        import_run_table_cls = schema.DirectIngestRawFileImportRun
        bq_file_table_cls = schema.DirectIngestRawBigQueryFileMetadata
        # check destination instance does not have any valid metadata rows
        file_import_check_query = (
            session.query(file_import_table_cls)
            .join(bq_file_table_cls)
            .filter(
                file_import_table_cls.region_code == self.region_code.upper(),
                file_import_table_cls.raw_data_instance
                == new_instance_manager.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                bq_file_table_cls.is_invalidated == False,
            )
            .all()
        )
        import_run_check_query = (
            session.query(import_run_table_cls)
            .join(file_import_table_cls)
            .join(bq_file_table_cls)
            .filter(
                import_run_table_cls.region_code == self.region_code.upper(),
                import_run_table_cls.raw_data_instance
                == new_instance_manager.raw_data_instance.value,
                # pylint: disable=singleton-comparison
                bq_file_table_cls.is_invalidated == False,
            )
            .all()
        )
        if file_import_check_query or import_run_check_query:
            raise ValueError(
                f"Destination instance should not have any file_ids that reference "
                f"valid raw big query file metadata rows. Found [{len(import_run_check_query) + len(file_import_check_query)}]."
            )

        # gather requires ids

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

        import_run_id_subquery = (
            select([file_import_table_cls.import_run_id])
            .distinct()
            .where(
                and_(
                    file_import_table_cls.region_code == self.region_code.upper(),
                    file_import_table_cls.raw_data_instance
                    == self.raw_data_instance.value,
                    file_import_table_cls.file_id.in_(file_id_subquery),
                )
            )
            .scalar_subquery()
        )

        import_run_update_query = (
            import_run_table_cls.__table__.update()
            .where(
                and_(
                    import_run_table_cls.region_code == self.region_code.upper(),
                    import_run_table_cls.raw_data_instance
                    == self.raw_data_instance.value,
                    import_run_table_cls.import_run_id.in_(import_run_id_subquery),
                )
            )
            .values(
                raw_data_instance=new_instance_manager.raw_data_instance.value,
            )
        )

        session.execute(import_run_update_query)

        file_import_update_query = (
            file_import_table_cls.__table__.update()
            .where(
                and_(
                    file_import_table_cls.region_code == self.region_code.upper(),
                    file_import_table_cls.raw_data_instance
                    == self.raw_data_instance.value,
                    file_import_table_cls.file_id.in_(file_id_subquery),
                )
            )
            .values(
                raw_data_instance=new_instance_manager.raw_data_instance.value,
            )
        )
        session.execute(file_import_update_query)
