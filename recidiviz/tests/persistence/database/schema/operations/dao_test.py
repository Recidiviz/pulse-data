#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================

"""Tests for operations/dao.py."""

import datetime
from typing import Optional
from unittest import TestCase

import pytest
import pytz

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import dao, schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(
            SchemaType.OPERATIONS
        )
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    # TODO(#14198): move operations/dao.py functionality into DirectIngestRawFileMetadataManager and migrate tests
    # to postgres_direct_ingest_file_metadata_manager_test.

    def test_get_raw_file_metadata_for_file_id(self) -> None:
        self.add_raw_file_metadata(
            file_id=10,
            region_code="US_XX",
            file_tag="file_tag_1",
            normalized_file_name="normalized_file_tag_1",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # If this call doesn't raise an exception, it means that the correct metadata row was retrieved.
            metadata = dao.get_raw_file_metadata_for_file_id(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            self.assertEqual(metadata.file_id, 10)

    def test_raw_file_metadata_mark_as_invalidated(self) -> None:
        self.add_raw_file_metadata(
            file_id=10,
            region_code="US_XX",
            file_tag="file_tag_1",
            normalized_file_name="normalized_file_tag_1",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            dao.mark_raw_file_as_invalidated(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            metadata = dao.get_raw_file_metadata_for_file_id(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            self.assertEqual(metadata.is_invalidated, True)

    def add_raw_file_metadata(
        self, file_id: int, region_code: str, file_tag: str, normalized_file_name: str
    ) -> None:
        """Used for testing purposes. Add a new row in the direct_ingest_raw_file_metadata table using the
        parameters."""
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestRawFileMetadata(
                    region_code=region_code,
                    file_tag=file_tag,
                    file_id=file_id,
                    normalized_file_name=normalized_file_name,
                    discovery_time=datetime.datetime.now(tz=pytz.UTC),
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=datetime.datetime.now(
                        tz=pytz.UTC
                    ),
                    raw_data_instance=DirectIngestInstance.PRIMARY.value,
                    is_invalidated=False,
                )
            )
