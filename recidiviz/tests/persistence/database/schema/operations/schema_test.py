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
"""Tests for the schema defined in operations/schema.py."""
import datetime
import sqlite3
import unittest
from typing import Optional

import pytest
import pytz
from more_itertools import one
from sqlalchemy.exc import IntegrityError

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


@pytest.mark.uses_db
class OperationsSchemaTest(unittest.TestCase):
    """Tests for the schema defined in operations/schema.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_raw_file_metadata(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime.now(),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 11, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            session.add(raw_metadata)
            session.commit()
            result_metadata = one(
                session.query(schema.DirectIngestRawFileMetadata).all()
            )
            self.assertEqual(result_metadata, raw_metadata)
            self.assertIsNotNone(result_metadata.file_id)

    def test_raw_file_metadata_all_fields(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 10, 12),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 11, tzinfo=pytz.UTC),
                file_processed_time=datetime.datetime.now(tz=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            session.add(raw_metadata)
            session.commit()
            result_metadata = one(
                session.query(schema.DirectIngestRawFileMetadata).all()
            )
            self.assertEqual(result_metadata, raw_metadata)
            self.assertIsNotNone(result_metadata.file_id)

    def test_raw_file_metadata_normalized_file_name_unique_non_invalidated_index(
        self,
    ) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata_1 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 10, 11),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 10, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            raw_metadata_2 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 11, 12),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 11, 11, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )

            session.add(raw_metadata_1)
            session.add(raw_metadata_2)

            with self.assertRaises(IntegrityError):
                session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                [], session.query(schema.DirectIngestRawFileMetadata).all()
            )

    def test_raw_file_metadata_normalized_file_name_unique_one_invalidated_index(
        self,
    ) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata_1 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 10, 11),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 10, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            raw_metadata_2 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 11, 12),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 11, 11, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=True,
            )

            session.add(raw_metadata_1)
            session.add(raw_metadata_2)

            # Should not raise any errors because one of the added rows is invalidated, and therefore the index
            # restrictions do not apply.
            session.commit()

            with SessionFactory.using_database(
                self.database_key, autocommit=False
            ) as session:
                self.assertEqual(
                    2, len(session.query(schema.DirectIngestRawFileMetadata).all())
                )

    def test_raw_file_metadata_normalized_file_name_nonnull_constraint(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 10, 11),
                update_datetime=datetime.datetime(2019, 10, 10, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )

            session.add(raw_metadata)

            with self.assertRaises(IntegrityError):
                session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                [], session.query(schema.DirectIngestRawFileMetadata).all()
            )

    def test_raw_file_metadata_discovery_time_name_nonnull_constraint(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 10, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )

            session.add(raw_metadata)

            with self.assertRaises(IntegrityError):
                session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                [], session.query(schema.DirectIngestRawFileMetadata).all()
            )

    def test_raw_file_metadata_normalized_file_name_unique_constraint_2(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            raw_metadata_1 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 10, 11),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 10, 10, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )

            session.add(raw_metadata_1)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata_2 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(2019, 11, 12),
                normalized_file_name="foo.txt",
                update_datetime=datetime.datetime(2019, 11, 11, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            session.add(raw_metadata_2)

            with self.assertRaises(IntegrityError):
                session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            self.assertEqual(
                1, len(session.query(schema.DirectIngestRawFileMetadata).all())
            )

    def test_direct_ingest_instance_status_uniqueness_constraint(self) -> None:
        shared_datetime = datetime.datetime.now(tz=pytz.UTC)
        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestInstanceStatus(
                region_code=StateCode.US_XX.value,
                instance=DirectIngestInstance.PRIMARY.value,
                status_timestamp=shared_datetime,
                status=DirectIngestStatus.INITIAL_STATE.value,
            )
            session.add(metadata)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            duplicate_timestamp = schema.DirectIngestInstanceStatus(
                region_code=StateCode.US_XX.value,
                instance=DirectIngestInstance.PRIMARY.value,
                # Different status, but same timestamp
                status_timestamp=shared_datetime,
                status=DirectIngestStatus.RAW_DATA_UP_TO_DATE.value,
            )

            session.add(duplicate_timestamp)
            with self.assertRaises(IntegrityError):
                session.commit()

    def test_direct_ingest_instance_status_timestamp_ordering(self) -> None:
        shared_datetime = datetime.datetime.now(tz=pytz.UTC)
        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestInstanceStatus(
                region_code=StateCode.US_XX.value,
                instance=DirectIngestInstance.PRIMARY.value,
                status_timestamp=shared_datetime,
                status=DirectIngestStatus.INITIAL_STATE.value,
            )
            session.add(metadata)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            earlier_timestamp = schema.DirectIngestInstanceStatus(
                region_code=StateCode.US_XX.value,
                instance=DirectIngestInstance.PRIMARY.value,
                # Add new row whose datetime is earlier than previous row
                status_timestamp=shared_datetime - datetime.timedelta(days=1),
                status=DirectIngestStatus.RAW_DATA_UP_TO_DATE.value,
            )

            session.add(earlier_timestamp)
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "Attempting to commit a DirectIngestInstanceStatus",
            ):
                session.commit()
