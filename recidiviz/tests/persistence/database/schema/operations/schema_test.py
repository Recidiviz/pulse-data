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

from more_itertools import one
from sqlalchemy.exc import IntegrityError

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils import fakes


class OperationsSchemaTest(unittest.TestCase):
    """Tests for the schema defined in operations/schema.py."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_raw_file_metadata(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                discovery_time=datetime.datetime.now(),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 11
                ),
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
                discovery_time=datetime.datetime(2019, 10, 12),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 11
                ),
                processed_time=datetime.datetime.now(),
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

    def test_raw_file_metadata_normalized_file_name_unique_constraint(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata_1 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                discovery_time=datetime.datetime(2019, 10, 11),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 10
                ),
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                is_invalidated=False,
            )
            raw_metadata_2 = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                discovery_time=datetime.datetime(2019, 11, 12),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 11, 11
                ),
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

    def test_raw_file_metadata_normalized_file_name_nonnull_constraint(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            raw_metadata = schema.DirectIngestRawFileMetadata(
                region_code="us_xx_yyyy",
                file_tag="file_tag",
                discovery_time=datetime.datetime(2019, 10, 11),
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 10
                ),
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
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 10
                ),
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
                discovery_time=datetime.datetime(2019, 10, 11),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 10, 10
                ),
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
                discovery_time=datetime.datetime(2019, 11, 12),
                normalized_file_name="foo.txt",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2019, 11, 11
                ),
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

    def test_ingest_view_materialization_metadata(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=datetime.datetime.now(),
                lower_bound_datetime_exclusive=None,
                upper_bound_datetime_inclusive=datetime.datetime(2020, 5, 11),
                materialization_time=None,
            )
            session.add(metadata)
            session.commit()
            result_metadata = one(
                session.query(schema.DirectIngestViewMaterializationMetadata).all()
            )
            self.assertEqual(result_metadata, metadata)

    def test_ingest_view_materialization_metadata_all_fields(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            now = datetime.datetime.now()
            file_datetime = datetime.datetime(2020, 5, 11)
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=now - datetime.timedelta(hours=1),
                lower_bound_datetime_exclusive=(
                    file_datetime - datetime.timedelta(days=1)
                ),
                upper_bound_datetime_inclusive=file_datetime,
                materialization_time=now,
            )
            session.add(metadata)
            session.commit()
            result_metadata = one(
                session.query(schema.DirectIngestViewMaterializationMetadata).all()
            )
            self.assertEqual(result_metadata, metadata)

    def test_ingest_view_materialization_datetime_bounds_ordering_constraint(
        self,
    ) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            now = datetime.datetime.now()
            file_datetime = datetime.datetime(2020, 5, 11)
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=now,
                lower_bound_datetime_exclusive=file_datetime,
                upper_bound_datetime_inclusive=file_datetime,
                materialization_time=now - datetime.timedelta(hours=1),
            )
            session.add(metadata)

            with self.assertRaisesRegex(
                IntegrityError, "CHECK constraint failed: datetime_bounds_ordering"
            ):
                session.commit()

    def test_ingest_view_materialization_job_times_ordering_constraint(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            now = datetime.datetime.now()
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=now,
                lower_bound_datetime_exclusive=None,
                upper_bound_datetime_inclusive=datetime.datetime(2020, 5, 11),
                materialization_time=now - datetime.timedelta(hours=1),
            )
            session.add(metadata)

            with self.assertRaisesRegex(
                IntegrityError, "CHECK constraint failed: job_times_ordering"
            ):
                session.commit()

    def test_ingest_view_materialization_job_uniqueness_constraint(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=datetime.datetime.now(),
                lower_bound_datetime_exclusive=None,
                upper_bound_datetime_inclusive=datetime.datetime(2020, 5, 11),
                materialization_time=None,
            )
            session.add(metadata)
            session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            duplicate_metadata = schema.DirectIngestViewMaterializationMetadata(
                region_code="us_xx_yyyy",
                instance="SECONDARY",
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=datetime.datetime.now(),
                lower_bound_datetime_exclusive=None,
                upper_bound_datetime_inclusive=datetime.datetime(2020, 5, 11),
                materialization_time=None,
            )
            session.add(duplicate_metadata)
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "Attempting to commit repeated DirectIngestViewMaterializationMetadata row",
            ):
                session.commit()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata_different_instance = (
                schema.DirectIngestViewMaterializationMetadata(
                    region_code="us_xx_yyyy",
                    instance="PRIMARY",
                    ingest_view_name="ingest_view_name",
                    is_invalidated=False,
                    job_creation_time=datetime.datetime.now(),
                    lower_bound_datetime_exclusive=None,
                    upper_bound_datetime_inclusive=datetime.datetime(2020, 5, 11),
                    materialization_time=None,
                )
            )
            session.add(metadata_different_instance)
            session.commit()
