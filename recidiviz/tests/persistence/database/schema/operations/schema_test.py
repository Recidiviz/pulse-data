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
import unittest

from more_itertools import one
from sqlalchemy.exc import IntegrityError

from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.utils import fakes


class OperationsSchemaTest(unittest.TestCase):
    """Tests for the schema defined in operations/schema.py."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(OperationsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_raw_file_metadata(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime.now(),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 11),
        )
        session.add(raw_metadata)
        session.commit()
        result_metadata = one(session.query(schema.DirectIngestRawFileMetadata).all())
        self.assertEqual(result_metadata, raw_metadata)
        self.assertIsNotNone(result_metadata.file_id)

    def test_raw_file_metadata_all_fields(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 10, 12),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 11),
            processed_time=datetime.datetime.now(),
        )
        session.add(raw_metadata)
        session.commit()
        result_metadata = one(session.query(schema.DirectIngestRawFileMetadata).all())
        self.assertEqual(result_metadata, raw_metadata)
        self.assertIsNotNone(result_metadata.file_id)

    def test_raw_file_metadata_normalized_file_name_unique_constraint(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata_1 = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 10, 11),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 10),
        )
        raw_metadata_2 = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 11, 12),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 11, 11),
        )

        session.add(raw_metadata_1)
        session.add(raw_metadata_2)

        with self.assertRaises(IntegrityError):
            session.commit()

        session = SessionFactory.for_schema_base(OperationsBase)
        self.assertEqual([], session.query(schema.DirectIngestRawFileMetadata).all())

    def test_raw_file_metadata_normalized_file_name_nonnull_constraint(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 10, 11),
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 10),
        )

        session.add(raw_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()

        session = SessionFactory.for_schema_base(OperationsBase)
        self.assertEqual([], session.query(schema.DirectIngestRawFileMetadata).all())

    def test_raw_file_metadata_discovery_time_name_nonnull_constraint(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 10),
        )

        session.add(raw_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()

        session = SessionFactory.for_schema_base(OperationsBase)
        self.assertEqual([], session.query(schema.DirectIngestRawFileMetadata).all())

    def test_raw_file_metadata_normalized_file_name_unique_constraint_2(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata_1 = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 10, 11),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 10, 10),
        )

        session.add(raw_metadata_1)
        session.commit()

        session = SessionFactory.for_schema_base(OperationsBase)
        raw_metadata_2 = schema.DirectIngestRawFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2019, 11, 12),
            normalized_file_name="foo.txt",
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2019, 11, 11),
        )
        session.add(raw_metadata_2)

        with self.assertRaises(IntegrityError):
            session.commit()

        session = SessionFactory.for_schema_base(OperationsBase)
        self.assertEqual(
            1, len(session.query(schema.DirectIngestRawFileMetadata).all())
        )

    def test_ingest_file_metadata(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
        )
        session.add(ingest_file_metadata)
        session.commit()
        result_metadata = one(
            session.query(schema.DirectIngestIngestFileMetadata).all()
        )
        self.assertEqual(result_metadata, ingest_file_metadata)
        self.assertIsNotNone(result_metadata.file_id)

    def test_ingest_file_metadata_split_file(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            is_file_split=True,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            normalized_file_name="file_name.csv",
        )
        session.add(ingest_file_metadata)
        session.commit()
        result_metadata = one(
            session.query(schema.DirectIngestIngestFileMetadata).all()
        )
        self.assertEqual(result_metadata, ingest_file_metadata)
        self.assertIsNotNone(result_metadata.file_id)

    def test_ingest_file_metadata_split_file_no_file_name_raises(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            is_file_split=True,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
        )
        session.add(ingest_file_metadata)
        with self.assertRaises(IntegrityError):
            session.commit()

    def test_ingest_file_metadata_export_time_without_file_name_raises(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            export_time=datetime.datetime(2020, 5, 12),
        )

        session.add(ingest_file_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()

    def test_ingest_file_metadata_file_name_without_export_time_does_not_raise(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            normalized_file_name="foo.txt",
        )

        session.add(ingest_file_metadata)
        session.commit()
        result_metadata = one(
            session.query(schema.DirectIngestIngestFileMetadata).all()
        )
        self.assertEqual(result_metadata, ingest_file_metadata)
        self.assertIsNotNone(result_metadata.file_id)

    def test_ingest_file_discovery_time_no_export_time_raises(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            discovery_time=datetime.datetime(2020, 5, 12),
        )
        session.add(ingest_file_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()

    def test_ingest_file_processed_time_no_discovery_time_raises(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            export_time=datetime.datetime(2020, 5, 12),
            normalized_file_name="foo.txt",
            processed_time=datetime.datetime(2020, 5, 13),
        )
        session.add(ingest_file_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()

    def test_ingest_file_datetimes_contained_constraint(self):
        session = SessionFactory.for_schema_base(OperationsBase)
        ingest_file_metadata = schema.DirectIngestIngestFileMetadata(
            region_code="us_xx_yyyy",
            file_tag="file_tag",
            is_invalidated=False,
            job_creation_time=datetime.datetime.now(),
            datetimes_contained_lower_bound_exclusive=datetime.datetime(2020, 6, 11),
            datetimes_contained_upper_bound_inclusive=datetime.datetime(2020, 5, 11),
            export_time=datetime.datetime(2020, 5, 12),
            discovery_time=datetime.datetime(2020, 5, 12),
            normalized_file_name="foo.txt",
            processed_time=datetime.datetime(2020, 5, 13),
        )
        session.add(ingest_file_metadata)

        with self.assertRaises(IntegrityError):
            session.commit()
