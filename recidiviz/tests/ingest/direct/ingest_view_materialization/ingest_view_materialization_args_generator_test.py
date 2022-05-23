# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for IngestViewMaterializationArgsGenerator."""
import datetime
import unittest
from typing import List, Optional

import attr
import sqlalchemy
from freezegun import freeze_time
from mock import Mock, patch

from recidiviz.ingest.direct.ingest_view_materialization.bq_based_materialization_args_generator_delegate import (
    BQBasedMaterializationArgsGeneratorDelegate,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator import (
    IngestViewMaterializationArgsGenerator,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestViewMaterializationMetadata,
)
from recidiviz.tests.ingest.direct.fakes.fake_single_ingest_view_collector import (
    FakeSingleIngestViewCollector,
)
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.regions import Region

_ID = 1
_DATE_1 = datetime.datetime(year=2019, month=7, day=20)
_DATE_2 = datetime.datetime(year=2020, month=7, day=20)
_DATE_3 = datetime.datetime(year=2021, month=7, day=20)
_DATE_4 = datetime.datetime(year=2022, month=4, day=14)
_DATE_5 = datetime.datetime(year=2022, month=4, day=15)


@attr.s
class _IngestFileMetadata:
    file_tag: str = attr.ib()
    datetimes_contained_lower_bound_exclusive: datetime.datetime = attr.ib()
    datetimes_contained_upper_bound_inclusive: datetime.datetime = attr.ib()
    job_creation_time: datetime.datetime = attr.ib()
    export_time: Optional[datetime.datetime] = attr.ib()


@attr.s
class _RawFileMetadata:
    file_tag: str = attr.ib()
    datetimes_contained_upper_bound_inclusive: datetime.datetime = attr.ib()
    discovery_time: datetime.datetime = attr.ib()
    processed_time: datetime.datetime = attr.ib()


# TODO(#7843): Debug and write test for edge case crash while uploading many raw data
#  files while queues are unpaused.
class TestIngestViewMaterializationArgsGenerator(unittest.TestCase):
    """Tests for IngestViewMaterializationArgsGenerator."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = "ingest_database_name"
        self.ingest_instance = DirectIngestInstance.PRIMARY
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def create_args_generator(
        self,
        region: Region,
        is_detect_row_deletion_view: bool = False,
        materialize_raw_data_table_views: bool = False,
        ingest_view_name: Optional[str] = None,
    ) -> IngestViewMaterializationArgsGenerator:
        raw_file_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
            region.region_code, self.ingest_database_name
        )
        ingest_view_name = ingest_view_name or "ingest_view"
        return IngestViewMaterializationArgsGenerator(
            region=region,
            raw_file_metadata_manager=raw_file_metadata_manager,
            delegate=BQBasedMaterializationArgsGeneratorDelegate(
                metadata_manager=DirectIngestViewMaterializationMetadataManager(
                    region.region_code, self.ingest_instance
                ),
            ),
            view_collector=FakeSingleIngestViewCollector(  # type: ignore[arg-type]
                region,
                ingest_view_name=ingest_view_name or "ingest_view",
                is_detect_row_deletion_view=is_detect_row_deletion_view,
                materialize_raw_data_table_views=materialize_raw_data_table_views,
            ),
            launched_ingest_views=[ingest_view_name],
        )

    def test_getIngestViewExportTaskArgs_happy(self) -> None:
        # Arrange
        region = fake_region(environment="production")
        args_generator = self.create_args_generator(region)
        args_generator.delegate.metadata_manager.get_most_recent_registered_job = Mock(  # type: ignore
            return_value=DirectIngestViewMaterializationMetadata(
                region_code=region.region_code,
                ingest_view_name="ingest_view",
                instance=self.ingest_instance,
                materialization_time=_DATE_1,
                is_invalidated=False,
                job_creation_time=_DATE_1,
                lower_bound_datetime_exclusive=_DATE_1,
                upper_bound_datetime_inclusive=_DATE_1,
            )
        )
        args_generator.raw_file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
            return_value=[
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag="my_raw_file",
                    discovery_time=_DATE_2,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_2,
                )
            ]
        )

        # Act
        args = args_generator.get_ingest_view_materialization_task_args()

        # Assert
        self.assertListEqual(
            args,
            [
                BQIngestViewMaterializationArgs(
                    ingest_view_name="ingest_view",
                    ingest_instance_=self.ingest_instance,
                    lower_bound_datetime_exclusive=_DATE_1,
                    upper_bound_datetime_inclusive=_DATE_2,
                )
            ],
        )

    def test_getIngestViewExportTaskArgs_rawFileOlderThanLastExport(self) -> None:
        # Arrange
        region = fake_region(environment="production")
        args_generator = self.create_args_generator(region)
        args_generator.delegate.metadata_manager.get_most_recent_registered_job = Mock(  # type: ignore
            return_value=DirectIngestViewMaterializationMetadata(
                region_code=region.region_code,
                ingest_view_name="ingest_view",
                instance=self.ingest_instance,
                materialization_time=_DATE_2,
                is_invalidated=False,
                job_creation_time=_DATE_2,
                lower_bound_datetime_exclusive=_DATE_2,
                upper_bound_datetime_inclusive=_DATE_2,
            )
        )
        args_generator.raw_file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
            return_value=[
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag="ingest_view",
                    discovery_time=_DATE_1,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_1,
                )
            ]
        )

        # Act
        with self.assertRaisesRegex(
            ValueError, r"upper bound date.*before the last valid export"
        ):
            args_generator.get_ingest_view_materialization_task_args()

    def test_getIngestViewExportTaskArgs_rawCodeTableOlderThanLastExport(self) -> None:
        # Arrange
        CODE_TABLE_TAG = "RECIDIVIZ_REFERENCE_ingest_view"
        region = fake_region(environment="production")
        args_generator = self.create_args_generator(
            region, ingest_view_name=CODE_TABLE_TAG
        )
        args_generator.delegate.metadata_manager.get_most_recent_registered_job = Mock(  # type: ignore
            return_value=DirectIngestViewMaterializationMetadata(
                region_code=region.region_code,
                ingest_view_name="ingest_view_using_code_table",
                instance=self.ingest_instance,
                materialization_time=_DATE_2,
                is_invalidated=False,
                job_creation_time=_DATE_2,
                lower_bound_datetime_exclusive=(_DATE_2 - datetime.timedelta(days=7)),
                upper_bound_datetime_inclusive=_DATE_2,
            )
        )
        args_generator.raw_file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
            return_value=[
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag=CODE_TABLE_TAG,
                    discovery_time=_DATE_1,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_1,
                )
            ]
        )

        # Act
        args = args_generator.get_ingest_view_materialization_task_args()

        # Assert
        # New code tables are backdated but don't need to be re-ingested, so ignore them.
        self.assertListEqual(args, [])

    def test_getIngestViewExportTaskArgs_reverseDateDiff(self) -> None:
        # Arrange
        region = fake_region(environment="production")
        args_generator = self.create_args_generator(
            region, is_detect_row_deletion_view=True
        )
        args_generator.delegate.metadata_manager.get_most_recent_registered_job = Mock(  # type: ignore
            return_value=None
        )
        args_generator.raw_file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
            return_value=[
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag="file_tag",
                    discovery_time=_DATE_1,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_1,
                ),
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag="file_tag",
                    discovery_time=_DATE_2,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_2,
                ),
            ]
        )

        # Act
        args = args_generator.get_ingest_view_materialization_task_args()

        # Assert
        self.assertListEqual(
            args,
            [
                # We create args for the time between DATE 1 and DATE 2 but not for
                # the time between None and DATE 1 (e.g. the historical query).
                BQIngestViewMaterializationArgs(
                    ingest_view_name="ingest_view",
                    ingest_instance_=self.ingest_instance,
                    lower_bound_datetime_exclusive=_DATE_1,
                    upper_bound_datetime_inclusive=_DATE_2,
                )
            ],
        )

    def test_ingest_view_export_job_created_between_raw_file_discoveries_with_same_datetime(
        self,
    ) -> None:
        """Exhibits behavior in a scenario where a set of identically dated raw files
        are slow to upload and an ingest view task gets generated between two uploads
        for raw file dependencies of this view. All timestamps are generated from
        real-world crash.

        It is not the responsibility of the export manager to handle this scenario -
        instead, users of the export manager must take measures to ensure that we don't
        attempt to schedule ingest view export jobs while a batch raw data import is
        in progress.
        """
        ingest_view_name = "ingest_view"
        file_tag_1 = "file_tag_first"
        file_tag_2 = "tagFullHistoricalExport"

        raw_file_datetimes = [
            # <Assume there are also raw files from 2021-07-24 (the day before here)>
            # This raw file was discovered at the same time, but uploaded 10 min earlier
            _RawFileMetadata(
                file_tag=file_tag_1,
                datetimes_contained_upper_bound_inclusive=datetime.datetime.fromisoformat(
                    "2021-07-25 09:02:24"
                ),
                discovery_time=datetime.datetime.fromisoformat(
                    "2021-07-25 09:29:33.690766"
                ),
                processed_time=datetime.datetime.fromisoformat(
                    "2021-07-25 09:31:17.856534"
                ),
            ),
            # This raw file took 10 extra minutes to upload and is also a dependency of
            # movement_facility_location_offstat_incarceration_periods
            _RawFileMetadata(
                file_tag=file_tag_2,
                datetimes_contained_upper_bound_inclusive=datetime.datetime.fromisoformat(
                    "2021-07-25 09:02:24"
                ),
                discovery_time=datetime.datetime.fromisoformat(
                    "2021-07-25 09:29:37.095288"
                ),
                processed_time=datetime.datetime.fromisoformat(
                    "2021-07-25 09:41:15.265905"
                ),
            ),
        ]

        ingest_file_metadata = [
            # This job was created between discovery of files 1 and 2, task was likely queued immediately.
            _IngestFileMetadata(
                file_tag=ingest_view_name,
                datetimes_contained_lower_bound_exclusive=datetime.datetime.fromisoformat(
                    "2021-07-24 09:02:44"
                ),
                datetimes_contained_upper_bound_inclusive=datetime.datetime.fromisoformat(
                    "2021-07-25 09:02:24"
                ),
                job_creation_time=datetime.datetime.fromisoformat(
                    "2021-07-25 09:29:35.195456"
                ),
                # Also crashes if export time is set
                export_time=None,
            ),
        ]

        with self.assertRaisesRegex(
            sqlalchemy.exc.IntegrityError,
            "CHECK constraint failed: datetime_bounds_ordering",
        ):
            self.run_get_args_test(
                ingest_view_name=ingest_view_name,
                committed_raw_file_metadata=raw_file_datetimes,
                committed_ingest_materialization_metadata=ingest_file_metadata,
            )

    def run_get_args_test(
        self,
        ingest_view_name: str,
        committed_raw_file_metadata: List[_RawFileMetadata],
        committed_ingest_materialization_metadata: List[_IngestFileMetadata],
    ) -> List[BQIngestViewMaterializationArgs]:
        """Runs test to generate ingest view materialization args given provided DB state."""
        region = fake_region(environment="production")

        with SessionFactory.using_database(self.database_key) as session:
            for i, ingest_file_datetimes in enumerate(
                committed_ingest_materialization_metadata
            ):
                metadata = schema.DirectIngestViewMaterializationMetadata(
                    job_id=i,
                    region_code=region.region_code.upper(),
                    instance=self.ingest_instance.value,
                    ingest_view_name=ingest_view_name,
                    is_invalidated=False,
                    lower_bound_datetime_exclusive=ingest_file_datetimes.datetimes_contained_lower_bound_exclusive,
                    upper_bound_datetime_inclusive=ingest_file_datetimes.datetimes_contained_upper_bound_inclusive,
                    job_creation_time=ingest_file_datetimes.job_creation_time,
                    materialization_time=ingest_file_datetimes.export_time,
                )
                session.add(metadata)

        with SessionFactory.using_database(self.database_key) as session:
            for i, raw_file_datetimes_item in enumerate(committed_raw_file_metadata):
                raw_file_metadata = schema.DirectIngestRawFileMetadata(
                    file_id=i,
                    region_code=region.region_code.upper(),
                    file_tag=raw_file_datetimes_item.file_tag,
                    normalized_file_name=f"{raw_file_datetimes_item.file_tag}_{i}_raw",
                    datetimes_contained_upper_bound_inclusive=raw_file_datetimes_item.datetimes_contained_upper_bound_inclusive,
                    discovery_time=raw_file_datetimes_item.discovery_time,
                    processed_time=raw_file_datetimes_item.processed_time,
                )
                session.add(raw_file_metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            args_generator = self.create_args_generator(region)
        with freeze_time(_DATE_5.isoformat()):
            args = args_generator.get_ingest_view_materialization_task_args()

        return args
