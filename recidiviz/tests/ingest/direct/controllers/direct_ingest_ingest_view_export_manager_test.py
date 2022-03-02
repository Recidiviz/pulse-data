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
"""Tests for direct_ingest_ingest_view_export_manager.py."""
import datetime
import unittest
from typing import Dict, List, Optional

import attr
import mock
import sqlalchemy
from freezegun import freeze_time
from google.cloud.bigquery import ScalarQueryParameter
from mock import Mock, patch
from more_itertools import one

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.controllers.direct_ingest_ingest_view_export_manager import (
    DirectIngestIngestViewExportManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestIngestFileMetadata,
    DirectIngestRawFileMetadata,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.regions import Region

_ID = 1
_DATE_1 = datetime.datetime(year=2019, month=7, day=20)
_DATE_2 = datetime.datetime(year=2020, month=7, day=20)
_DATE_3 = datetime.datetime(year=2021, month=7, day=20)
_DATE_4 = datetime.datetime(year=2022, month=4, day=14)
_DATE_5 = datetime.datetime(year=2022, month=4, day=15)


_DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT = """DROP TABLE IF EXISTS `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`;
CREATE TABLE `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= @update_timestamp
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);"""


_DATE_2_UPPER_BOUND_MATERIALIZED_RAW_TABLE_CREATE_TABLE_SCRIPT = """CREATE TEMP TABLE file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= @update_timestamp
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= @update_timestamp
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
DROP TABLE IF EXISTS `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`;
CREATE TABLE `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);"""


class _FakeDirectIngestViewBuilder(
    BigQueryViewBuilder[DirectIngestPreProcessedIngestView]
):
    """Fake BQ View Builder for tests."""

    def __init__(
        self,
        tag: str,
        is_detect_row_deletion_view: bool = False,
        materialize_raw_data_table_views: bool = False,
    ):
        self.tag = tag
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    # pylint: disable=unused-argument
    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> DirectIngestPreProcessedIngestView:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )

        query = "select * from {file_tag_first} JOIN {tagFullHistoricalExport} USING (COL_1)"
        primary_key_tables_for_entity_deletion = (
            [] if not self.is_detect_row_deletion_view else ["tagFullHistoricalExport"]
        )
        return DirectIngestPreProcessedIngestView(
            ingest_view_name=self.tag,
            view_query_template=query,
            region_raw_table_config=region_config,
            order_by_cols="colA, colC",
            is_detect_row_deletion_view=self.is_detect_row_deletion_view,
            primary_key_tables_for_entity_deletion=primary_key_tables_for_entity_deletion,
            materialize_raw_data_table_views=self.materialize_raw_data_table_views,
        )

    def build_and_print(self) -> None:
        self.build()

    def should_build(self) -> bool:
        return True

    @property
    def file_tag(self) -> str:
        return self.tag


class _ViewCollector(BigQueryViewCollector[_FakeDirectIngestViewBuilder]):
    """Fake ViewCollector for tests."""

    def __init__(
        self,
        region: Region,
        controller_file_tags: List[str],
        is_detect_row_deletion_view: bool,
        materialize_raw_data_table_views: bool,
    ):
        self.region = region
        self.controller_file_tags = controller_file_tags
        self.is_detect_row_deletion_view = is_detect_row_deletion_view
        self.materialize_raw_data_table_views = materialize_raw_data_table_views

    def collect_view_builders(self) -> List[_FakeDirectIngestViewBuilder]:
        builders = [
            _FakeDirectIngestViewBuilder(
                tag=tag,
                is_detect_row_deletion_view=self.is_detect_row_deletion_view,
                materialize_raw_data_table_views=self.materialize_raw_data_table_views,
            )
            for tag in self.controller_file_tags
        ]

        return builders


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


class DirectIngestIngestViewExportManagerTest(unittest.TestCase):
    """Tests for the DirectIngestIngestViewExportManager class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = "ingest_database_name"
        self.output_bucket_name = "output_bucket_name"
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.client_patcher = patch(
            "recidiviz.big_query.big_query_client.BigQueryClient"
        )
        self.mock_client = self.client_patcher.start().return_value

        project_id_mock = mock.PropertyMock(return_value=self.mock_project_id)
        type(self.mock_client).project_id = project_id_mock

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def to_entity(self, schema_obj: DatabaseEntity) -> Entity:
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False
        )

    @staticmethod
    def create_fake_region(environment: str = "production") -> Region:
        return fake_region(
            region_code="US_XX",
            environment=environment,
        )

    def create_export_manager(
        self,
        region: Region,
        is_detect_row_deletion_view: bool = False,
        materialize_raw_data_table_views: bool = False,
        controller_file_tags: Optional[List[str]] = None,
    ) -> DirectIngestIngestViewExportManager:
        metadata_manager = PostgresDirectIngestFileMetadataManager(
            region.region_code, self.ingest_database_name
        )
        controller_file_tags = (
            ["ingest_view"] if controller_file_tags is None else controller_file_tags
        )
        return DirectIngestIngestViewExportManager(
            region=region,
            fs=FakeGCSFileSystem(),
            output_bucket_name=self.output_bucket_name,
            ingest_instance=DirectIngestInstance.SECONDARY,
            big_query_client=self.mock_client,
            file_metadata_manager=metadata_manager,
            view_collector=_ViewCollector(  # type: ignore[arg-type]
                region,
                controller_file_tags=controller_file_tags,
                is_detect_row_deletion_view=is_detect_row_deletion_view,
                materialize_raw_data_table_views=materialize_raw_data_table_views,
            ),
            launched_file_tags=controller_file_tags,
        )

    @staticmethod
    def generate_query_params_for_date(
        date_param: datetime.datetime,
    ) -> ScalarQueryParameter:
        return ScalarQueryParameter("update_timestamp", "DATETIME", date_param)

    def assert_exported_to_gcs_with_query(self, expected_query: str) -> None:
        self.mock_client.export_query_results_to_cloud_storage.assert_called_once()
        _, kwargs = one(
            self.mock_client.export_query_results_to_cloud_storage.call_args_list
        )
        export_config = one(kwargs["export_configs"])
        normalized_exported_query = export_config.query.replace("\n", "")
        normalized_expected_query = expected_query.replace("\n", "")
        self.assertEqual(normalized_expected_query, normalized_exported_query)

    def test_getIngestViewExportTaskArgs_happy(self) -> None:
        # Arrange
        region = self.create_fake_region(environment="production")
        export_manager = self.create_export_manager(region)
        export_manager.file_metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job = Mock(  # type: ignore
            return_value=DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag="ingest_view",
                normalized_file_name="normalized_file_name",
                processed_time=_DATE_1,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=_DATE_1,
                datetimes_contained_lower_bound_exclusive=_DATE_1,
                datetimes_contained_upper_bound_inclusive=_DATE_1,
                discovery_time=_DATE_1,
                ingest_database_name=self.ingest_database_name,
            )
        )
        export_manager.file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
            return_value=[
                DirectIngestRawFileMetadata(
                    file_id=2,
                    region_code=region.region_code,
                    file_tag="ingest_view",
                    discovery_time=_DATE_2,
                    normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
                    processed_time=None,
                    datetimes_contained_upper_bound_inclusive=_DATE_2,
                )
            ]
        )

        # Act
        args = export_manager.get_ingest_view_export_task_args()

        # Assert
        self.assertListEqual(
            args,
            [
                GcsfsIngestViewExportArgs(
                    ingest_view_name="ingest_view",
                    output_bucket_name=self.output_bucket_name,
                    upper_bound_datetime_prev=_DATE_1,
                    upper_bound_datetime_to_export=_DATE_2,
                )
            ],
        )

    def test_getIngestViewExportTaskArgs_rawFileOlderThanLastExport(self) -> None:
        # Arrange
        region = self.create_fake_region(environment="production")
        export_manager = self.create_export_manager(region)
        export_manager.file_metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job = Mock(  # type: ignore
            return_value=DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag="ingest_view",
                normalized_file_name="normalized_file_name",
                processed_time=_DATE_2,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_2,
                export_time=_DATE_2,
                datetimes_contained_lower_bound_exclusive=_DATE_2,
                datetimes_contained_upper_bound_inclusive=_DATE_2,
                discovery_time=_DATE_2,
                ingest_database_name=self.ingest_database_name,
            )
        )
        export_manager.file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
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
            export_manager.get_ingest_view_export_task_args()

    def test_getIngestViewExportTaskArgs_rawCodeTableOlderThanLastExport(self) -> None:
        # Arrange
        CODE_TABLE_TAG = "RECIDIVIZ_REFERENCE_ingest_view"
        region = self.create_fake_region(environment="production")
        export_manager = self.create_export_manager(
            region, controller_file_tags=[CODE_TABLE_TAG]
        )
        export_manager.file_metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job = Mock(  # type: ignore
            return_value=DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=CODE_TABLE_TAG,
                normalized_file_name="normalized_file_name",
                processed_time=_DATE_2,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_2,
                export_time=_DATE_2,
                datetimes_contained_lower_bound_exclusive=_DATE_2
                - datetime.timedelta(days=7),
                datetimes_contained_upper_bound_inclusive=_DATE_2,
                discovery_time=_DATE_2,
                ingest_database_name=self.ingest_database_name,
            )
        )
        export_manager.file_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime = Mock(  # type: ignore
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
        args = export_manager.get_ingest_view_export_task_args()

        # Assert
        # New code tables are backdated but don't need to be re-ingested, so ignore them.
        self.assertListEqual(args, [])

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_exportViewForArgs_ingestViewExportsDisabled(self) -> None:
        # Arrange
        region = self.create_fake_region(environment="staging")
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        # Act
        with self.assertRaises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_dataset_if_necessary.assert_not_called()
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_exportViewForArgs_noExistingMetadata(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        # Act
        with self.assertRaises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_exportViewForArgs_alreadyExported(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=_DATE_2,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = self.to_entity(metadata)
            session.add(metadata)

        # Act
        export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_exportViewForArgs_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = attr.evolve(
                self.to_entity(metadata), export_time=_DATE_5
            )

            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(region)
        with freeze_time(_DATE_5.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        expected_upper_bound_query = _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT

        self.mock_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str=expected_upper_bound_query,
                    query_parameters=[
                        self.generate_query_params_for_date(
                            export_args.upper_bound_datetime_to_export
                        )
                    ],
                ),
            ]
        )
        expected_query = (
            "SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`"
            "ORDER BY colA, colC;"
        )
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                )
            ]
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_exportViewForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = attr.evolve(
                self.to_entity(metadata), export_time=_DATE_5
            )
            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(region)
        with freeze_time(_DATE_5.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        expected_upper_bound_query = _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT
        expected_lower_bound_query = expected_upper_bound_query.replace(
            "2020_07_20_00_00_00_upper_bound", "2019_07_20_00_00_00_lower_bound"
        )

        self.mock_client.dataset_ref_for_id.assert_has_calls(
            [mock.call("us_xx_ingest_views_20220414_secondary")]
        )
        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [mock.call(dataset_ref=mock.ANY, default_table_expiration_ms=86400000)]
        )
        self.mock_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str=expected_upper_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_2)],
                ),
                mock.call(
                    query_str=expected_lower_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_1)],
                ),
            ]
        )
        expected_query = (
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`) "
            "EXCEPT DISTINCT "
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2019_07_20_00_00_00_lower_bound`)"
            "ORDER BY colA, colC;"
        )
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_exportViewForArgsMaterializedViews(self) -> None:
        # Arrange
        region = self.create_fake_region()

        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = attr.evolve(
                self.to_entity(metadata), export_time=_DATE_5
            )
            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, materialize_raw_data_table_views=True
            )
        with freeze_time(_DATE_5.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        expected_upper_bound_query = (
            _DATE_2_UPPER_BOUND_MATERIALIZED_RAW_TABLE_CREATE_TABLE_SCRIPT
        )
        expected_lower_bound_query = expected_upper_bound_query.replace(
            "2020_07_20_00_00_00_upper_bound", "2019_07_20_00_00_00_lower_bound"
        )

        self.mock_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str=expected_upper_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_2)],
                ),
                mock.call(
                    query_str=expected_lower_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_1)],
                ),
            ]
        )
        expected_query = (
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`) "
            "EXCEPT DISTINCT "
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2019_07_20_00_00_00_lower_bound`)"
            "ORDER BY colA, colC;"
        )
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_exportViewForArgs_detectRowDeletionView_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = attr.evolve(
                self.to_entity(metadata), export_time=_DATE_5
            )

            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, is_detect_row_deletion_view=True
            )
        with freeze_time(_DATE_5.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_exportViewForArgs_detectRowDeletionView(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name="normalized_file_name",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
                ingest_database_name=self.ingest_database_name,
            )
            expected_metadata = attr.evolve(
                self.to_entity(metadata), export_time=_DATE_5
            )
            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, is_detect_row_deletion_view=True
            )
        with freeze_time(_DATE_5.isoformat()):
            export_manager.export_view_for_args(export_args)

        expected_upper_bound_query = _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT
        expected_lower_bound_query = expected_upper_bound_query.replace(
            "2020_07_20_00_00_00_upper_bound", "2019_07_20_00_00_00_lower_bound"
        )

        # Assert
        self.mock_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str=expected_upper_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_2)],
                ),
                mock.call(
                    query_str=expected_lower_bound_query,
                    query_parameters=[self.generate_query_params_for_date(_DATE_1)],
                ),
            ]
        )
        # Lower bound is the first part of the subquery, not upper bound.
        expected_query = (
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2019_07_20_00_00_00_lower_bound`) "
            "EXCEPT DISTINCT "
            "(SELECT * FROM `recidiviz-456.us_xx_ingest_views_20220414_secondary.ingest_view_2020_07_20_00_00_00_upper_bound`)"
            "ORDER BY colA, colC;"
        )
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id="us_xx_ingest_views_20220414_secondary",
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(assert_session.query(schema.DirectIngestIngestFileMetadata).all())
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_debugQueryForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(region)
        with freeze_time(_DATE_5.isoformat()):
            debug_query = DirectIngestIngestViewExportManager.debug_query_for_args(
                export_manager.ingest_views_by_tag, export_args
            )

        expected_debug_query = """CREATE TEMP TABLE ingest_view_2020_07_20_00_00_00_upper_bound AS (

WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= DATETIME(2020, 7, 20, 0, 0, 0)
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= DATETIME(2020, 7, 20, 0, 0, 0)
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);
CREATE TEMP TABLE ingest_view_2019_07_20_00_00_00_lower_bound AS (

WITH
file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= DATETIME(2019, 7, 20, 0, 0, 0)
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= DATETIME(2019, 7, 20, 0, 0, 0)
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
)
select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);
(
SELECT * FROM ingest_view_2020_07_20_00_00_00_upper_bound
) EXCEPT DISTINCT (
SELECT * FROM ingest_view_2019_07_20_00_00_00_lower_bound
)
ORDER BY colA, colC;"""

        # Assert
        self.assertEqual(expected_debug_query, debug_query)

    def test_debugQueryForArgsMaterializedRawTables(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2,
        )

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, materialize_raw_data_table_views=True
            )
        with freeze_time(_DATE_5.isoformat()):
            debug_query = DirectIngestIngestViewExportManager.debug_query_for_args(
                export_manager.ingest_views_by_tag, export_args
            )

        expected_debug_query = """CREATE TEMP TABLE upper_file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= DATETIME(2020, 7, 20, 0, 0, 0)
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE upper_tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= DATETIME(2020, 7, 20, 0, 0, 0)
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE ingest_view_2020_07_20_00_00_00_upper_bound AS (

select * from upper_file_tag_first_generated_view JOIN upper_tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);
CREATE TEMP TABLE lower_file_tag_first_generated_view AS (
    WITH normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.file_tag_first`
        WHERE
            update_datetime <= DATETIME(2019, 7, 20, 0, 0, 0)
    ),
    rows_with_recency_rank AS (
        SELECT
            col_name_1a, col_name_1b,
            ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )

    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE lower_tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime <= DATETIME(2019, 7, 20, 0, 0, 0)
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    normalized_rows AS (
        SELECT
            *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    ),
    rows_with_recency_rank AS (
        SELECT
            COL_1,
            ROW_NUMBER() OVER (PARTITION BY COL_1
                               ORDER BY update_datetime DESC) AS recency_rank
        FROM
            normalized_rows
    )
    SELECT *
    EXCEPT (recency_rank)
    FROM rows_with_recency_rank
    WHERE recency_rank = 1
);
CREATE TEMP TABLE ingest_view_2019_07_20_00_00_00_lower_bound AS (

select * from lower_file_tag_first_generated_view JOIN lower_tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);
(
SELECT * FROM ingest_view_2020_07_20_00_00_00_upper_bound
) EXCEPT DISTINCT (
SELECT * FROM ingest_view_2019_07_20_00_00_00_lower_bound
)
ORDER BY colA, colC;"""

        # Assert
        self.assertEqual(expected_debug_query, debug_query)

        self.mock_client.create_dataset_if_necessary.assert_not_called()

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
            "CHECK constraint failed: datetimes_contained_ordering",
        ):
            self.run_get_args_test(
                ingest_view_name=ingest_view_name,
                committed_raw_file_metadata=raw_file_datetimes,
                committed_ingest_file_metadata=ingest_file_metadata,
            )

    def run_get_args_test(
        self,
        ingest_view_name: str,
        committed_raw_file_metadata: List[_RawFileMetadata],
        committed_ingest_file_metadata: List[_IngestFileMetadata],
    ) -> List[GcsfsIngestViewExportArgs]:
        """Runs test to generate ingest view export args given provided DB state."""
        region = self.create_fake_region()

        with SessionFactory.using_database(self.database_key) as session:
            for i, ingest_file_datetimes in enumerate(committed_ingest_file_metadata):
                metadata = schema.DirectIngestIngestFileMetadata(
                    file_id=i,
                    region_code=region.region_code.upper(),
                    ingest_database_name=self.ingest_database_name,
                    file_tag=ingest_view_name,
                    normalized_file_name=f"{ingest_view_name}_{i}",
                    is_invalidated=False,
                    is_file_split=False,
                    datetimes_contained_lower_bound_exclusive=ingest_file_datetimes.datetimes_contained_lower_bound_exclusive,
                    datetimes_contained_upper_bound_inclusive=ingest_file_datetimes.datetimes_contained_upper_bound_inclusive,
                    job_creation_time=ingest_file_datetimes.job_creation_time,
                    export_time=ingest_file_datetimes.export_time,
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
            export_manager = self.create_export_manager(region)
        with freeze_time(_DATE_5.isoformat()):
            args = export_manager.get_ingest_view_export_task_args()

        return args
