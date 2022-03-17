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
"""Tests for ingest_view_materializer.py."""
import datetime
import unittest

import attr
import mock
from freezegun import freeze_time
from google.cloud.bigquery import ScalarQueryParameter
from mock import Mock, patch
from more_itertools import one

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.ingest_view_materialization.file_based_materializer_delegate import (
    FileBasedMaterializerDelegate,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializer,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestIngestFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity
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


class IngestViewMaterializerTest(unittest.TestCase):
    """Tests for the IngestViewMaterializer class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = "ingest_database_name"
        self.output_bucket_name = gcsfs_direct_ingest_bucket_for_region(
            project_id=self.mock_project_id,
            region_code="us_xx",
            system_level=SystemLevel.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        ).bucket_name
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
    ) -> IngestViewMaterializer:
        metadata_manager = PostgresDirectIngestIngestFileMetadataManager(
            region.region_code, self.ingest_database_name
        )
        ingest_view_name = "ingest_view"
        return IngestViewMaterializer(
            region=region,
            ingest_instance=DirectIngestInstance.SECONDARY,
            delegate=FileBasedMaterializerDelegate(
                ingest_file_metadata_manager=metadata_manager,
                big_query_client=self.mock_client,
            ),
            big_query_client=self.mock_client,
            view_collector=FakeSingleIngestViewCollector(  # type: ignore[arg-type]
                region,
                ingest_view_name="ingest_view",
                is_detect_row_deletion_view=is_detect_row_deletion_view,
                materialize_raw_data_table_views=materialize_raw_data_table_views,
            ),
            launched_ingest_views=[ingest_view_name],
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
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
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        # Act
        with self.assertRaises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_dataset_if_necessary.assert_not_called()
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs_noExistingMetadata(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        # Act
        with self.assertRaises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs_alreadyExported(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=_DATE_2,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
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
                            export_args.upper_bound_datetime_inclusive
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgsMaterializedViews(self) -> None:
        # Arrange
        region = self.create_fake_region()

        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs_detectRowDeletionView_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
                ingest_database_name=self.ingest_database_name,
            )

            session.add(metadata)

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, is_detect_row_deletion_view=True
            )
        with freeze_time(_DATE_5.isoformat()):
            with self.assertRaisesRegex(
                ValueError,
                r"Attempting to process reverse date diff view \[ingest_view\] with "
                r"no lower bound date.",
            ):
                export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_exportViewForArgs_detectRowDeletionView(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with SessionFactory.using_database(self.database_key) as session:
            metadata = schema.DirectIngestIngestFileMetadata(
                file_id=_ID,
                region_code=region.region_code,
                file_tag=export_args.ingest_view_name,
                normalized_file_name=FileBasedMaterializerDelegate.generate_output_path(
                    export_args
                ).file_name,
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=_DATE_1,
                export_time=None,
                datetimes_contained_lower_bound_exclusive=export_args.lower_bound_datetime_exclusive,
                datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_inclusive,
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_debugQueryForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(region)
        with freeze_time(_DATE_5.isoformat()):
            debug_query = IngestViewMaterializer.debug_query_for_args(
                export_manager.ingest_views_by_name, export_args
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

    # TODO(#9717): Write an analogous test that uses a materializer with a new
    #  BQ-based implementation of IngestViewMaterializerDelegate.
    def test_debugQueryForArgsMaterializedRawTables(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name="ingest_view",
            output_bucket_name=self.output_bucket_name,
            lower_bound_datetime_exclusive=_DATE_1,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager = self.create_export_manager(
                region, materialize_raw_data_table_views=True
            )
        with freeze_time(_DATE_5.isoformat()):
            debug_query = IngestViewMaterializer.debug_query_for_args(
                export_manager.ingest_views_by_name, export_args
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
