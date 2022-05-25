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
from unittest.mock import create_autospec

import mock
import sqlalchemy
from freezegun import freeze_time
from google.cloud.bigquery import ScalarQueryParameter
from mock import Mock, patch
from more_itertools import one

from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContents,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
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
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestViewMaterializationMetadata,
)
from recidiviz.tests.ingest.direct.fakes.fake_single_ingest_view_collector import (
    FakeSingleIngestViewCollector,
)
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.regions import Region
from recidiviz.utils.string import StrictStringFormatter

_ID = 1
_DATE_1 = datetime.datetime(year=2019, month=7, day=20)
_DATE_2 = datetime.datetime(year=2020, month=7, day=20)
_DATE_3 = datetime.datetime(year=2022, month=4, day=13)
_DATE_4 = datetime.datetime(year=2022, month=4, day=14)
_DATE_5 = datetime.datetime(year=2022, month=4, day=15)


_DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT = """DROP TABLE IF EXISTS `recidiviz-456.{temp_dataset}.ingest_view_2020_07_20_00_00_00_upper_bound`;
CREATE TABLE `recidiviz-456.{temp_dataset}.ingest_view_2020_07_20_00_00_00_upper_bound`
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
DROP TABLE IF EXISTS `recidiviz-456.{temp_dataset}.ingest_view_2020_07_20_00_00_00_upper_bound`;
CREATE TABLE `recidiviz-456.{temp_dataset}.ingest_view_2020_07_20_00_00_00_upper_bound`
OPTIONS(
  -- Data in this table will be deleted after 24 hours
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (

select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
ORDER BY colA, colC

);"""

_PROJECT_ID = "recidiviz-456"
_INGEST_INSTANCE = DirectIngestInstance.SECONDARY
_OUTPUT_BUCKET_NAME = gcsfs_direct_ingest_bucket_for_state(
    project_id=_PROJECT_ID,
    region_code="us_xx",
    ingest_instance=_INGEST_INSTANCE,
).bucket_name

_LEGACY_TEMP_DATASET = "us_xx_ingest_views_20220414_secondary"

_BQ_BASED_ARGS = IngestViewMaterializationArgs(
    ingest_view_name="ingest_view",
    ingest_instance=_INGEST_INSTANCE,
    lower_bound_datetime_exclusive=_DATE_1,
    upper_bound_datetime_inclusive=_DATE_2,
)

_TEMP_DATASET = "mock_us_xx_secondary_temp_20220413"


class IngestViewMaterializerTest(unittest.TestCase):
    """Tests for the IngestViewMaterializer class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = _PROJECT_ID
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.ingest_database_name = "ingest_database_name"
        self.ingest_instance = _INGEST_INSTANCE
        self.output_bucket_name = _OUTPUT_BUCKET_NAME
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.client_patcher = patch(
            "recidiviz.big_query.big_query_client.BigQueryClient"
        )
        self.mock_client = self.client_patcher.start().return_value

        project_id_mock = mock.PropertyMock(return_value=self.mock_project_id)
        type(self.mock_client).project_id = project_id_mock

        def fake_get_temp_dataset() -> str:
            date_ts = datetime.datetime.utcnow().strftime("%Y%m%d")
            return f"mock_us_xx_{self.ingest_instance.value.lower()}_temp_{date_ts}"

        self.mock_ingest_view_contents = create_autospec(InstanceIngestViewContents)
        self.mock_ingest_view_contents.temp_results_dataset.side_effect = (
            fake_get_temp_dataset
        )

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

    def create_materializer(
        self,
        region: Region,
        is_detect_row_deletion_view: bool = False,
        materialize_raw_data_table_views: bool = False,
    ) -> IngestViewMaterializerImpl:
        self.ingest_instance = DirectIngestInstance.SECONDARY

        date_ts = datetime.datetime.utcnow().strftime("%Y%m%d")
        self.mock_ingest_view_contents.temp_results_dataset = (
            f"mock_us_xx_{self.ingest_instance.value.lower()}_temp_{date_ts}"
        )

        ingest_view_name = "ingest_view"
        return IngestViewMaterializerImpl(
            region=region,
            ingest_instance=self.ingest_instance,
            metadata_manager=DirectIngestViewMaterializationMetadataManager(
                region_code=region.region_code, ingest_instance=self.ingest_instance
            ),
            ingest_view_contents=self.mock_ingest_view_contents,
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

    def assert_materialized_with_query(
        self, args: IngestViewMaterializationArgs, expected_query: str
    ) -> None:
        self.mock_ingest_view_contents.save_query_results.assert_called_with(
            ingest_view_name=args.ingest_view_name,
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            query_str=expected_query,
            order_by_cols_str="colA, colC",
        )

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_materializeViewForArgs_ingestViewExportsDisabled(self) -> None:
        # Arrange
        region = self.create_fake_region(environment="staging")
        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(region)

        # Act
        with self.assertRaisesRegex(
            ValueError, r"Ingest not enabled for region \[US_XX\]"
        ):
            ingest_view_materializer.materialize_view_for_args(_BQ_BASED_ARGS)

        # Assert
        self.mock_client.create_dataset_if_necessary.assert_not_called()
        self.mock_client.run_query_async.assert_not_called()
        self.mock_ingest_view_contents.save_query_results.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_materializeViewForArgs_noExistingMetadata(self) -> None:
        # Arrange
        region = self.create_fake_region()
        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(region)
        args = _BQ_BASED_ARGS

        # Act
        with self.assertRaisesRegex(
            sqlalchemy.exc.NoResultFound, "No row was found when one was required"
        ):
            ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_materializeViewForArgs_alreadyExported(self) -> None:
        # Arrange
        region = self.create_fake_region()

        args = _BQ_BASED_ARGS

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(region)

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.mark_ingest_view_materialized(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_ingest_view_contents.save_query_results.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code="US_XX",
            instance=DirectIngestInstance.SECONDARY,
            ingest_view_name="ingest_view",
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            job_creation_time=_DATE_3,
            materialization_time=_DATE_4,
            is_invalidated=False,
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(
                    assert_session.query(
                        schema.DirectIngestViewMaterializationMetadata
                    ).all()
                )
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_materializeViewForArgs_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view",
            ingest_instance=_INGEST_INSTANCE,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(region)

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        expected_upper_bound_query = StrictStringFormatter().format(
            _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT,
            temp_dataset=_TEMP_DATASET,
        )

        self.mock_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str=expected_upper_bound_query,
                    query_parameters=[
                        self.generate_query_params_for_date(
                            args.upper_bound_datetime_inclusive
                        )
                    ],
                ),
            ]
        )
        expected_query = (
            "SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2020_07_20_00_00_00_upper_bound`\n"
            "ORDER BY colA, colC;"
        )
        self.assert_materialized_with_query(args, expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                )
            ]
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code="US_XX",
            instance=DirectIngestInstance.SECONDARY,
            ingest_view_name="ingest_view",
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            job_creation_time=_DATE_4,
            materialization_time=_DATE_5,
            is_invalidated=False,
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(
                    assert_session.query(
                        schema.DirectIngestViewMaterializationMetadata
                    ).all()
                )
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_materializeViewForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        args = _BQ_BASED_ARGS

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(region)

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        expected_upper_bound_query = StrictStringFormatter().format(
            _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT,
            temp_dataset=_TEMP_DATASET,
        )
        expected_lower_bound_query = expected_upper_bound_query.replace(
            "2020_07_20_00_00_00_upper_bound", "2019_07_20_00_00_00_lower_bound"
        )

        self.mock_client.dataset_ref_for_id.assert_has_calls(
            [mock.call(_TEMP_DATASET), mock.call(_TEMP_DATASET)]
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
        expected_query = """(
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2020_07_20_00_00_00_upper_bound`
) EXCEPT DISTINCT (
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2019_07_20_00_00_00_lower_bound`
)
ORDER BY colA, colC;"""
        self.assert_materialized_with_query(args, expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code="US_XX",
            instance=DirectIngestInstance.SECONDARY,
            ingest_view_name="ingest_view",
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            job_creation_time=_DATE_4,
            materialization_time=_DATE_5,
            is_invalidated=False,
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(
                    assert_session.query(
                        schema.DirectIngestViewMaterializationMetadata
                    ).all()
                )
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_materializeViewForArgsMaterializedViews(self) -> None:
        # Arrange
        region = self.create_fake_region()

        args = _BQ_BASED_ARGS

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(
                region, materialize_raw_data_table_views=True
            )

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        expected_upper_bound_query = StrictStringFormatter().format(
            _DATE_2_UPPER_BOUND_MATERIALIZED_RAW_TABLE_CREATE_TABLE_SCRIPT,
            temp_dataset=_TEMP_DATASET,
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
        expected_query = """(
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2020_07_20_00_00_00_upper_bound`
) EXCEPT DISTINCT (
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2019_07_20_00_00_00_lower_bound`
)
ORDER BY colA, colC;"""

        self.assert_materialized_with_query(args, expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code="US_XX",
            instance=DirectIngestInstance.SECONDARY,
            ingest_view_name="ingest_view",
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            job_creation_time=_DATE_4,
            materialization_time=_DATE_5,
            is_invalidated=False,
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(
                    assert_session.query(
                        schema.DirectIngestViewMaterializationMetadata
                    ).all()
                )
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_materializeViewForArgs_detectRowDeletionView_noLowerBound(self) -> None:
        # Arrange
        region = self.create_fake_region()
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view",
            ingest_instance=_INGEST_INSTANCE,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(
                region, is_detect_row_deletion_view=True
            )

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            with self.assertRaisesRegex(
                ValueError,
                r"Attempting to process reverse date diff view \[ingest_view\] with "
                r"no lower bound date.",
            ):
                ingest_view_materializer.materialize_view_for_args(args)

        # Assert
        self.mock_client.run_query_async.assert_not_called()
        self.mock_ingest_view_contents.save_query_results.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_materializeViewForArgs_detectRowDeletionView(self) -> None:
        # Arrange
        region = self.create_fake_region()
        args = _BQ_BASED_ARGS

        with freeze_time(_DATE_3.isoformat()):
            ingest_view_materializer = self.create_materializer(
                region,
                is_detect_row_deletion_view=True,
            )

        with freeze_time(_DATE_4.isoformat()):
            ingest_view_materializer.metadata_manager.register_ingest_materialization_job(
                args
            )

        # Act
        with freeze_time(_DATE_5.isoformat()):
            ingest_view_materializer.materialize_view_for_args(args)

        expected_upper_bound_query = StrictStringFormatter().format(
            _DATE_2_UPPER_BOUND_CREATE_TABLE_SCRIPT,
            temp_dataset=_TEMP_DATASET,
        )
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
        expected_query = """(
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2019_07_20_00_00_00_lower_bound`
) EXCEPT DISTINCT (
SELECT * FROM `recidiviz-456.mock_us_xx_secondary_temp_20220413.ingest_view_2020_07_20_00_00_00_upper_bound`
)
ORDER BY colA, colC;"""
        self.assert_materialized_with_query(args, expected_query)
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2020_07_20_00_00_00_upper_bound",
                ),
                mock.call(
                    dataset_id=_TEMP_DATASET,
                    table_id="ingest_view_2019_07_20_00_00_00_lower_bound",
                ),
            ]
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code="US_XX",
            instance=DirectIngestInstance.SECONDARY,
            ingest_view_name="ingest_view",
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            job_creation_time=_DATE_4,
            materialization_time=_DATE_5,
            is_invalidated=False,
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as assert_session:
            found_metadata = self.to_entity(
                one(
                    assert_session.query(
                        schema.DirectIngestViewMaterializationMetadata
                    ).all()
                )
            )
            self.assertEqual(expected_metadata, found_metadata)

    def test_debugQueryForArgs(self) -> None:
        # Arrange
        region = self.create_fake_region()
        export_args = _BQ_BASED_ARGS

        # Act
        ingest_view_materializer = self.create_materializer(region)
        with freeze_time(_DATE_5.isoformat()):
            debug_query = IngestViewMaterializerImpl.debug_query_for_args(
                ingest_view_materializer.ingest_views_by_name, export_args
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
        export_args = _BQ_BASED_ARGS

        # Act
        ingest_view_materializer = self.create_materializer(
            region,
            materialize_raw_data_table_views=True,
        )
        with freeze_time(_DATE_5.isoformat()):
            debug_query = IngestViewMaterializerImpl.debug_query_for_args(
                ingest_view_materializer.ingest_views_by_name, export_args
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
