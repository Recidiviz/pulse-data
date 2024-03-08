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
import uuid
from typing import Dict

import mock
from freezegun import freeze_time
from mock import patch

from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.ingest.direct.fakes.fake_single_ingest_view_collector import (
    FakeSingleIngestViewCollector,
)
from recidiviz.tests.utils.fake_region import fake_region

_ID = 1
_DATE_1 = datetime.datetime(
    year=2019, month=7, day=20, hour=0, minute=0, second=0, microsecond=123456
)
_DATE_2 = datetime.datetime(
    year=2020, month=7, day=20, hour=1, minute=2, second=3, microsecond=4
)
_DATE_3 = datetime.datetime(year=2022, month=4, day=13)
_DATE_4 = datetime.datetime(year=2022, month=4, day=14)
_DATE_5 = datetime.datetime(year=2022, month=4, day=15)

_PROJECT_ID = "recidiviz-456"
_INGEST_INSTANCE = DirectIngestInstance.SECONDARY

_BQ_BASED_ARGS = IngestViewMaterializationArgs(
    ingest_view_name="ingest_view",
    ingest_instance=_INGEST_INSTANCE,
    upper_bound_datetime_inclusive=_DATE_2,
)


class IngestViewMaterializerTest(unittest.TestCase):
    """Tests for the IngestViewMaterializer class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = _PROJECT_ID
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.ingest_database_name = "ingest_database_name"
        self.ingest_instance = _INGEST_INSTANCE
        self.client_patcher = patch(
            "recidiviz.big_query.big_query_client.BigQueryClient"
        )
        self.mock_client = self.client_patcher.start().return_value

        project_id_mock = mock.PropertyMock(return_value=self.mock_project_id)
        type(self.mock_client).project_id = project_id_mock

        mock_uuid_value = uuid.UUID("abcd1234f8674b04adfb9b595b277dc3")
        self.uuid_patcher = patch("uuid.uuid4")
        mock_uuid = self.uuid_patcher.start()
        mock_uuid.return_value = mock_uuid_value

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()
        self.uuid_patcher.stop()

    @staticmethod
    def create_fake_region(environment: str = "production") -> DirectIngestRegion:
        return fake_region(
            region_code="US_XX",
            environment=environment,
        )

    def create_ingest_views_by_name(
        self, materialize_raw_data_table_views: bool
    ) -> Dict[str, DirectIngestViewQueryBuilder]:
        region = self.create_fake_region()
        view_collector = FakeSingleIngestViewCollector(
            region,
            ingest_view_name="ingest_view",
            materialize_raw_data_table_views=materialize_raw_data_table_views,
        )
        launched_ingest_views = ["ingest_view"]
        return {
            view.ingest_view_name: view
            for view in view_collector.collect_query_builders()
            if view.ingest_view_name in launched_ingest_views
        }

    def test_dataflow_query_for_args(self) -> None:
        export_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view",
            ingest_instance=_INGEST_INSTANCE,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        ingest_views_by_name = self.create_ingest_views_by_name(
            materialize_raw_data_table_views=False
        )
        with freeze_time(_DATE_5.isoformat()):
            dataflow_query = IngestViewMaterializerImpl.dataflow_query_for_args(
                ingest_views_by_name[export_args.ingest_view_name],
                DirectIngestInstance.PRIMARY,
                export_args,
            )
        expected_query = """
WITH view_results AS (
    WITH
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2020-07-20T01:02:03.000004"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE update_datetime <= DATETIME "2020-07-20T01:02:03.000004"
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    filtered_rows AS (
        SELECT *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    )
    SELECT COL_1
    FROM filtered_rows
)
select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
)
SELECT *,
    CURRENT_DATETIME('UTC') AS __materialization_time,
    DATETIME "2020-07-20T01:02:03.000004" AS __upper_bound_datetime_inclusive
FROM view_results;
"""
        self.assertEqual(expected_query, dataflow_query)

    def test_dataflow_query_for_args_handles_materialization_correctly(
        self,
    ) -> None:
        export_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view",
            ingest_instance=_INGEST_INSTANCE,
            upper_bound_datetime_inclusive=_DATE_2,
        )

        ingest_views_by_name = self.create_ingest_views_by_name(
            materialize_raw_data_table_views=True
        )
        with freeze_time(_DATE_5.isoformat()):
            dataflow_query = IngestViewMaterializerImpl.dataflow_query_for_args(
                ingest_views_by_name[export_args.ingest_view_name],
                DirectIngestInstance.PRIMARY,
                export_args,
            )
        expected_query = """
WITH view_results AS (
    WITH
file_tag_first_generated_view AS (
    WITH filtered_rows AS (
        SELECT
            * EXCEPT (recency_rank)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY col_name_1a, col_name_1b
                                   ORDER BY update_datetime DESC) AS recency_rank
            FROM
                `recidiviz-456.us_xx_raw_data.file_tag_first`
            WHERE update_datetime <= DATETIME "2020-07-20T01:02:03.000004"
        ) a
        WHERE
            recency_rank = 1
            AND is_deleted = False
    )
    SELECT col_name_1a, col_name_1b
    FROM filtered_rows
),
tagFullHistoricalExport_generated_view AS (
    WITH max_update_datetime AS (
        SELECT
            MAX(update_datetime) AS update_datetime
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE update_datetime <= DATETIME "2020-07-20T01:02:03.000004"
    ),
    max_file_id AS (
        SELECT
            MAX(file_id) AS file_id
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            update_datetime = (SELECT update_datetime FROM max_update_datetime)
    ),
    filtered_rows AS (
        SELECT *
        FROM
            `recidiviz-456.us_xx_raw_data.tagFullHistoricalExport`
        WHERE
            file_id = (SELECT file_id FROM max_file_id)
    )
    SELECT COL_1
    FROM filtered_rows
)
select * from file_tag_first_generated_view JOIN tagFullHistoricalExport_generated_view USING (COL_1)
)
SELECT *,
    CURRENT_DATETIME('UTC') AS __materialization_time,
    DATETIME "2020-07-20T01:02:03.000004" AS __upper_bound_datetime_inclusive
FROM view_results;
"""
        self.assertEqual(expected_query, dataflow_query)
