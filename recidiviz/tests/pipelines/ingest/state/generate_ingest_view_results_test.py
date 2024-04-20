# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Testing the GenerateIngestViewResults PTransform"""
import unittest
from datetime import datetime

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to
from mock import patch

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BQ_EMULATOR_PROJECT_ID,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestGenerateIngestViewResults(StateIngestPipelineTestCase):
    """Tests the GenerateIngestViewResults PTransform."""

    def setUp(self) -> None:
        super().setUp()
        self.read_from_bq_patcher = patch(
            "apache_beam.io.ReadFromBigQuery",
            self.create_fake_bq_read_source_constructor,
        )
        self.read_from_bq_patcher.start()

        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def tearDown(self) -> None:
        super().tearDown()
        self.read_from_bq_patcher.stop()

    def test_materialize_ingest_view_results(self) -> None:
        self.setup_single_ingest_view_raw_data_bq_tables(
            ingest_view_name="ingest12", test_name="ingest12"
        )
        expected_ingest_view_output = self.get_ingest_view_results_from_fixture(
            ingest_view_name="ingest12", test_name="ingest12"
        )

        output = self.test_pipeline | pipeline.GenerateIngestViewResults(
            project_id=BQ_EMULATOR_PROJECT_ID,
            state_code=self.region_code(),
            ingest_view_name="ingest12",
            raw_data_tables_to_upperbound_dates={
                "table1": datetime.fromisoformat(
                    "2022-07-04:00:00:00.000000"
                ).isoformat(),
                "table2": datetime.fromisoformat("2022-07-04:00:00:00").isoformat(),
            },
            ingest_instance=DirectIngestInstance.SECONDARY,
        )
        assert_that(
            output,
            self.validate_ingest_view_results(expected_ingest_view_output, "ingest12"),
        )
        self.test_pipeline.run()

    def test_materialize_ingest_view_results_for_empty_view(self) -> None:
        self.setup_single_ingest_view_raw_data_bq_tables(
            ingest_view_name="ingestCompletelyEmpty", test_name="ingestCompletelyEmpty"
        )
        output = self.test_pipeline | pipeline.GenerateIngestViewResults(
            project_id=BQ_EMULATOR_PROJECT_ID,
            state_code=self.region_code(),
            ingest_view_name="ingestCompletelyEmpty",
            raw_data_tables_to_upperbound_dates={
                "table6": None,
            },
            ingest_instance=DirectIngestInstance.SECONDARY,
        )
        assert_that(output, equal_to([]))
        self.test_pipeline.run()


class TestGenerateIngestViewQuery(unittest.TestCase):
    """Tests for the generate_ingest_view_query() function."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id

        query = "select * from {file_tag_first} JOIN {tagFullHistoricalExport} USING (COL_1)"

        self.ingest_view_query_builder = DirectIngestViewQueryBuilder(
            region="us_xx",
            ingest_view_name="ingest_view",
            view_query_template=query,
            region_module=fake_regions,
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_dataflow_query_for_args(self) -> None:
        dataflow_query = pipeline.GenerateIngestViewResults.generate_ingest_view_query(
            self.ingest_view_query_builder,
            DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=datetime(
                year=2020, month=7, day=20, hour=1, minute=2, second=3, microsecond=4
            ),
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
        dataflow_query = pipeline.GenerateIngestViewResults.generate_ingest_view_query(
            self.ingest_view_query_builder,
            DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=datetime(
                year=2020, month=7, day=20, hour=1, minute=2, second=3, microsecond=4
            ),
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
