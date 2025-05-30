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
from types import ModuleType
from typing import Optional

import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to
from mock import patch
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import read_ingest_view_results_fixture
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    DEFAULT_UPDATE_DATETIME,
)
from recidiviz.tests.pipelines.ingest.state.pipeline_test_case import (
    StateIngestPipelineTestCase,
)


class TestGenerateIngestViewResults(StateIngestPipelineTestCase):
    """Tests the GenerateIngestViewResults PTransform."""

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return fake_regions

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
        self._clear_emulator_table_data()
        self.read_from_bq_patcher.stop()

    def setup_single_ingest_view_raw_data_bq_tables(
        self, ingest_view_name: str, test_name: str
    ) -> DirectIngestViewQueryBuilder:
        ingest_view_builder = (
            self.ingest_view_collector().get_query_builder_by_view_name(
                ingest_view_name
            )
        )
        self.raw_fixture_loader.load_raw_fixtures_to_emulator(
            [ingest_view_builder],
            ingest_test_identifier=test_name,
            create_tables=False,
        )
        return ingest_view_builder

    def test_empty_raw_data_upper_bounds_yell_at_us(self) -> None:
        view_builder = self.setup_single_ingest_view_raw_data_bq_tables(
            ingest_view_name="ingest12", test_name="ingest12"
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Ingest View \[ingest12\] has raw data dependencies with missing upper bound datetimes: table1",
        ):
            GenerateIngestViewResults(
                ingest_view_builder=view_builder,
                raw_data_tables_to_upperbound_dates={
                    # MyPy doesn't like this, but we want to double check
                    "table1": None,  # type: ignore
                    "table2": datetime.fromisoformat("2022-07-04:00:00:00").isoformat(),
                },
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                resource_labels={"for": "testing"},
            )

    def test_materialize_ingest_view_results(self) -> None:
        view_builder = self.setup_single_ingest_view_raw_data_bq_tables(
            ingest_view_name="ingest12", test_name="ingest12"
        )
        df = read_ingest_view_results_fixture(
            self.state_code(), "ingest12", "for_generate_ingest_view_results_test.csv"
        )
        now_ = one(set(df[MATERIALIZATION_TIME_COL_NAME].to_list()))
        expected_output = df.to_dict("records")

        output = (
            self.test_pipeline
            | GenerateIngestViewResults(
                ingest_view_builder=view_builder,
                raw_data_tables_to_upperbound_dates={
                    "table1": DEFAULT_UPDATE_DATETIME.isoformat(),
                    "table2": DEFAULT_UPDATE_DATETIME.isoformat(),
                },
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                resource_labels={"for": "testing"},
            )
            # The emulator's 'now' call is just slightly different than when
            # we generate the data, so we overrite that here.
            | apache_beam.Map(
                lambda row: {
                    k: now_ if k == MATERIALIZATION_TIME_COL_NAME else v
                    for k, v in row.items()
                }
            )
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()


class TestGenerateIngestViewQuery(unittest.TestCase):
    """Tests for the generate_ingest_view_query() function."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_fn.return_value = self.mock_project_id
        self.maxDiff = None

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
        dataflow_query = GenerateIngestViewResults.generate_ingest_view_query(
            self.ingest_view_query_builder,
            DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=datetime(
                year=2020, month=7, day=20, hour=1, minute=2, second=3, microsecond=4
            ),
        )
        expected_query = """
WITH view_results AS (
    WITH
-- Pulls the latest data from file_tag_first received on or before 2020-07-20 01:02:03.000004
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
-- Pulls the latest data from tagFullHistoricalExport received on or before 2020-07-20 01:02:03.000004
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
        dataflow_query = GenerateIngestViewResults.generate_ingest_view_query(
            self.ingest_view_query_builder,
            DirectIngestInstance.PRIMARY,
            upper_bound_datetime_inclusive=datetime(
                year=2020, month=7, day=20, hour=1, minute=2, second=3, microsecond=4
            ),
        )
        expected_query = """
WITH view_results AS (
    WITH
-- Pulls the latest data from file_tag_first received on or before 2020-07-20 01:02:03.000004
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
-- Pulls the latest data from tagFullHistoricalExport received on or before 2020-07-20 01:02:03.000004
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
