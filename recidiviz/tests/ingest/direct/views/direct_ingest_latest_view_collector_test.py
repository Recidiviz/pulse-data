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
"""Tests for classes in direct_ingest_latest_vie_collector.py"""
import unittest
from unittest.mock import patch

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
    DirectIngestRawDataTableLatestViewCollector,
)
from recidiviz.tests.ingest.direct import fake_regions

NON_HISTORICAL_LATEST_VIEW_QUERY = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY col1, col2
                               ORDER BY update_datetime DESC, CAST(seq_num AS INT64)) AS recency_rank
        FROM
            `recidiviz-456.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, col2
FROM filtered_rows
"""

NON_HISTORICAL_LATEST_VIEW_QUERY_WITH_UNDOCUMENTED = """
WITH filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY col1, col2
                               ORDER BY update_datetime DESC, CAST(seq_num AS INT64)) AS recency_rank
        FROM
            `recidiviz-456.us_xx_raw_data.table_name`
        
    ) a
    WHERE
        recency_rank = 1
        AND is_deleted = False
)
SELECT col1, col2, undocumented_col
FROM filtered_rows
"""


HISTORICAL_LATEST_VIEW_QUERY = """
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
filtered_rows AS (
    SELECT *
    FROM
        `recidiviz-456.us_xx_raw_data.table_name`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT col1, col2
FROM filtered_rows
"""


class DirectIngestRawDataTableLatestViewBuilderTest(unittest.TestCase):
    """Tests DirectIngestRawDataTableLatestViewBuilder"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.project_id
        self.raw_file_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="table_name",
            file_path="path/to/file.yaml",
            file_description="file description",
            data_classification=RawDataClassification.SOURCE,
            primary_key_cols=["col1", "col2"],
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                ),
                RawTableColumnInfo(
                    name="col2",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col2 description",
                ),
                RawTableColumnInfo(
                    name="undocumented_col",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                ),
            ],
            supplemental_order_by_clause="CAST(seq_num AS INT64)",
            encoding="any-encoding",
            separator="@",
            custom_line_terminator=None,
            ignore_quotes=False,
            always_historical_export=False,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_build_latest_view(self) -> None:
        view = DirectIngestRawDataTableLatestViewBuilder(
            region_code="us_xx",
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_file_config=self.raw_file_config,
            regions_module=fake_regions,
            filter_to_only_documented_columns=True,
        ).build(sandbox_context=None)

        self.assertEqual(self.project_id, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_latest", view.table_id)
        self.assertEqual("table_name_latest", view.view_id)

        self.assertEqual(NON_HISTORICAL_LATEST_VIEW_QUERY, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`",
            view.select_query,
        )
        self.assertTrue(view.should_deploy())

    def test_build_latest_view_with_undocumented(self) -> None:
        view = DirectIngestRawDataTableLatestViewBuilder(
            region_code="us_xx",
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_file_config=self.raw_file_config,
            regions_module=fake_regions,
            # INCLUDE ALL COLUMNS, INCLUDING UNDOCUMENTED ONES
            filter_to_only_documented_columns=False,
        ).build(sandbox_context=None)

        self.assertEqual(self.project_id, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_latest", view.table_id)
        self.assertEqual("table_name_latest", view.view_id)

        self.assertEqual(
            NON_HISTORICAL_LATEST_VIEW_QUERY_WITH_UNDOCUMENTED, view.view_query
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`",
            view.select_query,
        )
        self.assertTrue(view.should_deploy())

    def test_build_historical_file_latest_view(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config, always_historical_export=True
        )
        view = DirectIngestRawDataTableLatestViewBuilder(
            region_code="us_xx",
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_file_config=raw_file_config,
            regions_module=fake_regions,
            filter_to_only_documented_columns=True,
        ).build(sandbox_context=None)

        self.assertEqual(self.project_id, view.project)
        self.assertEqual("us_xx_raw_data_up_to_date_views", view.dataset_id)
        self.assertEqual("table_name_latest", view.table_id)
        self.assertEqual("table_name_latest", view.view_id)

        self.assertEqual(HISTORICAL_LATEST_VIEW_QUERY, view.view_query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-456.us_xx_raw_data_up_to_date_views.table_name_latest`",
            view.select_query,
        )
        self.assertTrue(view.should_deploy())

    def test_build_no_primary_keys_no_throw(self) -> None:
        for no_valid_primary_keys in (True, False):
            raw_file_config = attr.evolve(
                self.raw_file_config,
                # primary_key_cols=[] is allowed for any value of no_valid_primary_keys
                primary_key_cols=[],
                no_valid_primary_keys=no_valid_primary_keys,
            )
            if no_valid_primary_keys:
                # If no_valid_primary_keys is explicitly set to True, then this config
                # does count as documented
                self.assertFalse(raw_file_config.is_undocumented)
            else:
                self.assertTrue(raw_file_config.is_undocumented)

            _ = DirectIngestRawDataTableLatestViewBuilder(
                region_code="us_xx",
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_file_config=raw_file_config,
                regions_module=fake_regions,
                filter_to_only_documented_columns=True,
            ).build(sandbox_context=None)

    def test_build_primary_keys_nonempty_no_throw(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            primary_key_cols=["col1"],
            no_valid_primary_keys=False,
        )

        _ = DirectIngestRawDataTableLatestViewBuilder(
            region_code="us_xx",
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_file_config=raw_file_config,
            regions_module=fake_regions,
            filter_to_only_documented_columns=True,
        ).build(sandbox_context=None)

    def test_build_primary_keys_throw(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Incorrect primary key setup found for file_tag=table_name: `no_valid_primary_keys`=True and "
            r"`primary_key_cols` is not empty: \['primary_key'\]",
        ):
            _ = attr.evolve(
                self.raw_file_config,
                primary_key_cols=["primary_key"],
                no_valid_primary_keys=True,
            )

    def test_build_no_documented_columns_throws(self) -> None:
        # Columns with no documentation
        raw_file_config = attr.evolve(
            self.raw_file_config,
            always_historical_export=True,
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                ),
                RawTableColumnInfo(
                    name="col2",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                ),
            ],
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found no available \(documented\) columns for file \[table_name\]",
        ):
            _ = DirectIngestRawDataTableLatestViewBuilder(
                region_code="us_xx",
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_file_config=raw_file_config,
                regions_module=fake_regions,
                filter_to_only_documented_columns=True,
            ).build(sandbox_context=None)

    def test_build_no_columns_throws(self) -> None:
        # Config with no columns
        raw_file_config = attr.evolve(
            self.raw_file_config,
            always_historical_export=True,
            columns=[],
            primary_key_cols=[],
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found no available \(documented\) columns for file \[table_name\]",
        ):
            _ = DirectIngestRawDataTableLatestViewBuilder(
                region_code="us_xx",
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_file_config=raw_file_config,
                regions_module=fake_regions,
                filter_to_only_documented_columns=True,
            ).build(sandbox_context=None)


class DirectIngestRawDataTableLatestViewCollectorTest(unittest.TestCase):
    """Tests DirectIngestRawDataTableLatestViewCollector"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.project_id

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_collect_latest_view_builders(self) -> None:
        collector = DirectIngestRawDataTableLatestViewCollector(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
            regions_module=fake_regions,
            filter_to_documented=True,
        )

        builders = collector.collect_view_builders()
        self.assertCountEqual(
            [
                "file_tag_first_latest",
                "file_tag_second_latest",
                "multipleColPrimaryKeyHistorical_latest",
                "singlePrimaryKey_latest",
                "tagBasicData_latest",
                "tagCustomLineTerminatorNonUTF8_latest",
                "tagDoubleDaggerWINDOWS1252_latest",
                "tagFullHistoricalExport_latest",
                "tagInvalidCharacters_latest",
                "tagMoreBasicData_latest",
                "tagNormalizationConflict_latest",
                "tagPipeSeparatedNonUTF8_latest",
                "tagColumnRenamed_latest",
            ],
            [b.view_id for b in builders],
        )

    def test_collect_latest_view_builders_include_undocumented(self) -> None:
        collector = DirectIngestRawDataTableLatestViewCollector(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
            regions_module=fake_regions,
            filter_to_documented=False,
        )

        builders = collector.collect_view_builders()
        self.assertCountEqual(
            [
                "file_tag_first_latest",
                "file_tag_second_latest",
                "multipleColPrimaryKeyHistorical_latest",
                "singlePrimaryKey_latest",
                "tagBasicData_latest",
                "tagCustomLineTerminatorNonUTF8_latest",
                "tagDoubleDaggerWINDOWS1252_latest",
                "tagFullHistoricalExport_latest",
                "tagInvalidCharacters_latest",
                "tagMoreBasicData_latest",
                "tagNormalizationConflict_latest",
                "tagPipeSeparatedNonUTF8_latest",
                "tagColumnRenamed_latest",
                "tagColCapsDoNotMatchConfig_latest",
                "tagRowMissingColumns_latest",
                "tagInvalidFileConfigHeaders_latest",
                "tagRowExtraColumns_latest",
                "tagMissingColumnsDefined_latest",
                "tagFileConfigHeaders_latest",
                "tagOneAllNullRow_latest",
                "tagChunkedFileTwo_latest",
                "tagColumnMissingInRawData_latest",
                "tagChunkedFile_latest",
                "tagOneAllNullRowTwoGoodRows_latest",
                "tagColumnsMissing_latest",
                "tagFileConfigHeadersUnexpectedHeader_latest",
                "tagFileConfigCustomDatetimeSql_latest",
            ],
            [b.view_id for b in builders],
        )

    def test_collect_latest_view_builders_all(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            collector = DirectIngestRawDataTableLatestViewCollector(
                state_code.value,
                DirectIngestInstance.PRIMARY,
                filter_to_documented=True,
            )

            # Just make sure we don't crash
            _ = collector.collect_view_builders()
