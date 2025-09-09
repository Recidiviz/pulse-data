# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for raw_data_import_block_validation_collector.py"""
import datetime
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_config_enums import (
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_collector import (
    RawDataImportBlockingValidationCollector,
)


class TestRawDataImportBlockingValidationCollector(unittest.TestCase):
    """Unit tests for RawDataImportBlockingValidationCollector"""

    def test_collect_validations(self) -> None:
        #  Test that we can collect validations for all RawDataImportBlockingValidationTypes
        result = RawDataImportBlockingValidationCollector.collect_validations_for_file(
            state_code=StateCode.US_XX,
            file_tag="test_file",
            project_id="test_project",
            temp_table_address=BigQueryAddress(
                dataset_id="test_dataset", table_id="test_table"
            ),
            raw_file_config=DirectIngestRawFileConfig(
                state_code=StateCode.US_XX,
                file_tag="test_file",
                file_path="/path/to/myFile.yaml",
                file_description="This is a raw data file",
                data_classification=RawDataClassification.SOURCE,
                columns=[],
                custom_line_terminator=None,
                primary_key_cols=[],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                ignore_quotes=True,
                export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
                no_valid_primary_keys=False,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
            ),
            raw_data_instance=DirectIngestInstance.PRIMARY,
            file_update_datetime=datetime.datetime.now(),
        )
        self.assertIsInstance(result, list)
