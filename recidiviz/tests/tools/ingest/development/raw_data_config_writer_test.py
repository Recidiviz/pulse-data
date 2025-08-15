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
"""Tests for RawDataConfigWriter."""
import os
import tempfile
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    ImportBlockingValidationExemption,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tools.docs.utils import PLACEHOLDER_TO_DO_STRING
from recidiviz.tools.ingest.development.raw_data_config_writer import (
    RawDataConfigWriter,
)


class RawDataConfigWriterTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_output_to_file(self) -> None:
        # for state_code in [StateCode.US_XX, StateCode.US_LL]:
        # self._run_test_for_region(state_code)
        self._run_test_for_region(state_code=StateCode.US_XX)

    def _run_test_for_region(self, state_code: StateCode) -> None:
        """Tests that for a given state the generator produces identical raw data YAMLs."""
        region_config = DirectIngestRegionRawFileConfig(
            region_code=state_code.value.lower(),
            region_module=fake_regions_module,
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            # pylint: disable=protected-access
            # We read the configs directly from disk so they will not be normalized
            # in the region constructor (i.e. have reciprocal table relationships set
            # on the config for the other table in the relationship).
            for file_tag, config in region_config._read_configs_from_disk().items():
                with open(config.file_path, "r", encoding="utf-8") as f:
                    expected_contents = f.read()

                # Support cardinality being explicitly set or not (generation will not
                # set it)
                expected_contents = expected_contents.replace(
                    "    cardinality: MANY_TO_MANY\n", ""
                )

                test_output_path = os.path.join(tmpdirname, f"{file_tag}.yaml")
                config_writer = RawDataConfigWriter()
                config_writer.output_to_file(
                    default_encoding="UTF-8",
                    default_separator=",",
                    default_update_cadence=RawDataFileUpdateCadence.WEEKLY,
                    default_ignore_quotes=False,
                    default_export_lookback_window=RawDataExportLookbackWindow.TWO_WEEK_INCREMENTAL_LOOKBACK,
                    default_no_valid_primary_keys=False,
                    output_path=test_output_path,
                    raw_file_config=config,
                    default_custom_line_terminator="‡\n",
                    default_infer_columns_from_config=False,
                    default_import_blocking_validation_exemptions=[
                        ImportBlockingValidationExemption(
                            validation_type=RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS,
                            exemption_reason="reason",
                        )
                    ],
                )
                with open(test_output_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                with open(test_output_path, "w", encoding="utf-8") as f:
                    for line in lines:
                        if PLACEHOLDER_TO_DO_STRING not in line:
                            f.write(line)

                with open(test_output_path, "r", encoding="utf-8") as f:
                    written_contents = f.read()

                self.assertEqual(expected_contents, written_contents)
