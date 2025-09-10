#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Unit tests for ruamel yaml writer"""

import difflib
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import attr

from recidiviz.common.constants.encoding import UTF_8
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_data_yaml_writer import (
    update_region_raw_file_yamls,
)
from recidiviz.ingest.direct.raw_data.raw_file_config_enums import (
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


def compare_files(expected_file: str, actual_file: str) -> str:
    if not os.path.exists(expected_file):
        raise RuntimeError(f"Error: Expected file not found: {expected_file}")
    if not os.path.exists(actual_file):
        raise RuntimeError(f"Error: Actual file not found: {actual_file}")

    with open(expected_file, "r", encoding=UTF_8) as f1, open(
        actual_file, "r", encoding=UTF_8
    ) as f2:
        expected_lines = f1.readlines()
        actual_lines = f2.readlines()

        diff = difflib.unified_diff(
            expected_lines,
            actual_lines,
            fromfile=expected_file,
            tofile=actual_file,
        )
        return "".join(diff)


class TestUpdateRegionRawFileConfigYamls(unittest.TestCase):
    """Tests for yaml writer"""

    def setUp(self) -> None:
        self.fixtures_path = Path(
            os.path.join(os.path.dirname(__file__), "raw_data_yaml_writer_fixtures")
        )

        self._tmp_dir = tempfile.mkdtemp()
        self.tmp_output_dir = Path(self._tmp_dir)

        self.state_code = StateCode.US_LL
        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code.value, region_module=fake_regions_module
        )

    def tearDown(self) -> None:
        if self._tmp_dir and os.path.exists(self._tmp_dir):
            shutil.rmtree(self._tmp_dir)
            self._tmp_dir = ""

    def _get_file_name(self, file_tag: str) -> str:
        return f"{self.state_code.value.lower()}_{file_tag}.yaml"

    def _compare_raw_file_and_fixture(self, file_name: str) -> str:
        expected_file = str(self.fixtures_path / file_name)
        actual_file = str(self.tmp_output_dir / file_name)
        return compare_files(expected_file, actual_file)

    def test_basic_update(self) -> None:
        file_tag_to_update = "basicData"
        file_name = self._get_file_name(file_tag_to_update)

        def updater_fn(
            region_config: DirectIngestRegionRawFileConfig,
        ) -> DirectIngestRegionRawFileConfig:
            region_config.raw_file_configs[file_tag_to_update] = attr.evolve(
                region_config.raw_file_configs[file_tag_to_update],
                file_description="Updated description for basic data file.",
                file_path=str(self.tmp_output_dir / file_name),
            )

            return region_config

        update_region_raw_file_yamls(self.region_raw_file_config, updater_fn)

        diff = self._compare_raw_file_and_fixture(file_name)
        self.assertFalse(diff, f"File mismatch for {file_name}:\n{diff}")

    def test_add_new_file_tag(self) -> None:
        file_tag_to_add = "newDataSource"
        file_name = self._get_file_name(file_tag_to_add)

        def updater_fn(
            region_config: DirectIngestRegionRawFileConfig,
        ) -> DirectIngestRegionRawFileConfig:
            # TODO(#46293): fill in some list fields
            new_file_config = DirectIngestRawFileConfig(
                file_tag=file_tag_to_add,
                file_description="New data source file.",
                file_path=str(self.tmp_output_dir / file_name),
                state_code=StateCode.US_LL,
                data_classification=RawDataClassification.SOURCE,
                columns=[],
                custom_line_terminator=None,
                primary_key_cols=[],
                supplemental_order_by_clause="",
                encoding="UTF-8",
                separator=",",
                ignore_quotes=False,
                export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
                no_valid_primary_keys=False,
                infer_columns_from_config=False,
                table_relationships=[],
                update_cadence=RawDataFileUpdateCadence.WEEKLY,
                is_code_file=False,
            )
            region_config.raw_file_configs[file_tag_to_add] = new_file_config
            return region_config

        update_region_raw_file_yamls(self.region_raw_file_config, updater_fn)

        diff = self._compare_raw_file_and_fixture(file_name)
        self.assertFalse(diff, f"File mismatch for {file_name}:\n{diff}")

    def test_remove_file_tag(self) -> None:
        file_tag_to_remove = "columnsMissing"
        file_name = self._get_file_name(file_tag_to_remove)
        new_file_path = self.fixtures_path / file_name

        raw_file_config = self.region_raw_file_config.raw_file_configs[
            file_tag_to_remove
        ]

        shutil.copy(
            raw_file_config.file_path,
            new_file_path,
        )

        self.region_raw_file_config.raw_file_configs[file_tag_to_remove] = attr.evolve(
            raw_file_config,
            file_path=str(new_file_path),
        )

        def updater_fn(
            region_config: DirectIngestRegionRawFileConfig,
        ) -> DirectIngestRegionRawFileConfig:
            configs = region_config.raw_file_configs
            configs.pop(file_tag_to_remove, None)
            region_config.raw_file_configs = configs
            return region_config

        update_region_raw_file_yamls(self.region_raw_file_config, updater_fn)

        self.assertFalse(os.path.exists(new_file_path))

    def test_column_update(self) -> None:
        file_tag_to_update = "noValidPrimaryKeys"
        file_name = self._get_file_name(file_tag_to_update)

        def updater_fn(
            region_config: DirectIngestRegionRawFileConfig,
        ) -> DirectIngestRegionRawFileConfig:
            raw_file_config = region_config.raw_file_configs[file_tag_to_update]
            for column in raw_file_config.current_columns:
                if column.known_values:
                    column.known_values.append(
                        ColumnEnumValueInfo(value="C", description="A new value")
                    )
            region_config.raw_file_configs[file_tag_to_update] = attr.evolve(
                raw_file_config,
                file_path=str(self.tmp_output_dir / file_name),
            )

            return region_config

        update_region_raw_file_yamls(self.region_raw_file_config, updater_fn)

        diff = self._compare_raw_file_and_fixture(file_name)
        self.assertFalse(diff, f"File mismatch for {file_name}:\n{diff}")

    # TODO(#46293): Add more unit tests
