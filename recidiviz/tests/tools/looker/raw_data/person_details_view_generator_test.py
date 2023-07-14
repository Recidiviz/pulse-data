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
"""Unit tests for person details LookML View generation"""

import filecmp
import os
import tempfile
import unittest

from freezegun import freeze_time
from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    generate_lookml_views,
)


class LookMLViewTest(unittest.TestCase):
    """Tests LookML view generation functions"""

    @freeze_time("2000-06-30")
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_view_generator.get_existing_direct_ingest_states"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_view_generator.DirectIngestRegionRawFileConfig"
    )
    def test_generate_lookml_views(
        self, mock_region_config: Mock, mock_get_states: Mock
    ) -> None:
        # Only generate views corresponding to US_LL raw data files
        def mock_config_constructor(
            *, region_code: str
        ) -> DirectIngestRegionRawFileConfig:
            return DirectIngestRegionRawFileConfig(
                region_code=region_code,
                region_module=fake_regions,
            )

        mock_region_config.side_effect = mock_config_constructor
        mock_get_states.return_value = [StateCode.US_LL]

        with tempfile.TemporaryDirectory() as tmp_dir:
            generate_lookml_views(tmp_dir)
            fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")

            for fixtures_path, _, filenames in os.walk(fixtures_dir):
                # Get the fixtures inner directory corresponding to the temp inner directory
                relpath = os.path.relpath(fixtures_path, start=fixtures_dir)
                tmp_path = os.path.join(tmp_dir, relpath)

                # Ensure every .lkml file in the fixture directory is equal
                # byte-by-byte to the one in the temp directory
                lkml_filenames = filter(lambda name: name.endswith(".lkml"), filenames)
                _, mismatch, errors = filecmp.cmpfiles(
                    tmp_path, fixtures_path, lkml_filenames, shallow=False
                )
                self.assertFalse(mismatch)
                self.assertFalse(errors)
