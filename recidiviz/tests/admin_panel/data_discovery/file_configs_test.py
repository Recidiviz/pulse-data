# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Tests for data discovery"""
import csv
from unittest import TestCase
from unittest.mock import patch

from recidiviz.admin_panel.data_discovery.file_configs import (
    get_ingest_view_configs,
    get_raw_data_configs,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
    get_existing_direct_ingest_states,
)


class TestDataDiscoveryFileConfigs(TestCase):
    """TestCase for data discovery file configs"""

    def setUp(self) -> None:
        self.get_data_discovery_cache_patcher = patch("redis.Redis")

    def tearDown(self) -> None:
        self.get_data_discovery_cache_patcher.stop()

    def test_get_raw_data_configs(self) -> None:
        raw_data_configs = get_raw_data_configs("us_id")
        self.assertIsNotNone(raw_data_configs)

        mittimus = next(
            raw_data_config
            for raw_data_config in raw_data_configs
            if raw_data_config.file_tag == "mittimus"
        )

        self.assertIn("mitt_srl", mittimus.columns)
        self.assertIn("mitt_srl", mittimus.primary_keys)
        self.assertEqual("|", mittimus.separator)
        self.assertEqual("ISO-8859-1", mittimus.encoding)
        self.assertEqual(csv.QUOTE_MINIMAL, mittimus.quoting)

        for region_code in get_existing_region_dir_names():
            self.assertIsNotNone(get_raw_data_configs(region_code))

    def test_get_ingest_view_configs(self) -> None:
        ingest_view_configs = get_ingest_view_configs("us_id")

        early_discharge_incarceration_sentence = next(
            ingest_view_config
            for ingest_view_config in ingest_view_configs
            if ingest_view_config.file_tag == "early_discharge_incarceration_sentence"
        )

        self.assertIsNotNone(early_discharge_incarceration_sentence)
        self.assertIn("mitt_srl", early_discharge_incarceration_sentence.columns)

        for region_code in get_existing_direct_ingest_states():
            self.assertIsNotNone(get_ingest_view_configs(region_code.value))
