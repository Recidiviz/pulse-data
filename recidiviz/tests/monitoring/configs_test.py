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
""" Tests for configuration building / enums"""
import unittest

from recidiviz.monitoring.configs import MonitoringConfig
from recidiviz.tests.utils.monitoring_test_utils import OTLMock


class MonitoringConfigTest(unittest.TestCase):
    """Tests for configuration building / enums"""

    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

    def tearDown(self) -> None:
        self.otl_mock.tear_down()

    def test_config_builds(self) -> None:
        monitoring_config = MonitoringConfig.build()
        self.assertIsNotNone(monitoring_config.instruments)

    def test_all_instruments_created(self) -> None:
        monitoring_config = MonitoringConfig.build()
        for instrument_config in monitoring_config.instruments:
            meter = self.otl_mock.meter_provider.get_meter("test_meter")
            instrument_config.create_instrument(meter)
