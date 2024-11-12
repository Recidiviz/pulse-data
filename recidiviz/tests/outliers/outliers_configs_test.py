# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for outliers_configs.py"""
import unittest

from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.utils.types import assert_type


class TestOutliersConfigs(unittest.TestCase):
    """Tests for outliers_configs.py"""

    def test_get_outliers_backend_config(self) -> None:
        for state_code_str in get_outliers_enabled_states_for_bigquery():
            # There should be a valid backend config defined
            backend_config = get_outliers_backend_config(state_code_str)

            for metric_config in backend_config.metrics:
                state_code = assert_type(metric_config.state_code, StateCode)
                self.assertEqual(
                    state_code_str,
                    state_code.value,
                    f"Found metric in backend config for [{state_code_str}] with "
                    f"state_code that does not match: "
                    f"{state_code.value}.",
                )

        # Test US_ID special case
        us_id_backend_config = get_outliers_backend_config(StateCode.US_ID.value)

        for metric_config in us_id_backend_config.metrics:
            # The state_code value on the metrics corresponds to the state_code value in
            # the *data*.
            self.assertEqual(
                StateCode.US_IX, assert_type(metric_config.state_code, StateCode)
            )
