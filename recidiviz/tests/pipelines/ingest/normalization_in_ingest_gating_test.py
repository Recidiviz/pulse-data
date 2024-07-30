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
"""Tests for normalization_in_ingest_gating.py."""
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.normalization_in_ingest_gating import (
    is_combined_ingest_and_normalization_launched_in_env,
    should_run_normalization_in_ingest,
)
from recidiviz.utils.environment import GCPEnvironment


class TestNormalizationInIngestGating(unittest.TestCase):
    """Tests for normalization_in_ingest_gating.py."""

    def test_staging_gating(self) -> None:
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.STAGING.value,
        ):
            for state_code in StateCode:
                writes_enabled = should_run_normalization_in_ingest(state_code)
                reads_enabled = is_combined_ingest_and_normalization_launched_in_env(
                    state_code
                )

                if writes_enabled or not reads_enabled:
                    continue

                raise ValueError(
                    f"Cannot set is_combined_ingest_and_normalization_launched_in_env() "
                    f"in staging for state without also enabling "
                    f"should_run_normalization_in_ingest(): {state_code.value}"
                )

    def test_prod_gating(self) -> None:
        with patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        ):
            for state_code in StateCode:
                writes_enabled = should_run_normalization_in_ingest(state_code)
                reads_enabled = is_combined_ingest_and_normalization_launched_in_env(
                    state_code
                )

                if writes_enabled or not reads_enabled:
                    continue

                raise ValueError(
                    f"Cannot set is_combined_ingest_and_normalization_launched_in_env() "
                    f"in staging for state without also enabling "
                    f"should_run_normalization_in_ingest(): {state_code.value}"
                )
