# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for providers.py lazy MeterProvider initialization."""
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

from opentelemetry.sdk.metrics import MeterProvider

from recidiviz.monitoring.providers import get_global_meter_provider


class DefaultMeterProviderTypeTest(unittest.TestCase):
    """Guards the assumption that OTL's default provider is not an SDK
    MeterProvider. If this fails after an opentelemetry upgrade, the lazy init
    isinstance check in get_global_meter_provider() needs to be updated."""

    def test_default_meter_provider_is_not_sdk_type(self) -> None:
        # We run this check in a subprocess because OpenTelemetry's
        # set_meter_provider() permanently mutates process-global state with no
        # public reset API. In our test suite (which uses pytest-xdist), other
        # tests in the same worker process may have already set an SDK
        # MeterProvider — either explicitly or via our lazy init — making it
        # impossible to observe the default provider in-process.
        # A fresh subprocess guarantees a clean OTL state.
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from opentelemetry.metrics import get_meter_provider; "
                "from opentelemetry.sdk.metrics import MeterProvider; "
                "assert not isinstance(get_meter_provider(), MeterProvider), "
                "'Default OTL provider is now an SDK MeterProvider — update "
                "the isinstance check in get_global_meter_provider()'",
            ],
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())


class GetGlobalMeterProviderTest(unittest.TestCase):
    """Tests for get_global_meter_provider() lazy initialization."""

    @patch("recidiviz.monitoring.providers.set_meter_provider")
    @patch("recidiviz.monitoring.providers.create_monitoring_meter_provider")
    @patch("recidiviz.monitoring.providers.get_meter_provider")
    def test_lazy_init_creates_real_provider_when_proxy(
        self,
        mock_get: MagicMock,
        mock_create: MagicMock,
        mock_set: MagicMock,
    ) -> None:
        """When no SDK MeterProvider has been set (proxy provider is returned),
        get_global_meter_provider() lazily creates and sets a real one."""
        proxy_provider = MagicMock(spec=[])
        real_provider = MagicMock(spec=MeterProvider)
        mock_get.return_value = proxy_provider
        mock_create.return_value = real_provider

        result = get_global_meter_provider()

        mock_create.assert_called_once()
        mock_set.assert_called_once_with(real_provider)
        self.assertIs(result, real_provider)

    @patch("recidiviz.monitoring.providers.set_meter_provider")
    @patch("recidiviz.monitoring.providers.create_monitoring_meter_provider")
    @patch("recidiviz.monitoring.providers.get_meter_provider")
    def test_lazy_init_does_not_replace_existing_sdk_provider(
        self,
        mock_get: MagicMock,
        mock_create: MagicMock,
        mock_set: MagicMock,
    ) -> None:
        """When an SDK MeterProvider has already been set,
        get_global_meter_provider() returns it without creating a new one."""
        existing_provider = MagicMock(spec=MeterProvider)
        mock_get.return_value = existing_provider

        result = get_global_meter_provider()

        mock_create.assert_not_called()
        mock_set.assert_not_called()
        self.assertIs(result, existing_provider)
