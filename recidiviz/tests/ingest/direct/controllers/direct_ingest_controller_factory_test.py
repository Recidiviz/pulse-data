# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the DirectIngestControllerFactory."""
import unittest

from mock import Mock, patch, create_autospec

from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_controller import UsNdController


class TestDirectIngestControllerFactory(unittest.TestCase):
    """Tests for the DirectIngestControllerFactory."""

    def test_build_gcsfs_ingest_controller(self) -> None:
        mock_package = Mock()
        mock_controller = create_autospec(spec=UsNdController)
        mock_package.UsNdController.return_value = mock_controller

        with patch.dict(
            "sys.modules",
            {"recidiviz.ingest.direct.regions.us_nd.us_nd_controller": mock_package},
        ):
            controller = DirectIngestControllerFactory.build_gcsfs_ingest_controller(
                region_code="us_nd", fs=Mock()
            )
            assert controller is mock_controller
