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
"""Tests for Identity Service helpers."""
from unittest import TestCase
from unittest.mock import patch

from recidiviz.common.constants.identity import ProductApp
from recidiviz.services.identity.exceptions import UnknownCallerError
from recidiviz.services.identity.helpers import get_source_product_app

_MAPPED_CALLER = "mapped-caller@x.iam.gserviceaccount.com"
_NO_APP_CALLER = "no-app-caller@x.iam.gserviceaccount.com"

_MAPPING = "recidiviz.services.identity.helpers.PRODUCT_APP_BY_SERVICE_ACCOUNT"


class GetSourceProductAppTest(TestCase):
    """Tests for get_source_product_app."""

    def setUp(self) -> None:
        patcher = patch.dict(
            _MAPPING,
            {_MAPPED_CALLER: ProductApp.ADMIN_PANEL, _NO_APP_CALLER: None},
            clear=True,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_mapped_product_app(self) -> None:
        self.assertEqual(ProductApp.ADMIN_PANEL, get_source_product_app(_MAPPED_CALLER))

    def test_returns_none_for_mapped_caller_without_app(self) -> None:
        self.assertIsNone(get_source_product_app(_NO_APP_CALLER))

    def test_raises_for_unknown_caller(self) -> None:
        with self.assertRaisesRegex(UnknownCallerError, r"Unknown caller \[stranger\]"):
            get_source_product_app("stranger")

    def test_raises_for_none_caller(self) -> None:
        with self.assertRaisesRegex(UnknownCallerError, r"Unknown caller \[None\]"):
            get_source_product_app(None)
