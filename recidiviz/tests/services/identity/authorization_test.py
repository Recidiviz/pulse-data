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
"""Tests for Identity Service caller authorization."""
from unittest import TestCase
from unittest.mock import patch

from recidiviz.common.constants.identity import ProductApp
from recidiviz.services.identity.authorization import (
    CallerRole,
    IdentityServiceCaller,
    get_caller,
)
from recidiviz.services.identity.exceptions import UnknownCallerError

_READER_CALLER = "reader-caller@example.com"
_EDITOR_CALLER = "editor-caller@example.com"
_IMPORTER_CALLER = "importer-caller@example.com"

_MAPPING = "recidiviz.services.identity.authorization.CALLER_BY_SERVICE_ACCOUNT"


class GetCallerTest(TestCase):
    """Tests for get_caller."""

    def setUp(self) -> None:
        self.reader_caller = IdentityServiceCaller(
            role=CallerRole.READER, source_product_app=None
        )
        self.editor_caller = IdentityServiceCaller(
            role=CallerRole.EDITOR, source_product_app=ProductApp.ADMIN_PANEL
        )
        self.importer_caller = IdentityServiceCaller(
            role=CallerRole.IMPORTER, source_product_app=None
        )
        patcher = patch.dict(
            _MAPPING,
            {
                _READER_CALLER: self.reader_caller,
                _EDITOR_CALLER: self.editor_caller,
                _IMPORTER_CALLER: self.importer_caller,
            },
            clear=True,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_mapped_reader_caller(self) -> None:
        self.assertEqual(self.reader_caller, get_caller(_READER_CALLER))

    def test_returns_mapped_editor_caller(self) -> None:
        self.assertEqual(self.editor_caller, get_caller(_EDITOR_CALLER))

    def test_returns_mapped_importer_caller(self) -> None:
        self.assertEqual(self.importer_caller, get_caller(_IMPORTER_CALLER))

    def test_raises_for_unknown_caller(self) -> None:
        with self.assertRaisesRegex(UnknownCallerError, r"Unknown caller \[stranger\]"):
            get_caller("stranger")

    def test_raises_for_none_caller(self) -> None:
        with self.assertRaisesRegex(UnknownCallerError, r"Unknown caller \[None\]"):
            get_caller(None)


class IdentityServiceCallerTest(TestCase):
    """Tests for the IdentityServiceCaller cross-field invariant."""

    def test_reader_with_product_app_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Only editor callers contribute product-app-sourced"
        ):
            IdentityServiceCaller(
                role=CallerRole.READER, source_product_app=ProductApp.ADMIN_PANEL
            )

    def test_editor_without_product_app_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Editor callers contribute product-app-sourced attributes"
        ):
            IdentityServiceCaller(role=CallerRole.EDITOR, source_product_app=None)

    def test_importer_with_product_app_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Only editor callers contribute product-app-sourced"
        ):
            IdentityServiceCaller(
                role=CallerRole.IMPORTER, source_product_app=ProductApp.ADMIN_PANEL
            )
