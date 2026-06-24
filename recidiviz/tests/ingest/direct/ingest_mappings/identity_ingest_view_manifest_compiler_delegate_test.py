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
"""Tests for IdentityIngestViewManifestCompilerDelegate."""

import unittest

from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
    IdentityName,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region


class _IdentityDelegateTestBase(unittest.TestCase):
    """Base class providing a fake-region-backed IdentityIngestViewManifestCompilerDelegate."""

    def setUp(self) -> None:
        self.delegate = IdentityIngestViewManifestCompilerDelegate(
            region=fake_region(
                region_code="us_xx",
                environment="staging",
                region_module=fake_regions,
            )
        )


class TestGetEntityCls(_IdentityDelegateTestBase):
    def test_resolves_identity_fragment(self) -> None:
        self.assertIs(
            self.delegate.get_entity_cls("IdentityFragment"), IdentityFragment
        )

    def test_resolves_identity_external_id(self) -> None:
        self.assertIs(
            self.delegate.get_entity_cls("IdentityExternalId"), IdentityExternalId
        )

    def test_resolves_identity_attributes(self) -> None:
        self.assertIs(
            self.delegate.get_entity_cls("IdentityAttributes"), IdentityAttributes
        )

    def test_resolves_identity_name(self) -> None:
        self.assertIs(self.delegate.get_entity_cls("IdentityName"), IdentityName)


class TestGetEntityFactoryClass(_IdentityDelegateTestBase):
    def test_resolves_identity_fragment_factory(self) -> None:
        factory_cls = self.delegate.get_entity_factory_class("IdentityFragment")
        self.assertEqual("IdentityFragmentFactory", factory_cls.__name__)

    def test_unknown_entity_raises(self) -> None:
        with self.assertRaises(ValueError):
            self.delegate.get_entity_factory_class("NonexistentEntity")


class TestGetEnumCls(_IdentityDelegateTestBase):
    def test_resolves_gender(self) -> None:
        self.assertIs(self.delegate.get_enum_cls("Gender"), Gender)

    def test_resolves_race(self) -> None:
        self.assertIs(self.delegate.get_enum_cls("Race"), Race)

    def test_resolves_sex(self) -> None:
        self.assertIs(self.delegate.get_enum_cls("Sex"), Sex)

    def test_resolves_ethnicity(self) -> None:
        self.assertIs(self.delegate.get_enum_cls("Ethnicity"), Ethnicity)

    def test_unknown_enum_raises(self) -> None:
        with self.assertRaises(ValueError):
            self.delegate.get_enum_cls("NonexistentEnum")


class TestGetCommonArgs(_IdentityDelegateTestBase):
    def test_returns_tenant(self) -> None:
        self.assertEqual({"tenant": Tenant.US_XX}, self.delegate.get_common_args())


class TestGetFilterIfNullField(_IdentityDelegateTestBase):
    def test_returns_none(self) -> None:
        self.assertIsNone(self.delegate.get_filter_if_null_field(IdentityFragment))


class TestIsJsonField(_IdentityDelegateTestBase):
    def test_returns_false(self) -> None:
        self.assertFalse(self.delegate.is_json_field(IdentityFragment, "tenant"))
