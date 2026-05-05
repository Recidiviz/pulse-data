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

from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.pipelines.batch_identity_clustering.entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
    IdentityName,
)
from recidiviz.pipelines.batch_identity_clustering.manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)


class TestGetIdentityViewNames(unittest.TestCase):
    def test_no_mappings_returns_empty(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_NONEXISTENT")
        self.assertEqual([], delegate.get_identity_ingest_view_names())


class TestGetEntityCls(unittest.TestCase):
    def test_resolves_identity_fragment(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        cls = delegate.get_entity_cls("IdentityFragment")
        self.assertIs(cls, IdentityFragment)

    def test_resolves_identity_external_id(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        cls = delegate.get_entity_cls("IdentityExternalId")
        self.assertIs(cls, IdentityExternalId)

    def test_resolves_identity_attributes(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        cls = delegate.get_entity_cls("IdentityAttributes")
        self.assertIs(cls, IdentityAttributes)

    def test_resolves_identity_name(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        cls = delegate.get_entity_cls("IdentityName")
        self.assertIs(cls, IdentityName)


class TestGetEntityFactoryClass(unittest.TestCase):
    def test_resolves_identity_fragment_factory(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        factory_cls = delegate.get_entity_factory_class("IdentityFragment")
        self.assertEqual("IdentityFragmentFactory", factory_cls.__name__)

    def test_unknown_entity_raises(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        with self.assertRaises(ValueError):
            delegate.get_entity_factory_class("NonexistentEntity")


class TestGetEnumCls(unittest.TestCase):
    def test_resolves_gender(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertIs(delegate.get_enum_cls("Gender"), Gender)

    def test_resolves_race(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertIs(delegate.get_enum_cls("Race"), Race)

    def test_resolves_sex(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertIs(delegate.get_enum_cls("Sex"), Sex)

    def test_resolves_ethnicity(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertIs(delegate.get_enum_cls("Ethnicity"), Ethnicity)

    def test_unknown_enum_raises(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        with self.assertRaises(ValueError):
            delegate.get_enum_cls("NonexistentEnum")


class TestGetCommonArgs(unittest.TestCase):
    def test_returns_tenant(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertEqual({"tenant": "US_OZ"}, delegate.get_common_args())


class TestGetFilterIfNullField(unittest.TestCase):
    def test_returns_none(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertIsNone(delegate.get_filter_if_null_field(IdentityFragment))


class TestIsJsonField(unittest.TestCase):
    def test_returns_false(self) -> None:
        delegate = IdentityIngestViewManifestCompilerDelegate(tenant="US_OZ")
        self.assertFalse(delegate.is_json_field(IdentityFragment, "tenant"))
