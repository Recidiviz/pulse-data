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
"""Tests for build_blessed_cluster_attributes."""
import datetime
import unittest

from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityName,
    IdentitySex,
)
from recidiviz.pipelines.ingest.identity.build_blessed_cluster_attributes import (
    build_blessed_cluster_attributes,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    BlessedIdentityValues,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)

_TENANT = Tenant.US_XX


class TestBuildBlessedClusterAttributes(unittest.TestCase):
    """Tests for build_blessed_cluster_attributes."""

    def test_conflicting_component_taken_from_blessed_rest_from_resolved(
        self,
    ) -> None:
        resolved = IdentityAttributes(
            tenant=_TENANT,
            name=IdentityName(tenant=_TENANT, given_name="John", surname="Johnson"),
        )

        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=resolved,
            blessed_values=BlessedIdentityValues(surname="Rivera"),
            conflicting_attributes={ConflictCheckedAttribute.SURNAME},
        )

        assert attributes is not None and attributes.name is not None
        self.assertEqual(attributes.name.surname, "Rivera")
        self.assertEqual(attributes.name.given_name, "John")

    def test_conflicting_scalar_taken_from_blessed(self) -> None:
        resolved = IdentityAttributes(
            tenant=_TENANT,
            birthdate=datetime.date(1990, 5, 5),
            sex=IdentitySex(tenant=_TENANT, sex=Sex.FEMALE),
        )

        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=resolved,
            blessed_values=BlessedIdentityValues(
                birthdate=datetime.date(1980, 1, 1), sex=Sex.MALE
            ),
            conflicting_attributes={
                ConflictCheckedAttribute.BIRTHDATE,
                ConflictCheckedAttribute.SEX,
            },
        )

        assert attributes is not None
        self.assertEqual(attributes.birthdate, datetime.date(1980, 1, 1))
        assert attributes.sex is not None
        self.assertEqual(attributes.sex.sex, Sex.MALE)

    def test_non_conflicting_scalar_kept_from_resolved(self) -> None:
        resolved = IdentityAttributes(
            tenant=_TENANT, birthdate=datetime.date(1990, 5, 5)
        )

        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=resolved,
            blessed_values=BlessedIdentityValues(surname="Rivera"),
            conflicting_attributes={ConflictCheckedAttribute.SURNAME},
        )

        assert attributes is not None
        self.assertEqual(attributes.birthdate, datetime.date(1990, 5, 5))
        assert attributes.name is not None
        self.assertEqual(attributes.name.surname, "Rivera")

    def test_no_resolved_attributes_builds_from_blessed_alone(self) -> None:
        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=None,
            blessed_values=BlessedIdentityValues(surname="Rivera"),
            conflicting_attributes={ConflictCheckedAttribute.SURNAME},
        )

        assert attributes is not None and attributes.name is not None
        self.assertEqual(attributes.name.surname, "Rivera")
        self.assertIsNone(attributes.birthdate)

    def test_conflicting_attribute_left_unset_resolves_to_null(self) -> None:
        # The reviewer blesses a surname conflict without supplying a surname, so
        # the kept cluster stores a null surname rather than one of the
        # fragments' surnames.
        resolved = IdentityAttributes(
            tenant=_TENANT,
            name=IdentityName(tenant=_TENANT, given_name="John", surname="Johnson"),
        )

        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=resolved,
            blessed_values=BlessedIdentityValues(),
            conflicting_attributes={ConflictCheckedAttribute.SURNAME},
        )

        assert attributes is not None and attributes.name is not None
        self.assertIsNone(attributes.name.surname)
        self.assertEqual(attributes.name.given_name, "John")

    def test_all_attributes_blessed_to_null_returns_none(self) -> None:
        # A cluster whose only attribute conflicts, blessed to null, resolves to
        # no attributes at all.
        resolved = IdentityAttributes(
            tenant=_TENANT, sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE)
        )

        attributes = build_blessed_cluster_attributes(
            tenant=_TENANT,
            resolved_attributes=resolved,
            blessed_values=BlessedIdentityValues(),
            conflicting_attributes={ConflictCheckedAttribute.SEX},
        )

        self.assertIsNone(attributes)
