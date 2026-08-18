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
"""Tests for identity_cluster_override."""
import datetime
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    BlessedIdentityValues,
    IdentityClusterOverride,
    IdentityClusterOverrideDisposition,
    IdentityClusterOverrides,
    identity_cluster_override_schema_fields,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)

_TENANT = Tenant.US_XX
_RECORDED_AT = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
_CONFLICTS_HASH = "conflictshash1"


def _bless(
    external_ids: tuple[tuple[str, str], ...],
    blessed_values: BlessedIdentityValues,
    tenant: Tenant = _TENANT,
    person_type: PersonType = PersonType.JII,
) -> IdentityClusterOverride:
    return IdentityClusterOverride(
        tenant=tenant,
        person_type=person_type,
        external_ids=external_ids,
        disposition=IdentityClusterOverrideDisposition.BLESS,
        blessed_values=blessed_values,
        conflicts_hash=_CONFLICTS_HASH,
        recorded_by="tester",
        recorded_at=_RECORDED_AT,
        note="test",
    )


def _exclude(
    external_ids: tuple[tuple[str, str], ...],
    tenant: Tenant = _TENANT,
) -> IdentityClusterOverride:
    return IdentityClusterOverride(
        tenant=tenant,
        person_type=PersonType.JII,
        external_ids=external_ids,
        disposition=IdentityClusterOverrideDisposition.EXCLUDE,
        blessed_values=None,
        recorded_by="tester",
        recorded_at=_RECORDED_AT,
        note="test",
    )


class TestIdentityClusterOverride(unittest.TestCase):
    """Tests for IdentityClusterOverride."""

    def test_bless_round_trip(self) -> None:
        override = _bless(
            (("A", "T1"), ("B", "T2")),
            BlessedIdentityValues(
                given_name="John",
                middle_name="Q",
                surname="Rivera",
                name_suffix="JR",
                birthdate=datetime.date(1980, 1, 1),
                sex=Sex.MALE,
                gender=Gender.MALE,
                ethnicity=Ethnicity.HISPANIC,
            ),
        )
        self.assertEqual(
            override, IdentityClusterOverride.from_bq_row(override.to_bq_row())
        )

    def test_exclude_round_trip(self) -> None:
        override = _exclude((("A", "T1"),))
        self.assertEqual(
            override, IdentityClusterOverride.from_bq_row(override.to_bq_row())
        )

    def test_exclude_with_blessed_values_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"EXCLUDE override"):
            IdentityClusterOverride(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(("A", "T1"),),
                disposition=IdentityClusterOverrideDisposition.EXCLUDE,
                blessed_values=BlessedIdentityValues(surname="Rivera"),
                recorded_by="tester",
                recorded_at=_RECORDED_AT,
                note="test",
            )

    def test_bless_without_values_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"BLESS override must carry a blessed_values record"
        ):
            IdentityClusterOverride(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(("A", "T1"),),
                disposition=IdentityClusterOverrideDisposition.BLESS,
                blessed_values=None,
                recorded_by="tester",
                recorded_at=_RECORDED_AT,
                note="test",
            )

    def test_bless_without_conflicts_hash_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"BLESS override must carry the conflicts_hash"
        ):
            IdentityClusterOverride(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(("A", "T1"),),
                disposition=IdentityClusterOverrideDisposition.BLESS,
                blessed_values=BlessedIdentityValues(surname="Rivera"),
                conflicts_hash=None,
                recorded_by="tester",
                recorded_at=_RECORDED_AT,
                note="test",
            )

    def test_exclude_with_conflicts_hash_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"EXCLUDE override .* must carry no conflicts_hash"
        ):
            IdentityClusterOverride(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(("A", "T1"),),
                disposition=IdentityClusterOverrideDisposition.EXCLUDE,
                blessed_values=None,
                conflicts_hash=_CONFLICTS_HASH,
                recorded_by="tester",
                recorded_at=_RECORDED_AT,
                note="test",
            )

    def test_bless_allows_empty_values(self) -> None:
        # A BLESS override may leave every field null: the pipeline keeps the
        # cluster and stores null for each conflicting attribute.
        override = _bless((("A", "T1"),), BlessedIdentityValues())
        self.assertEqual(BlessedIdentityValues(), override.blessed_values)

    def test_naive_recorded_at_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"timezone-aware recorded_at"):
            IdentityClusterOverride(
                tenant=_TENANT,
                person_type=PersonType.JII,
                external_ids=(("A", "T1"),),
                disposition=IdentityClusterOverrideDisposition.EXCLUDE,
                blessed_values=None,
                recorded_by="tester",
                recorded_at=datetime.datetime(2026, 1, 2, 3, 4, 5),
                note="test",
            )

    def test_blessed_values_cover_every_conflict_checked_attribute(self) -> None:
        """A new ConflictCheckedAttribute member must gain a matching
        BlessedIdentityValues field, or the recording tool could not record a
        blessed value for it and the pipeline would silently resolve an
        attribute a human already judged."""
        self.assertEqual(
            {field.name for field in attr.fields(BlessedIdentityValues)},
            {attribute.value for attribute in ConflictCheckedAttribute},
        )

    def test_to_bq_row_matches_schema(self) -> None:
        """Every key the row emits is a column in the schema, and vice versa, so
        the value object and the table cannot drift apart."""
        override = _bless(
            (("A", "T1"),),
            BlessedIdentityValues(surname="Rivera", sex=Sex.MALE),
        )
        row = override.to_bq_row()
        schema = identity_cluster_override_schema_fields()

        self.assertEqual(set(row.keys()), {field.name for field in schema})
        blessed_values_field = next(
            field for field in schema if field.name == "blessed_values"
        )
        blessed_values_row = row["blessed_values"]
        assert isinstance(blessed_values_row, dict)
        self.assertEqual(
            set(blessed_values_row.keys()),
            {field.name for field in blessed_values_field.fields},
        )


class TestIdentityClusterOverrides(unittest.TestCase):
    """Tests for the IdentityClusterOverrides lookup collection."""

    def test_get_override_matches_regardless_of_id_order(self) -> None:
        override = _exclude((("A", "T1"), ("B", "T2")))
        overrides = IdentityClusterOverrides.for_overrides(
            tenant=_TENANT, overrides=[override]
        )
        self.assertEqual(overrides.get_override((("B", "T2"), ("A", "T1"))), override)

    def test_get_override_requires_exact_id_set(self) -> None:
        overrides = IdentityClusterOverrides.for_overrides(
            tenant=_TENANT, overrides=[_exclude((("A", "T1"),))]
        )
        # A subset and a superset of the recorded ids both fail to match.
        self.assertIsNone(overrides.get_override((("B", "T2"),)))
        self.assertIsNone(overrides.get_override((("A", "T1"), ("B", "T2"))))

    def test_duplicate_external_ids_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"two overrides for the same cluster"):
            IdentityClusterOverrides.for_overrides(
                tenant=_TENANT,
                overrides=[
                    _exclude((("A", "T1"),)),
                    _bless((("A", "T1"),), BlessedIdentityValues(surname="Rivera")),
                ],
            )

    def test_wrong_tenant_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"belongs to tenant \[US_YY\]"):
            IdentityClusterOverrides.for_overrides(
                tenant=_TENANT,
                overrides=[_exclude((("A", "T1"),), tenant=Tenant.US_YY)],
            )

    def test_empty(self) -> None:
        overrides = IdentityClusterOverrides.empty(_TENANT)
        self.assertIsNone(overrides.get_override((("A", "T1"),)))
