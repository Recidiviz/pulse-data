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
"""Tests for the Identity Service Marshmallow response schemas."""
import datetime
import typing
import unittest

from recidiviz.common.constants.identity import (
    IdentifierType,
    PhoneType,
    ProductApp,
    SourceType,
)
from recidiviz.services.identity import api_schemas, types
from recidiviz.tests.services.identity.test_utils import (
    RECIDIVIZ_ID,
    build_full_identity,
    make_sourced_attribute,
)
from recidiviz.utils.types import assert_type


class IdentityDomainTypesTest(unittest.TestCase):
    """Smoke test that the expanded domain model constructs and wires together."""

    def test_build_full_identity(self) -> None:
        identity_history = build_full_identity()
        identity = identity_history.identity
        self.assertEqual(RECIDIVIZ_ID, identity.recidiviz_id)
        self.assertEqual(2, len(identity.external_ids))
        self.assertEqual(2, len(identity.attributes.dates_of_birth))
        dob_value = assert_type(
            identity.attributes.dates_of_birth[1].value, types.DateOfBirth
        )
        self.assertTrue(dob_value.canonical)
        self.assertEqual(1, len(identity_history.merge_events))
        self.assertEqual(1, len(identity_history.split_events[0].destinations))


class BuildingBlockSchemaTest(unittest.TestCase):
    """Value, sourced, and external-id schemas serialize to camelCase with enums by name."""

    def test_external_id_schema_includes_is_active(self) -> None:
        external_id = types.ExternalId(
            external_id="A123", id_type=IdentifierType.US_OZ_LOTR_ID, is_active=True
        )
        self.assertEqual(
            {"externalId": "A123", "idType": "US_OZ_LOTR_ID", "isActive": True},
            api_schemas.ExternalIdSchema().dump(external_id),
        )

    def test_external_id_schema_can_exclude_is_active(self) -> None:
        external_id = types.ExternalId(
            external_id="A123", id_type=IdentifierType.US_OZ_LOTR_ID, is_active=True
        )
        self.assertEqual(
            {"externalId": "A123", "idType": "US_OZ_LOTR_ID"},
            api_schemas.ExternalIdSchema(exclude=("is_active",)).dump(external_id),
        )

    def test_sourced_phone_number_schema(self) -> None:
        value = make_sourced_attribute(
            types.PhoneNumber(number="5551234567", type=PhoneType.CELL, preferred=True),
            source_type=SourceType.PRODUCT_APP,
            source_product_app=ProductApp.ADMIN_PANEL,
        )
        self.assertEqual(
            {
                "value": {"number": "5551234567", "type": "CELL", "preferred": True},
                "sourceType": "PRODUCT_APP",
                "sourceProductApp": "ADMIN_PANEL",
                "lastUpdatedUtc": "2026-01-02T03:04:05+00:00",
            },
            api_schemas.SourcedPhoneNumberSchema().dump(value),
        )

    def test_sourced_value_null_product_app(self) -> None:
        value = make_sourced_attribute(
            types.Email(address="frodo@fake.com", address_hash="hashfrodofakecom")
        )
        dumped = api_schemas.SourcedEmailSchema().dump(value)
        self.assertEqual({"address": "frodo@fake.com"}, dumped["value"])
        self.assertEqual("EXTERNAL_DATA_SYSTEM", dumped["sourceType"])
        self.assertIsNone(dumped["sourceProductApp"])


class DefaultIdentityAttributesSchemaTest(unittest.TestCase):
    """Default attributes: single canonical DOB, canonical flags stripped, full lists."""

    def setUp(self) -> None:
        self.dumped = api_schemas.IdentityAttributesSchema().dump(
            build_full_identity().identity.attributes
        )

    def test_collapses_to_single_canonical_date_of_birth(self) -> None:
        self.assertNotIn("datesOfBirth", self.dumped)
        self.assertEqual({"date": "1990-01-01"}, self.dumped["dateOfBirth"]["value"])
        self.assertEqual("PRODUCT_APP", self.dumped["dateOfBirth"]["sourceType"])

    def test_strips_canonical_flags_from_demographics(self) -> None:
        self.assertEqual({"gender": "MALE"}, self.dumped["genders"][0]["value"])
        self.assertEqual({"sex": "MALE"}, self.dumped["sexes"][0]["value"])
        self.assertEqual(
            {"ethnicity": "NOT_HISPANIC"}, self.dumped["ethnicities"][0]["value"]
        )

    def test_keeps_full_lists_and_phone_preferred(self) -> None:
        self.assertEqual("Frodo", self.dumped["names"][0]["value"]["givenName"])
        self.assertEqual({"race": "WHITE"}, self.dumped["races"][0]["value"])
        self.assertTrue(self.dumped["phoneNumbers"][0]["value"]["preferred"])
        self.assertEqual("test@fake.com", self.dumped["emails"][0]["value"]["address"])

    def test_canonical_date_of_birth_is_null_when_absent(self) -> None:
        attributes = types.IdentityAttributes(
            names=[],
            dates_of_birth=[
                make_sourced_attribute(
                    types.DateOfBirth(
                        date=datetime.date(1990, 1, 1),
                        canonical=False,
                        canonical_locked=False,
                    )
                )
            ],
            genders=[],
            races=[],
            sexes=[],
            ethnicities=[],
            phone_numbers=[],
            emails=[],
        )
        self.assertIsNone(
            api_schemas.IdentityAttributesSchema().dump(attributes)["dateOfBirth"]
        )


class FullIdentityAttributesSchemaTest(unittest.TestCase):
    """Full attributes: dates_of_birth list with flags, no collapsed date_of_birth."""

    def setUp(self) -> None:
        self.dumped = api_schemas.IdentityFullAttributesSchema().dump(
            build_full_identity().identity.attributes
        )

    def test_emits_dates_of_birth_list_not_single(self) -> None:
        self.assertNotIn("dateOfBirth", self.dumped)
        self.assertEqual(2, len(self.dumped["datesOfBirth"]))

    def test_retains_canonical_flags(self) -> None:
        canonical = [
            dob for dob in self.dumped["datesOfBirth"] if dob["value"]["canonical"]
        ]
        self.assertEqual(1, len(canonical))
        self.assertEqual("1990-01-01", canonical[0]["value"]["date"])
        self.assertFalse(canonical[0]["value"]["canonicalLocked"])
        self.assertTrue(self.dumped["genders"][0]["value"]["canonical"])
        self.assertIn("canonicalLocked", self.dumped["sexes"][0]["value"])
        self.assertIn("canonical", self.dumped["ethnicities"][0]["value"])


class AuditEventSchemaTest(unittest.TestCase):
    """Merge/split event schemas, incl. heterogeneous conflict/moved-attribute values."""

    def test_merge_event_schema(self) -> None:
        merge_event = build_full_identity().merge_events[0]
        dumped = api_schemas.MergeEventSchema().dump(merge_event)
        self.assertEqual("11111111-1111-1111-1111-111111111111", dumped["survivingId"])
        self.assertEqual("22222222-2222-2222-2222-222222222222", dumped["retiredId"])
        self.assertEqual("MERGE_ENDPOINT", dumped["trigger"])
        self.assertEqual("auditor@fake.com", dumped["requestedBy"])
        conflict = dumped["conflicts"][0]
        self.assertEqual("NAME", conflict["attributeType"])
        self.assertEqual("Frodo", conflict["retiredValue"]["value"]["givenName"])
        self.assertEqual("Mr. Frodo", conflict["survivingValue"]["value"]["givenName"])

    def test_split_event_schema(self) -> None:
        split_event = build_full_identity().split_events[0]
        dumped = api_schemas.SplitEventSchema().dump(split_event)
        self.assertEqual("11111111-1111-1111-1111-111111111111", dumped["originalId"])
        self.assertEqual("SPLIT_ENDPOINT", dumped["trigger"])
        self.assertIsNone(dumped["requestedBy"])
        destination = dumped["destinations"][0]
        self.assertEqual(
            "33333333-3333-3333-3333-333333333333", destination["newRecidivizId"]
        )
        self.assertEqual("OLD9", destination["externalIds"][0]["externalId"])
        self.assertEqual(
            "test@fake.com", destination["attributes"][0]["value"]["address"]
        )


class IdentitySchemaTest(unittest.TestCase):
    """Default (Identity), full (Identity), and history (IdentityHistory) forms."""

    def setUp(self) -> None:
        self.identity_history = build_full_identity()
        self.identity = self.identity_history.identity

    def test_default_form_excludes_internals_and_audit(self) -> None:
        dumped = api_schemas.IdentitySchema().dump(self.identity)
        self.assertEqual("11111111-1111-1111-1111-111111111111", dumped["recidivizId"])
        self.assertEqual("US_OZ", dumped["tenant"])
        self.assertEqual("JII", dumped["personType"])
        self.assertEqual("ACTIVE", dumped["status"])
        self.assertIsNone(dumped["mergedInto"])
        self.assertNotIn("mergeEvents", dumped)
        self.assertNotIn("splitEvents", dumped)
        # internal reconciliation bookkeeping is omitted in the default form
        self.assertNotIn("lastClusterHash", dumped)
        self.assertNotIn("skipDemographicGuard", dumped)
        # external IDs omit is_active in the default form
        self.assertNotIn("isActive", dumped["externalIds"][0])
        # attributes are the default (collapsed) form
        self.assertIn("dateOfBirth", dumped["attributes"])
        self.assertNotIn("datesOfBirth", dumped["attributes"])

    def test_full_form_adds_internals_to_the_same_identity(self) -> None:
        # IdentityFullSchema takes the same input as IdentitySchema (an Identity) and
        # emits more fields; audit history lives on IdentityHistorySchema instead.
        dumped = api_schemas.IdentityFullSchema().dump(self.identity)
        self.assertTrue(dumped["externalIds"][0]["isActive"])
        self.assertIn("datesOfBirth", dumped["attributes"])
        self.assertNotIn("dateOfBirth", dumped["attributes"])
        self.assertEqual("cluster-hash-abc", dumped["lastClusterHash"])
        self.assertFalse(dumped["skipDemographicGuard"])
        self.assertNotIn("mergeEvents", dumped)
        self.assertNotIn("splitEvents", dumped)

    def test_history_form_adds_audit_to_the_full_identity(self) -> None:
        dumped = api_schemas.IdentityHistorySchema().dump(self.identity_history)
        # everything the full form has...
        self.assertTrue(dumped["externalIds"][0]["isActive"])
        self.assertIn("datesOfBirth", dumped["attributes"])
        self.assertEqual("cluster-hash-abc", dumped["lastClusterHash"])
        self.assertFalse(dumped["skipDemographicGuard"])
        self.assertEqual("2026-01-01T00:00:00+00:00", dumped["createdUtc"])
        # ...plus the merge/split audit history
        self.assertEqual(1, len(dumped["mergeEvents"]))
        self.assertEqual(1, len(dumped["splitEvents"]))


class ResponseWrapperSchemaTest(unittest.TestCase):
    """Merge/split/search response envelopes."""

    def setUp(self) -> None:
        self.identity = build_full_identity().identity

    def test_merge_response_schema(self) -> None:
        dumped = api_schemas.MergeResponseSchema().dump(
            {"surviving_identity": self.identity}
        )
        self.assertEqual(
            "11111111-1111-1111-1111-111111111111",
            dumped["survivingIdentity"]["recidivizId"],
        )

    def test_split_response_schema(self) -> None:
        dumped = api_schemas.SplitResponseSchema().dump(
            {"original_identity": self.identity, "new_identity": self.identity}
        )
        self.assertEqual(
            "11111111-1111-1111-1111-111111111111",
            dumped["originalIdentity"]["recidivizId"],
        )
        self.assertEqual(
            "11111111-1111-1111-1111-111111111111",
            dumped["newIdentity"]["recidivizId"],
        )

    def test_search_response_schema_with_cursor(self) -> None:
        dumped = api_schemas.SearchResponseSchema().dump(
            {"results": [self.identity], "next_cursor": "abc"}
        )
        self.assertEqual(1, len(dumped["results"]))
        self.assertEqual("abc", dumped["nextCursor"])

    def test_search_response_schema_null_cursor(self) -> None:
        dumped = api_schemas.SearchResponseSchema().dump(
            {"results": [], "next_cursor": None}
        )
        self.assertEqual([], dumped["results"])
        self.assertIsNone(dumped["nextCursor"])


class SourcedSchemaDispatchTest(unittest.TestCase):
    """Guards the value-type -> sourced-schema dispatch table against drift."""

    def test_dispatch_table_covers_every_attribute_value_type(self) -> None:
        # If this fails, a value type was added to types.AttributeValue without a
        # corresponding entry in api_schemas._SOURCED_SCHEMA_BY_VALUE_TYPE, which
        # would raise a KeyError when serializing that value in an audit event.
        self.assertEqual(
            set(typing.get_args(types.AttributeValue)),
            set(
                api_schemas._SOURCED_SCHEMA_BY_VALUE_TYPE  # pylint: disable=protected-access
            ),
            "_SOURCED_SCHEMA_BY_VALUE_TYPE is out of sync with types.AttributeValue",
        )
