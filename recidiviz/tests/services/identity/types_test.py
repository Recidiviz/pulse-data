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
"""Tests for the Identity Service domain types and their field validators."""
import datetime
import unittest
import uuid

import attr

from recidiviz.common import demographics
from recidiviz.common.constants.identity import (
    AttributeType,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    PhoneType,
    ProductApp,
    SourceType,
    SplitTrigger,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.services.identity import types
from recidiviz.services.identity.types import (
    AttributeConflict,
    CanonicalAttributeMixin,
    DateOfBirth,
    Email,
    Ethnicity,
    ExternalId,
    Gender,
    Identity,
    IdentityAttributes,
    IdentityHistory,
    MergeEvent,
    Name,
    PhoneNumber,
    Race,
    Sex,
    SourcedAttributeValue,
    SplitDestination,
    SplitEvent,
)
from recidiviz.utils.types import assert_type

_UTC = datetime.timezone.utc
_TS = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=_UTC)
_RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_MERGED_INTO_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
_NEW_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
_RETIRED_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")


def _empty_attributes() -> IdentityAttributes:
    return IdentityAttributes(
        names=[],
        dates_of_birth=[],
        genders=[],
        races=[],
        sexes=[],
        ethnicities=[],
        phone_numbers=[],
        emails=[],
    )


def _active_identity() -> Identity:
    return Identity(
        recidiviz_id=_RECIDIVIZ_ID,
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        status=IdentityStatus.ACTIVE,
        merged_into=None,
        last_cluster_hash=None,
        skip_demographic_guard=False,
        created_utc=_TS,
        last_updated_utc=_TS,
        external_ids=[],
        attributes=_empty_attributes(),
    )


def _sourced(
    value: types.AttributeValue,
    *,
    source_type: SourceType = SourceType.EXTERNAL_DATA_SYSTEM,
    source_product_app: ProductApp | None = None,
) -> SourcedAttributeValue:
    return SourcedAttributeValue(
        value=value,
        source_type=source_type,
        source_product_app=source_product_app,
        last_updated_utc=_TS,
    )


class ExternalIdTest(unittest.TestCase):
    """Tests for ExternalId construction and validation."""

    def test_constructs(self) -> None:
        ext_id = ExternalId(
            external_id="ABC123",
            id_type=IdentifierType.US_OZ_LOTR_ID,
            is_active=True,
        )
        self.assertEqual("ABC123", ext_id.external_id)
        self.assertIs(IdentifierType.US_OZ_LOTR_ID, ext_id.id_type)
        self.assertTrue(ext_id.is_active)

    def test_inactive_constructs(self) -> None:
        ext_id = ExternalId(
            external_id="OLD9",
            id_type=IdentifierType.US_OZ_LOTR_ID,
            is_active=False,
        )
        self.assertFalse(ext_id.is_active)


class NameTest(unittest.TestCase):
    """Tests for Name value type."""

    def test_constructs_with_all_fields(self) -> None:
        name = Name(
            surname="Baggins",
            given_name="Frodo",
            middle_names=["R"],
            name_suffix="Jr.",
            use=NameUse.OFFICIAL,
        )
        self.assertEqual("Baggins", name.surname)
        self.assertEqual(["R"], name.middle_names)
        self.assertIs(NameUse.OFFICIAL, name.use)

    def test_constructs_with_nullable_fields_none(self) -> None:
        name = Name(
            surname=None,
            given_name=None,
            middle_names=[],
            name_suffix=None,
            use=None,
        )
        self.assertIsNone(name.surname)
        self.assertIsNone(name.use)


class DateOfBirthTest(unittest.TestCase):
    """Tests for DateOfBirth value type."""

    def test_constructs(self) -> None:
        dob = DateOfBirth(
            date=datetime.date(1990, 9, 22),
            canonical=True,
            canonical_locked=False,
        )
        self.assertEqual(datetime.date(1990, 9, 22), dob.date)
        self.assertTrue(dob.canonical)
        self.assertFalse(dob.canonical_locked)


class GenderTest(unittest.TestCase):
    def test_constructs(self) -> None:
        g = Gender(
            gender=demographics.Gender.MALE, canonical=True, canonical_locked=False
        )
        self.assertIs(demographics.Gender.MALE, g.gender)


class RaceTest(unittest.TestCase):
    def test_constructs(self) -> None:
        r = Race(race=demographics.Race.WHITE)
        self.assertIs(demographics.Race.WHITE, r.race)


class SexTest(unittest.TestCase):
    def test_constructs(self) -> None:
        s = Sex(sex=demographics.Sex.MALE, canonical=True, canonical_locked=False)
        self.assertIs(demographics.Sex.MALE, s.sex)


class EthnicityTest(unittest.TestCase):
    def test_constructs(self) -> None:
        e = Ethnicity(
            ethnicity=demographics.Ethnicity.NOT_HISPANIC,
            canonical=True,
            canonical_locked=False,
        )
        self.assertIs(demographics.Ethnicity.NOT_HISPANIC, e.ethnicity)


class PhoneNumberTest(unittest.TestCase):
    def test_constructs_with_optional_fields(self) -> None:
        phone = PhoneNumber(number="5551234567", type=PhoneType.CELL, preferred=True)
        self.assertEqual("5551234567", phone.number)
        self.assertIs(PhoneType.CELL, phone.type)
        self.assertTrue(phone.preferred)

    def test_constructs_with_null_optional_fields(self) -> None:
        phone = PhoneNumber(number="5551234567", type=None, preferred=None)
        self.assertIsNone(phone.type)
        self.assertIsNone(phone.preferred)


class EmailTest(unittest.TestCase):
    def test_constructs(self) -> None:
        email = Email(address="frodo@fake.com", address_hash="hashfrodofakecom")
        self.assertEqual("frodo@fake.com", email.address)
        self.assertEqual("hashfrodofakecom", email.address_hash)


class CanonicalAttributeMixinTest(unittest.TestCase):
    """Tests that CanonicalAttributeMixin supplies the shared canonical-selection
    fields to exactly the attribute value types that participate in canonical
    selection, and to no others."""

    def test_canonical_value_types_inherit_the_mixin(self) -> None:
        for value in (
            DateOfBirth(
                date=datetime.date(1990, 1, 1), canonical=True, canonical_locked=False
            ),
            Gender(
                gender=demographics.Gender.MALE,
                canonical=True,
                canonical_locked=False,
            ),
            Sex(sex=demographics.Sex.MALE, canonical=True, canonical_locked=False),
            Ethnicity(
                ethnicity=demographics.Ethnicity.NOT_HISPANIC,
                canonical=True,
                canonical_locked=False,
            ),
        ):
            with self.subTest(value_type=type(value).__name__):
                self.assertIsInstance(value, CanonicalAttributeMixin)

    def test_non_canonical_value_types_do_not_inherit_the_mixin(self) -> None:
        for value in (
            Name(
                surname=None,
                given_name=None,
                middle_names=[],
                name_suffix=None,
                use=None,
            ),
            Race(race=demographics.Race.WHITE),
            PhoneNumber(number="5551234567", type=None, preferred=None),
            Email(address="a@fake.com", address_hash="hashafakecom"),
        ):
            with self.subTest(value_type=type(value).__name__):
                self.assertNotIsInstance(value, CanonicalAttributeMixin)

    def test_mixin_fields_are_validated_on_the_subclass(self) -> None:
        with self.assertRaises(TypeError):
            DateOfBirth(
                date=datetime.date(1990, 1, 1),
                canonical="yes",  # type: ignore[arg-type]
                canonical_locked=False,
            )


class SourcedAttributeValueTest(unittest.TestCase):
    """Tests for SourcedAttributeValue wrapping each value type."""

    def test_wraps_name(self) -> None:
        name = Name(
            surname="Baggins",
            given_name="Frodo",
            middle_names=[],
            name_suffix=None,
            use=None,
        )
        sv = _sourced(name)
        self.assertIs(name, sv.value)
        self.assertIs(SourceType.EXTERNAL_DATA_SYSTEM, sv.source_type)
        self.assertIsNone(sv.source_product_app)

    def test_wraps_email(self) -> None:
        email = Email(address="test@fake.com", address_hash="hashtestfakecom")
        sv = _sourced(email)
        self.assertIs(email, sv.value)

    def test_product_app_source(self) -> None:
        dob = DateOfBirth(
            date=datetime.date(1990, 1, 1), canonical=True, canonical_locked=False
        )
        sv = _sourced(
            dob,
            source_type=SourceType.PRODUCT_APP,
            source_product_app=ProductApp.ADMIN_PANEL,
        )
        self.assertIs(SourceType.PRODUCT_APP, sv.source_type)
        self.assertIs(ProductApp.ADMIN_PANEL, sv.source_product_app)

    def test_rejects_naive_last_updated_utc(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Expected timezone value to not be empty"
        ):
            SourcedAttributeValue(
                value=Email(address="a@fake.com", address_hash="hashafakecom"),
                source_type=SourceType.EXTERNAL_DATA_SYSTEM,
                source_product_app=None,
                last_updated_utc=datetime.datetime(2026, 1, 1),
            )


class SourcedAttributeValueFromDictTest(unittest.TestCase):
    """Tests for SourcedAttributeValue.from_dict, which decodes the JSONB
    snapshots stored in the audit tables (attribute_conflicts and
    split_event_moved_attributes) back into typed values. The wrapped value is
    polymorphic, so the row's attribute_type selects the leaf decoder.

    The decoded shape is the JSON-native form psycopg returns from a JSONB
    column: enums as their string `.value`, dates/datetimes as ISO strings.
    """

    def test_from_dict_decodes_each_attribute_type(self) -> None:
        """from_dict decodes the JSON-native dict for each leaf value type,
        dispatching on the row's attribute_type, and decodes the shared
        provenance fields (here exercising the PRODUCT_APP path)."""
        cases: list[tuple[AttributeType, dict, types.AttributeValue]] = [
            (
                AttributeType.NAME,
                {
                    "surname": "Baggins",
                    "given_name": "Frodo",
                    "middle_names": ["R"],
                    "name_suffix": None,
                    "use": "OFFICIAL",
                },
                Name(
                    surname="Baggins",
                    given_name="Frodo",
                    middle_names=["R"],
                    name_suffix=None,
                    use=NameUse.OFFICIAL,
                ),
            ),
            (
                AttributeType.DATE_OF_BIRTH,
                {"date": "1990-09-22", "canonical": True, "canonical_locked": False},
                DateOfBirth(
                    date=datetime.date(1990, 9, 22),
                    canonical=True,
                    canonical_locked=False,
                ),
            ),
            (
                AttributeType.GENDER,
                {"gender": "MALE", "canonical": True, "canonical_locked": False},
                Gender(
                    gender=demographics.Gender.MALE,
                    canonical=True,
                    canonical_locked=False,
                ),
            ),
            (
                AttributeType.RACE,
                {"race": "WHITE"},
                Race(race=demographics.Race.WHITE),
            ),
            (
                AttributeType.SEX,
                {"sex": "MALE", "canonical": False, "canonical_locked": True},
                Sex(sex=demographics.Sex.MALE, canonical=False, canonical_locked=True),
            ),
            (
                AttributeType.ETHNICITY,
                {
                    "ethnicity": "NOT_HISPANIC",
                    "canonical": True,
                    "canonical_locked": False,
                },
                Ethnicity(
                    ethnicity=demographics.Ethnicity.NOT_HISPANIC,
                    canonical=True,
                    canonical_locked=False,
                ),
            ),
            (
                AttributeType.PHONE_NUMBER,
                {"number": "5551234567", "type": "CELL", "preferred": True},
                PhoneNumber(number="5551234567", type=PhoneType.CELL, preferred=True),
            ),
            (
                AttributeType.EMAIL,
                {"address": "frodo@fake.com", "address_hash": "hashfrodofakecom"},
                Email(address="frodo@fake.com", address_hash="hashfrodofakecom"),
            ),
        ]
        for attribute_type, value_dict, expected_value in cases:
            with self.subTest(attribute_type=attribute_type):
                data = {
                    "value": value_dict,
                    "source_type": "PRODUCT_APP",
                    "source_product_app": "admin_panel",
                    "last_updated_utc": "2026-01-01T12:00:00+00:00",
                }
                self.assertEqual(
                    SourcedAttributeValue(
                        value=expected_value,
                        source_type=SourceType.PRODUCT_APP,
                        source_product_app=ProductApp.ADMIN_PANEL,
                        last_updated_utc=_TS,
                    ),
                    SourcedAttributeValue.from_dict(
                        data, attribute_type=attribute_type
                    ),
                )

    def test_from_dict_decodes_null_optional_value_fields(self) -> None:
        data = {
            "value": {"number": "5551234567", "type": None, "preferred": None},
            "source_type": "EXTERNAL_DATA_SYSTEM",
            "source_product_app": None,
            "last_updated_utc": "2026-01-01T12:00:00+00:00",
        }
        result = SourcedAttributeValue.from_dict(
            data, attribute_type=AttributeType.PHONE_NUMBER
        )
        self.assertEqual(
            PhoneNumber(number="5551234567", type=None, preferred=None), result.value
        )


class IdentityAttributesTest(unittest.TestCase):
    """Tests for IdentityAttributes container."""

    def test_empty_attributes_construct(self) -> None:
        attrs = _empty_attributes()
        self.assertEqual([], attrs.names)
        self.assertEqual([], attrs.dates_of_birth)
        self.assertEqual([], attrs.emails)

    def test_populated_attributes_construct(self) -> None:
        name_sv = _sourced(
            Name(
                surname="Baggins",
                given_name="Frodo",
                middle_names=[],
                name_suffix=None,
                use=NameUse.OFFICIAL,
            )
        )
        attrs = IdentityAttributes(
            names=[name_sv],
            dates_of_birth=[],
            genders=[],
            races=[],
            sexes=[],
            ethnicities=[],
            phone_numbers=[],
            emails=[],
        )
        self.assertEqual(1, len(attrs.names))
        self.assertEqual("Frodo", assert_type(attrs.names[0].value, Name).given_name)


class IdentityDomainTypesTest(unittest.TestCase):
    """Tests that the Identity domain type validates its fields and invariants."""

    def test_active_identity_constructs(self) -> None:
        identity = _active_identity()
        self.assertEqual(_RECIDIVIZ_ID, identity.recidiviz_id)
        self.assertIsNone(identity.merged_into)
        self.assertIs(datetime.timezone.utc, identity.created_utc.tzinfo)
        self.assertEqual([], identity.external_ids)

    def test_active_identity_with_external_ids_and_attributes(self) -> None:
        ext_id = ExternalId(
            external_id="A123", id_type=IdentifierType.US_OZ_LOTR_ID, is_active=True
        )
        email_sv = _sourced(
            Email(address="frodo@fake.com", address_hash="hashfrodofakecom")
        )
        identity = Identity(
            recidiviz_id=_RECIDIVIZ_ID,
            tenant=Tenant.US_OZ,
            person_type=PersonType.JII,
            status=IdentityStatus.ACTIVE,
            merged_into=None,
            last_cluster_hash=None,
            skip_demographic_guard=False,
            created_utc=_TS,
            last_updated_utc=_TS,
            external_ids=[ext_id],
            attributes=IdentityAttributes(
                names=[],
                dates_of_birth=[],
                genders=[],
                races=[],
                sexes=[],
                ethnicities=[],
                phone_numbers=[],
                emails=[email_sv],
            ),
        )
        self.assertEqual(1, len(identity.external_ids))
        self.assertEqual(1, len(identity.attributes.emails))

    def test_retired_identity_constructs(self) -> None:
        identity = attr.evolve(
            _active_identity(),
            status=IdentityStatus.RETIRED,
            merged_into=_MERGED_INTO_ID,
        )
        self.assertIs(IdentityStatus.RETIRED, identity.status)
        self.assertEqual(_MERGED_INTO_ID, identity.merged_into)

    def test_rejects_naive_datetime(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Expected timezone value to not be empty"
        ):
            attr.evolve(
                _active_identity(),
                created_utc=datetime.datetime(2026, 1, 1, 12, 0),
            )

    def test_rejects_non_utc_datetime(self) -> None:
        plus_five = datetime.timezone(datetime.timedelta(hours=5))
        with self.assertRaisesRegex(ValueError, r"Expected timezone value to be UTC"):
            attr.evolve(
                _active_identity(),
                created_utc=datetime.datetime(2026, 1, 1, 12, 0, tzinfo=plus_five),
            )

    def test_rejects_wrong_typed_enum_field(self) -> None:
        with self.assertRaises(TypeError):
            attr.evolve(_active_identity(), status="ACTIVE")  # type: ignore[arg-type]

    def test_rejects_retired_without_merged_into(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"has status RETIRED but no merged_into"
        ):
            attr.evolve(_active_identity(), status=IdentityStatus.RETIRED)

    def test_rejects_merged_into_on_non_retired_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, r"is not RETIRED"):
            attr.evolve(_active_identity(), merged_into=_MERGED_INTO_ID)


class AttributeConflictTest(unittest.TestCase):
    """Tests for AttributeConflict merge audit type."""

    def test_constructs(self) -> None:
        conflict = AttributeConflict(
            attribute_type=AttributeType.NAME,
            retired_value=_sourced(
                Name(
                    surname="A",
                    given_name="B",
                    middle_names=[],
                    name_suffix=None,
                    use=None,
                )
            ),
            surviving_value=_sourced(
                Name(
                    surname="A",
                    given_name="C",
                    middle_names=[],
                    name_suffix=None,
                    use=None,
                )
            ),
        )
        self.assertIs(AttributeType.NAME, conflict.attribute_type)
        self.assertEqual(
            "B", assert_type(conflict.retired_value.value, Name).given_name
        )
        self.assertEqual(
            "C", assert_type(conflict.surviving_value.value, Name).given_name
        )


class MergeEventTest(unittest.TestCase):
    """Tests for MergeEvent audit type."""

    def test_constructs(self) -> None:
        event = MergeEvent(
            surviving_id=_RECIDIVIZ_ID,
            retired_id=_RETIRED_ID,
            trigger=MergeTrigger.MERGE_ENDPOINT,
            requested_by="auditor@fake.com",
            timestamp_utc=_TS,
            conflicts=[],
        )
        self.assertIs(MergeTrigger.MERGE_ENDPOINT, event.trigger)
        self.assertEqual(_RECIDIVIZ_ID, event.surviving_id)
        self.assertEqual(_RETIRED_ID, event.retired_id)
        self.assertEqual("auditor@fake.com", event.requested_by)
        self.assertEqual([], event.conflicts)

    def test_constructs_with_null_requested_by(self) -> None:
        event = MergeEvent(
            surviving_id=_RECIDIVIZ_ID,
            retired_id=_RETIRED_ID,
            trigger=MergeTrigger.IMPORT,
            requested_by=None,
            timestamp_utc=_TS,
            conflicts=[],
        )
        self.assertIsNone(event.requested_by)

    def test_with_conflicts(self) -> None:
        conflict = AttributeConflict(
            attribute_type=AttributeType.EMAIL,
            retired_value=_sourced(
                Email(address="old@fake.com", address_hash="hasholdfakecom")
            ),
            surviving_value=_sourced(
                Email(address="new@fake.com", address_hash="hashnewfakecom")
            ),
        )
        event = MergeEvent(
            surviving_id=_RECIDIVIZ_ID,
            retired_id=_RETIRED_ID,
            trigger=MergeTrigger.IMPORT,
            requested_by=None,
            timestamp_utc=_TS,
            conflicts=[conflict],
        )
        self.assertEqual(1, len(event.conflicts))


class SplitEventTest(unittest.TestCase):
    """Tests for SplitEvent and SplitDestination audit types."""

    def test_constructs(self) -> None:
        destination = SplitDestination(
            new_recidiviz_id=_NEW_ID,
            external_ids=[
                ExternalId(
                    external_id="OLD9",
                    id_type=IdentifierType.US_OZ_LOTR_ID,
                    is_active=True,
                )
            ],
            attributes=[
                _sourced(Email(address="sam@fake.com", address_hash="hashsamfakecom"))
            ],
        )
        event = SplitEvent(
            original_id=_RECIDIVIZ_ID,
            trigger=SplitTrigger.SPLIT_ENDPOINT,
            requested_by="splitter@fake.com",
            timestamp_utc=_TS,
            destinations=[destination],
        )
        self.assertIs(SplitTrigger.SPLIT_ENDPOINT, event.trigger)
        self.assertEqual(_RECIDIVIZ_ID, event.original_id)
        self.assertEqual("splitter@fake.com", event.requested_by)
        self.assertEqual(1, len(event.destinations))
        self.assertEqual(_NEW_ID, event.destinations[0].new_recidiviz_id)
        self.assertEqual(1, len(event.destinations[0].external_ids))
        self.assertEqual(1, len(event.destinations[0].attributes))


class IdentityHistoryTest(unittest.TestCase):
    """Tests for IdentityHistory as a standalone container pairing an Identity with
    its audit events."""

    def _build(
        self,
        merge_events: list[MergeEvent] | None = None,
        split_events: list[SplitEvent] | None = None,
    ) -> IdentityHistory:
        return IdentityHistory(
            identity=_active_identity(),
            merge_events=merge_events or [],
            split_events=split_events or [],
        )

    def test_is_not_a_subclass_of_identity(self) -> None:
        identity_history = self._build()
        self.assertNotIsInstance(identity_history, Identity)

    def test_empty_history_constructs(self) -> None:
        identity_history = self._build()
        self.assertEqual([], identity_history.merge_events)
        self.assertEqual([], identity_history.split_events)

    def test_identity_field_is_accessible(self) -> None:
        identity_history = self._build()
        self.assertEqual(_RECIDIVIZ_ID, identity_history.identity.recidiviz_id)

    def test_with_merge_and_split_events(self) -> None:
        merge_event = MergeEvent(
            surviving_id=_RECIDIVIZ_ID,
            retired_id=_RETIRED_ID,
            trigger=MergeTrigger.IMPORT,
            requested_by=None,
            timestamp_utc=_TS,
            conflicts=[],
        )
        split_event = SplitEvent(
            original_id=_RECIDIVIZ_ID,
            trigger=SplitTrigger.SPLIT_ENDPOINT,
            requested_by=None,
            timestamp_utc=_TS,
            destinations=[],
        )
        identity_history = self._build(
            merge_events=[merge_event], split_events=[split_event]
        )
        self.assertEqual(1, len(identity_history.merge_events))
        self.assertEqual(1, len(identity_history.split_events))

    def test_identity_does_not_have_history_fields(self) -> None:
        """Plain Identity does not carry merge_events or split_events."""
        identity = _active_identity()
        self.assertFalse(hasattr(identity, "merge_events"))
        self.assertFalse(hasattr(identity, "split_events"))
