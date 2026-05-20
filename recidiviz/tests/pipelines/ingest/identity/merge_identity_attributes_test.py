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
"""Tests for merge_identity_attributes."""
import datetime
import unittest

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
    attribute_field_type_reference_for_class,
)
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import (
    EntityFieldIndex,
    EntityFieldType,
)
from recidiviz.persistence.entity.identity import entities as identity_entities_module
from recidiviz.persistence.entity.identity.entities import (
    IdentityAttributes,
    IdentityEmail,
    IdentityEthnicity,
    IdentityExternalId,
    IdentityFragment,
    IdentityGender,
    IdentityName,
    IdentityPhoneNumber,
    IdentityRace,
    IdentitySex,
)
from recidiviz.pipelines.ingest.identity.merge_identity_attributes import (
    merge_identity_attributes,
)

_TENANT = "US_XX"


def _fragment(
    *,
    name: IdentityName | None = None,
    birthdate: datetime.date | None = None,
    gender: IdentityGender | None = None,
    sex: IdentitySex | None = None,
    ethnicity: IdentityEthnicity | None = None,
    races: list[IdentityRace] | None = None,
    phone_numbers: list[IdentityPhoneNumber] | None = None,
    emails: list[IdentityEmail] | None = None,
) -> IdentityFragment:
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[IdentityExternalId(tenant=_TENANT, external_id="X", id_type="T")],
        attributes=IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            name=name,
            birthdate=birthdate,
            gender=gender,
            sex=sex,
            ethnicity=ethnicity,
            races=races or [],
            phone_numbers=phone_numbers or [],
            emails=emails or [],
        ),
    )


def _name(given: str, surname: str) -> IdentityName:
    return IdentityName(tenant=_TENANT, given_name=given, surname=surname)


def _gender(g: Gender) -> IdentityGender:
    return IdentityGender(tenant=_TENANT, gender=g)


def _sex(s: Sex) -> IdentitySex:
    return IdentitySex(tenant=_TENANT, sex=s)


def _ethnicity(e: Ethnicity) -> IdentityEthnicity:
    return IdentityEthnicity(tenant=_TENANT, ethnicity=e)


def _race(r: Race, raw: str | None = None) -> IdentityRace:
    return IdentityRace(tenant=_TENANT, race=r, race_raw_text=raw)


def _phone(number: str) -> IdentityPhoneNumber:
    return IdentityPhoneNumber(tenant=_TENANT, number=number)


def _email(address: str) -> IdentityEmail:
    return IdentityEmail(tenant=_TENANT, address=address)


def _field_index() -> EntityFieldIndex:
    return entities_module_context_for_module(identity_entities_module).field_index()


class TestMergeIdentityAttributes(unittest.TestCase):
    """Tests for merge_identity_attributes."""

    def test_single_fragment(self) -> None:
        name = _name("John", "Doe")
        dob = datetime.date(1990, 1, 1)
        fragment = _fragment(name=name, birthdate=dob)

        result = merge_identity_attributes([fragment], _field_index())

        self.assertEqual(result.name, name)
        self.assertEqual(result.birthdate, dob)

    def test_entity_sub_field_conflict_raises(self) -> None:
        fragment_a = _fragment(name=_name("John", "Doe"))
        fragment_b = _fragment(name=_name("Jonathan", "Doe"))

        with self.assertRaisesRegex(
            ValueError, "Conflicting non-None values for 'attributes.name.given_name'"
        ):
            merge_identity_attributes(
                [
                    fragment_a,
                    fragment_b,
                ],
                _field_index(),
            )

    def test_entity_sub_field_partial_merge(self) -> None:
        fragment_a = _fragment(name=_name("John", "Doe"))
        fragment_b = _fragment(
            name=IdentityName(tenant=_TENANT, middle_name="A"),
        )

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        assert result.name is not None
        self.assertEqual(result.name.given_name, "John")
        self.assertEqual(result.name.surname, "Doe")
        self.assertEqual(result.name.middle_name, "A")

    def test_entity_sub_field_agreement_keeps_value(self) -> None:
        fragment_a = _fragment(
            gender=IdentityGender(
                tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
            ),
        )
        fragment_b = _fragment(
            gender=IdentityGender(
                tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
            ),
        )

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        assert result.gender is not None
        self.assertEqual(result.gender.gender, Gender.MALE)
        self.assertEqual(result.gender.gender_raw_text, "M")

    def test_entity_sub_field_fills_in_raw_text(self) -> None:
        fragment_a = _fragment(
            gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
        )
        fragment_b = _fragment(
            gender=IdentityGender(
                tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"
            ),
        )

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        assert result.gender is not None
        self.assertEqual(result.gender.gender, Gender.MALE)
        self.assertEqual(result.gender.gender_raw_text, "M")

    def test_birthdate_conflict_raises(self) -> None:
        fragment_a = _fragment(birthdate=datetime.date(1990, 1, 1))
        fragment_b = _fragment(birthdate=datetime.date(1995, 6, 15))

        with self.assertRaisesRegex(
            ValueError, "Conflicting non-None values for 'attributes.birthdate'"
        ):
            merge_identity_attributes(
                [
                    fragment_a,
                    fragment_b,
                ],
                _field_index(),
            )

    def test_scalar_agreement_keeps_value(self) -> None:
        name = _name("John", "Doe")
        fragment_a = _fragment(name=name)
        fragment_b = _fragment(name=_name("John", "Doe"))

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(result.name, name)

    def test_scalar_keeps_values(self) -> None:
        name = _name("John", "Doe")
        dob = datetime.date(1990, 1, 1)

        fragment_with_values = _fragment(name=name, birthdate=dob)
        fragment_with_nones = _fragment(name=None, birthdate=None)

        result = merge_identity_attributes(
            [
                fragment_with_values,
                fragment_with_nones,
            ],
            _field_index(),
        )

        self.assertEqual(result.name, name)
        self.assertEqual(result.birthdate, dob)

    def test_non_conflicting_scalars_from_different_fragments(self) -> None:
        fragment_a = _fragment(name=_name("John", "Doe"))
        fragment_b = _fragment(birthdate=datetime.date(1990, 1, 1))

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(result.name, _name("John", "Doe"))
        self.assertEqual(result.birthdate, datetime.date(1990, 1, 1))

    def test_list_union(self) -> None:
        race_a = _race(Race.WHITE)
        race_b = _race(Race.BLACK)

        fragment_a = _fragment(races=[race_a])
        fragment_b = _fragment(races=[race_b])

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(len(result.races), 2)
        race_values = {r.race for r in result.races}
        self.assertEqual(race_values, {Race.WHITE, Race.BLACK})

    def test_list_dedup_same_race(self) -> None:
        race = _race(Race.WHITE, "W")

        fragment_a = _fragment(races=[race])
        fragment_b = _fragment(races=[_race(Race.WHITE, "W")])

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(len(result.races), 1)

    def test_list_dedup_same_enum_different_raw_text(self) -> None:
        fragment_a = _fragment(races=[_race(Race.WHITE, "W")])
        fragment_b = _fragment(races=[_race(Race.WHITE, "WHITE")])

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(len(result.races), 2)

    def test_phone_dedup(self) -> None:
        fragment_a = _fragment(phone_numbers=[_phone("5550100001")])
        fragment_b = _fragment(phone_numbers=[_phone("5550100001")])

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(len(result.phone_numbers), 1)

    def test_email_dedup(self) -> None:
        fragment_a = _fragment(emails=[_email("a@b.com")])
        fragment_b = _fragment(emails=[_email("a@b.com")])

        result = merge_identity_attributes(
            [
                fragment_a,
                fragment_b,
            ],
            _field_index(),
        )

        self.assertEqual(len(result.emails), 1)

    def test_list_element_types_have_no_forward_edges(self) -> None:
        """`_merge_entity` merges list-typed forward edges via dedup-by-equality,
        which only works when the list elements have no forward-edge children of
        their own."""
        field_index = _field_index()
        class_ref = attribute_field_type_reference_for_class(IdentityAttributes)
        for field_name in field_index.get_all_entity_fields(
            IdentityAttributes, EntityFieldType.FORWARD_EDGE
        ):
            if (
                attr_field_type_for_field_name(IdentityAttributes, field_name)
                == BuildableAttrFieldType.FORWARD_REF
            ):
                continue
            element_cls_name = class_ref.field_referenced_cls_name_for_field_name(
                field_name
            )
            assert element_cls_name is not None
            element_cls = getattr(identity_entities_module, element_cls_name)
            forward_edges = field_index.get_all_entity_fields(
                element_cls, EntityFieldType.FORWARD_EDGE
            )
            self.assertEqual(
                forward_edges,
                set(),
                f"List field IdentityAttributes.{field_name} has elements of "
                f"type {element_cls.__name__} with forward-edge children "
                f"{forward_edges}. Update `_merge_entity` to handle this case.",
            )

    def test_all_scalar_types(self) -> None:
        """Verify all scalar fields are unchanged after a merge."""
        fragment = _fragment(
            name=_name("John", "Doe"),
            birthdate=datetime.date(1990, 1, 1),
            gender=_gender(Gender.MALE),
            sex=_sex(Sex.MALE),
            ethnicity=_ethnicity(Ethnicity.NOT_HISPANIC),
        )

        field_index = _field_index()
        result = merge_identity_attributes([fragment], field_index)

        for field_name in field_index.get_all_entity_fields(
            IdentityAttributes, EntityFieldType.FLAT_FIELD
        ):
            before_merge = fragment.attributes.get_field(field_name)
            after_merge = result.get_field(field_name)
            self.assertEqual(
                after_merge,
                before_merge,
                f"Field {field_name!r} changed from {before_merge!r} to {after_merge!r}",
            )
