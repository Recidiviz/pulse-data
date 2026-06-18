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
"""Tests the entities defined in identity_fragment_entities.py."""
import datetime
import pickle
import unittest

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
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

_TENANT = Tenant.US_XX

_EXTERNAL_ID = IdentityExternalId(
    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
)
_NAME = IdentityName(
    tenant=_TENANT, given_name="John", surname="Doe", middle_name="Q", name_suffix="Jr"
)
_GENDER = IdentityGender(tenant=_TENANT, gender=Gender.MALE)
_SEX = IdentitySex(tenant=_TENANT, sex=Sex.MALE)
_RACE = IdentityRace(tenant=_TENANT, race=Race.WHITE)
_ETHNICITY = IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC)
_PHONE = IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
_EMAIL = IdentityEmail(tenant=_TENANT, address="john@example.com")


class TestIdentityExternalId(unittest.TestCase):
    """Tests the IdentityExternalId entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
            IdentityExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityExternalId(
                tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
            ),
            IdentityExternalId(
                tenant=_TENANT, external_id="EXT_002", id_type="US_XX_ID_TYPE"
            ),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EXTERNAL_ID, pickle.loads(pickle.dumps(_EXTERNAL_ID)))


class TestIdentityName(unittest.TestCase):
    """Tests the IdentityName entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
            IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
            IdentityName(tenant=_TENANT, given_name="Jane", surname="Doe"),
        )

    def test_inequality_different_preferred_name(self) -> None:
        self.assertNotEqual(
            IdentityName(tenant=_TENANT, given_name="John", preferred_name="Johnny"),
            IdentityName(tenant=_TENANT, given_name="John", preferred_name="JJ"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_NAME, pickle.loads(pickle.dumps(_NAME)))

    def test_rejects_digit_in_given_name(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityName(tenant=_TENANT, given_name="J0hn", surname="Doe")

    def test_rejects_digit_in_surname(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityName(tenant=_TENANT, given_name="John", surname="Do3")

    def test_rejects_digit_in_middle_name(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityName(
                tenant=_TENANT, given_name="John", surname="Doe", middle_name="Q2"
            )

    def test_rejects_digit_in_preferred_name(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            IdentityName(
                tenant=_TENANT,
                given_name="John",
                surname="Doe",
                preferred_name="Johnny5",
            )

    def test_allows_numeric_name_suffix(self) -> None:
        name = IdentityName(
            tenant=_TENANT, given_name="John", surname="Doe", name_suffix="3rd"
        )
        self.assertEqual(name.name_suffix, "3rd")

    def test_allows_bare_digit_name_suffix(self) -> None:
        name = IdentityName(
            tenant=_TENANT, given_name="John", surname="Smith", name_suffix="2"
        )
        self.assertEqual(name.name_suffix, "2")

    def test_rejects_overlong_name_suffix(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must be no more than"):
            IdentityName(
                tenant=_TENANT,
                given_name="John",
                surname="Doe",
                name_suffix="Father of John",
            )

    def test_rejects_garbage_name_suffix(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"must contain only letters, digits, periods, commas, hyphens, "
            r"and whitespace",
        ):
            IdentityName(
                tenant=_TENANT,
                given_name="John",
                surname="Doe",
                name_suffix="Jr<br>",
            )


class TestIdentityGender(unittest.TestCase):
    """Tests the IdentityGender entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityGender(tenant=_TENANT, gender=Gender.MALE),
            IdentityGender(tenant=_TENANT, gender=Gender.MALE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityGender(tenant=_TENANT, gender=Gender.MALE),
            IdentityGender(tenant=_TENANT, gender=Gender.FEMALE),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityGender(tenant=_TENANT, gender=Gender.MALE, gender_raw_text="M"),
            IdentityGender(tenant=_TENANT, gender=Gender.MALE, gender_raw_text="MALE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_GENDER, pickle.loads(pickle.dumps(_GENDER)))


class TestIdentitySex(unittest.TestCase):
    """Tests the IdentitySex entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentitySex(tenant=_TENANT, sex=Sex.MALE),
            IdentitySex(tenant=_TENANT, sex=Sex.MALE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentitySex(tenant=_TENANT, sex=Sex.MALE),
            IdentitySex(tenant=_TENANT, sex=Sex.FEMALE),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentitySex(tenant=_TENANT, sex=Sex.MALE, sex_raw_text="M"),
            IdentitySex(tenant=_TENANT, sex=Sex.MALE, sex_raw_text="MALE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_SEX, pickle.loads(pickle.dumps(_SEX)))


class TestIdentityRace(unittest.TestCase):
    """Tests the IdentityRace entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityRace(tenant=_TENANT, race=Race.WHITE),
            IdentityRace(tenant=_TENANT, race=Race.WHITE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityRace(tenant=_TENANT, race=Race.WHITE),
            IdentityRace(tenant=_TENANT, race=Race.BLACK),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityRace(tenant=_TENANT, race=Race.WHITE, race_raw_text="W"),
            IdentityRace(tenant=_TENANT, race=Race.WHITE, race_raw_text="WHITE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_RACE, pickle.loads(pickle.dumps(_RACE)))


class TestIdentityEthnicity(unittest.TestCase):
    """Tests the IdentityEthnicity entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.NOT_HISPANIC),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityEthnicity(
                tenant=_TENANT, ethnicity=Ethnicity.HISPANIC, ethnicity_raw_text="H"
            ),
            IdentityEthnicity(
                tenant=_TENANT,
                ethnicity=Ethnicity.HISPANIC,
                ethnicity_raw_text="HISPANIC",
            ),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_ETHNICITY, pickle.loads(pickle.dumps(_ETHNICITY)))


class TestIdentityPhoneNumber(unittest.TestCase):
    """Tests the IdentityPhoneNumber entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityPhoneNumber(tenant=_TENANT, number="5550100001"),
            IdentityPhoneNumber(tenant=_TENANT, number="5550100001"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityPhoneNumber(tenant=_TENANT, number="5550100001"),
            IdentityPhoneNumber(tenant=_TENANT, number="5550199999"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_PHONE, pickle.loads(pickle.dumps(_PHONE)))


class TestIdentityEmail(unittest.TestCase):
    """Tests the IdentityEmail entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityEmail(tenant=_TENANT, address="a@b.com"),
            IdentityEmail(tenant=_TENANT, address="a@b.com"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityEmail(tenant=_TENANT, address="a@b.com"),
            IdentityEmail(tenant=_TENANT, address="c@b.com"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EMAIL, pickle.loads(pickle.dumps(_EMAIL)))


class TestIdentityAttributes(unittest.TestCase):
    """Tests the IdentityAttributes entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                name=_NAME,
                birthdate=datetime.date(1990, 1, 1),
                gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
                sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE),
                races=[IdentityRace(tenant=_TENANT, race=Race.WHITE)],
                ethnicity=IdentityEthnicity(
                    tenant=_TENANT, ethnicity=Ethnicity.HISPANIC
                ),
                phone_numbers=[
                    IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
                ],
                emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
            ),
            IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                name=_NAME,
                birthdate=datetime.date(1990, 1, 1),
                gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
                sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE),
                races=[IdentityRace(tenant=_TENANT, race=Race.WHITE)],
                ethnicity=IdentityEthnicity(
                    tenant=_TENANT, ethnicity=Ethnicity.HISPANIC
                ),
                phone_numbers=[
                    IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
                ],
                emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
            ),
        )

    def test_inequality_different_birthdate(self) -> None:
        self.assertNotEqual(
            IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                birthdate=datetime.date(1990, 1, 1),
            ),
            IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                birthdate=datetime.date(1991, 1, 1),
            ),
        )

    def test_defaults(self) -> None:
        attrs = IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            birthdate=datetime.date(1990, 1, 1),
        )
        self.assertIsNone(attrs.name)
        self.assertIsNone(attrs.gender)
        self.assertIsNone(attrs.sex)
        self.assertEqual(attrs.races, [])
        self.assertIsNone(attrs.ethnicity)
        self.assertEqual(attrs.phone_numbers, [])
        self.assertEqual(attrs.emails, [])

    def test_construction_raises_when_no_attributes_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "must have at least one attribute set beyond"
        ):
            IdentityAttributes(tenant=_TENANT, person_type=PersonType.JII)

    def test_construction_succeeds_for_each_scalar(self) -> None:
        cases: list[tuple[str, dict]] = [
            ("name", {"name": _NAME}),
            ("birthdate", {"birthdate": datetime.date(1990, 1, 1)}),
            ("gender", {"gender": _GENDER}),
            ("sex", {"sex": _SEX}),
            ("ethnicity", {"ethnicity": _ETHNICITY}),
        ]
        for field_name, kwargs in cases:
            with self.subTest(field=field_name):
                attrs = IdentityAttributes(
                    tenant=_TENANT, person_type=PersonType.JII, **kwargs
                )
                self.assertEqual(getattr(attrs, field_name), kwargs[field_name])

    def test_construction_succeeds_for_each_list(self) -> None:
        cases: list[tuple[str, dict]] = [
            ("races", {"races": [_RACE]}),
            ("phone_numbers", {"phone_numbers": [_PHONE]}),
            ("emails", {"emails": [_EMAIL]}),
        ]
        for field_name, kwargs in cases:
            with self.subTest(field=field_name):
                attrs = IdentityAttributes(
                    tenant=_TENANT, person_type=PersonType.JII, **kwargs
                )
                self.assertEqual(getattr(attrs, field_name), kwargs[field_name])

    def test_pickle_roundtrip(self) -> None:
        attrs = IdentityAttributes(
            tenant=_TENANT,
            person_type=PersonType.JII,
            name=_NAME,
            birthdate=datetime.date(1990, 1, 1),
            gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
            sex=IdentitySex(tenant=_TENANT, sex=Sex.MALE),
            races=[IdentityRace(tenant=_TENANT, race=Race.WHITE)],
            ethnicity=IdentityEthnicity(tenant=_TENANT, ethnicity=Ethnicity.HISPANIC),
            phone_numbers=[IdentityPhoneNumber(tenant=_TENANT, number="5550100001")],
            emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
        )
        self.assertEqual(attrs, pickle.loads(pickle.dumps(attrs)))


class TestIdentityFragment(unittest.TestCase):
    """Tests the IdentityFragment root entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[
                    IdentityExternalId(
                        tenant=_TENANT,
                        external_id="EXT_001",
                        id_type="US_XX_ID_TYPE",
                    )
                ],
                attributes=IdentityAttributes(
                    tenant=_TENANT,
                    person_type=PersonType.JII,
                    name=_NAME,
                    birthdate=datetime.date(1990, 1, 1),
                    gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
                    races=[
                        IdentityRace(tenant=_TENANT, race=Race.WHITE),
                        IdentityRace(
                            tenant=_TENANT,
                            race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                        ),
                    ],
                    phone_numbers=[
                        IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
                    ],
                    emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
                ),
            ),
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[
                    IdentityExternalId(
                        tenant=_TENANT,
                        external_id="EXT_001",
                        id_type="US_XX_ID_TYPE",
                    )
                ],
                attributes=IdentityAttributes(
                    tenant=_TENANT,
                    person_type=PersonType.JII,
                    name=_NAME,
                    birthdate=datetime.date(1990, 1, 1),
                    gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
                    races=[
                        IdentityRace(tenant=_TENANT, race=Race.WHITE),
                        IdentityRace(
                            tenant=_TENANT,
                            race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                        ),
                    ],
                    phone_numbers=[
                        IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
                    ],
                    emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
                ),
            ),
        )

    def test_inequality_different_external_id(self) -> None:
        self.assertNotEqual(
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[
                    IdentityExternalId(
                        tenant=_TENANT,
                        external_id="EXT_001",
                        id_type="US_XX_ID_TYPE",
                    )
                ],
            ),
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[
                    IdentityExternalId(
                        tenant=_TENANT,
                        external_id="EXT_002",
                        id_type="US_XX_ID_TYPE",
                    )
                ],
            ),
        )

    def test_inequality_different_name(self) -> None:
        self.assertNotEqual(
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[_EXTERNAL_ID],
                attributes=IdentityAttributes(
                    tenant=_TENANT,
                    person_type=PersonType.JII,
                    name=IdentityName(tenant=_TENANT, given_name="John", surname="Doe"),
                ),
            ),
            IdentityFragment(
                tenant=_TENANT,
                external_ids=[_EXTERNAL_ID],
                attributes=IdentityAttributes(
                    tenant=_TENANT,
                    person_type=PersonType.JII,
                    name=IdentityName(tenant=_TENANT, given_name="Jane", surname="Doe"),
                ),
            ),
        )

    def test_defaults(self) -> None:
        fragment = IdentityFragment(
            tenant=_TENANT,
            external_ids=[_EXTERNAL_ID],
        )
        self.assertEqual(fragment.tenant, _TENANT)
        self.assertIsNone(fragment.attributes)

    def test_pickle_roundtrip(self) -> None:
        fragment = IdentityFragment(
            tenant=_TENANT,
            external_ids=[
                IdentityExternalId(
                    tenant=_TENANT, external_id="EXT_001", id_type="US_XX_ID_TYPE"
                )
            ],
            attributes=IdentityAttributes(
                tenant=_TENANT,
                person_type=PersonType.JII,
                name=_NAME,
                birthdate=datetime.date(1990, 1, 1),
                gender=IdentityGender(tenant=_TENANT, gender=Gender.MALE),
                races=[
                    IdentityRace(tenant=_TENANT, race=Race.WHITE),
                    IdentityRace(
                        tenant=_TENANT, race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE
                    ),
                ],
                phone_numbers=[
                    IdentityPhoneNumber(tenant=_TENANT, number="5550100001")
                ],
                emails=[IdentityEmail(tenant=_TENANT, address="john@example.com")],
            ),
        )
        self.assertEqual(fragment, pickle.loads(pickle.dumps(fragment)))
