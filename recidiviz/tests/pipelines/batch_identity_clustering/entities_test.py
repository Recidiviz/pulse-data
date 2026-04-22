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
"""Unit tests for batch identity clustering pipeline entities."""
import datetime
import pickle
import unittest

from recidiviz.common.demographics import Gender, Race
from recidiviz.pipelines.batch_identity_clustering.entities import (
    IdentityFragment,
    IdentityFragmentEmail,
    IdentityFragmentExternalId,
    IdentityFragmentName,
    IdentityFragmentPhoneNumber,
    IdentityFragmentRace,
)

_EXTERNAL_ID = IdentityFragmentExternalId(
    external_id="EXT_001", id_type="US_OZ_ID_TYPE"
)
_NAME = IdentityFragmentName(
    given_name="John", surname="Doe", middle_name="Q", name_suffix="Jr"
)
_RACE = IdentityFragmentRace(race=Race.WHITE)
_PHONE = IdentityFragmentPhoneNumber(number="5550100001")
_EMAIL = IdentityFragmentEmail(address="john@example.com")


class TestIdentityFragmentExternalId(unittest.TestCase):
    """Tests the IdentityFragmentExternalId entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragmentExternalId(external_id="EXT_001", id_type="US_OZ_ID_TYPE"),
            IdentityFragmentExternalId(external_id="EXT_001", id_type="US_OZ_ID_TYPE"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityFragmentExternalId(external_id="EXT_001", id_type="US_OZ_ID_TYPE"),
            IdentityFragmentExternalId(external_id="EXT_002", id_type="US_OZ_ID_TYPE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EXTERNAL_ID, pickle.loads(pickle.dumps(_EXTERNAL_ID)))


class TestIdentityFragmentRace(unittest.TestCase):
    """Tests the IdentityFragmentRace entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragmentRace(race=Race.WHITE),
            IdentityFragmentRace(race=Race.WHITE),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityFragmentRace(race=Race.WHITE),
            IdentityFragmentRace(race=Race.BLACK),
        )

    def test_inequality_different_raw_text(self) -> None:
        self.assertNotEqual(
            IdentityFragmentRace(race=Race.WHITE, race_raw_text="W"),
            IdentityFragmentRace(race=Race.WHITE, race_raw_text="WHITE"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_RACE, pickle.loads(pickle.dumps(_RACE)))


class TestIdentityFragmentPhoneNumber(unittest.TestCase):
    """Tests the IdentityFragmentPhoneNumber entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragmentPhoneNumber(number="5550100001"),
            IdentityFragmentPhoneNumber(number="5550100001"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityFragmentPhoneNumber(number="5550100001"),
            IdentityFragmentPhoneNumber(number="5550199999"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_PHONE, pickle.loads(pickle.dumps(_PHONE)))


class TestIdentityFragmentEmail(unittest.TestCase):
    """Tests the IdentityFragmentEmail entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragmentEmail(address="a@b.com"),
            IdentityFragmentEmail(address="a@b.com"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityFragmentEmail(address="a@b.com"),
            IdentityFragmentEmail(address="c@b.com"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_EMAIL, pickle.loads(pickle.dumps(_EMAIL)))


class TestIdentityFragmentName(unittest.TestCase):
    """Tests the IdentityFragmentName entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragmentName(given_name="John", surname="Doe"),
            IdentityFragmentName(given_name="John", surname="Doe"),
        )

    def test_inequality(self) -> None:
        self.assertNotEqual(
            IdentityFragmentName(given_name="John", surname="Doe"),
            IdentityFragmentName(given_name="Jane", surname="Doe"),
        )

    def test_pickle_roundtrip(self) -> None:
        self.assertEqual(_NAME, pickle.loads(pickle.dumps(_NAME)))


class TestIdentityFragment(unittest.TestCase):
    """Tests the IdentityFragment root entity."""

    def test_equality(self) -> None:
        self.assertEqual(
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[
                    IdentityFragmentExternalId(
                        external_id="EXT_001", id_type="US_OZ_ID_TYPE"
                    )
                ],
                name=_NAME,
                birthdate=datetime.date(1990, 1, 1),
                gender=Gender.MALE,
                races=[
                    IdentityFragmentRace(race=Race.WHITE),
                    IdentityFragmentRace(race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE),
                ],
                phone_numbers=[IdentityFragmentPhoneNumber(number="5550100001")],
                emails=[IdentityFragmentEmail(address="john@example.com")],
            ),
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[
                    IdentityFragmentExternalId(
                        external_id="EXT_001", id_type="US_OZ_ID_TYPE"
                    )
                ],
                name=_NAME,
                birthdate=datetime.date(1990, 1, 1),
                gender=Gender.MALE,
                races=[
                    IdentityFragmentRace(race=Race.WHITE),
                    IdentityFragmentRace(race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE),
                ],
                phone_numbers=[IdentityFragmentPhoneNumber(number="5550100001")],
                emails=[IdentityFragmentEmail(address="john@example.com")],
            ),
        )

    def test_inequality_different_external_id(self) -> None:
        self.assertNotEqual(
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[
                    IdentityFragmentExternalId(
                        external_id="EXT_001", id_type="US_OZ_ID_TYPE"
                    )
                ],
            ),
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[
                    IdentityFragmentExternalId(
                        external_id="EXT_002", id_type="US_OZ_ID_TYPE"
                    )
                ],
            ),
        )

    def test_inequality_different_name(self) -> None:
        self.assertNotEqual(
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[_EXTERNAL_ID],
                name=IdentityFragmentName(given_name="John", surname="Doe"),
            ),
            IdentityFragment(
                tenant="US_OZ",
                external_ids=[_EXTERNAL_ID],
                name=IdentityFragmentName(given_name="Jane", surname="Doe"),
            ),
        )

    def test_defaults(self) -> None:
        fragment = IdentityFragment(tenant="US_OZ", external_ids=[_EXTERNAL_ID])
        self.assertEqual(fragment.tenant, "US_OZ")
        self.assertIsNone(fragment.name)
        self.assertIsNone(fragment.birthdate)
        self.assertIsNone(fragment.gender)
        self.assertIsNone(fragment.gender_raw_text)
        self.assertEqual(fragment.races, [])
        self.assertEqual(fragment.phone_numbers, [])
        self.assertEqual(fragment.emails, [])

    def test_pickle_roundtrip(self) -> None:
        fragment = IdentityFragment(
            tenant="US_OZ",
            external_ids=[
                IdentityFragmentExternalId(
                    external_id="EXT_001", id_type="US_OZ_ID_TYPE"
                )
            ],
            name=_NAME,
            birthdate=datetime.date(1990, 1, 1),
            gender=Gender.MALE,
            races=[
                IdentityFragmentRace(race=Race.WHITE),
                IdentityFragmentRace(race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE),
            ],
            phone_numbers=[IdentityFragmentPhoneNumber(number="5550100001")],
            emails=[IdentityFragmentEmail(address="john@example.com")],
        )
        self.assertEqual(fragment, pickle.loads(pickle.dumps(fragment)))
