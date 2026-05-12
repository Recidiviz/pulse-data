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
"""Tests for identity ingest pipeline entity_factories.py."""
import unittest
from typing import Set

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.persistence.entity.batch_identity_clustering import (
    entities,
    entity_factories,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_all_entity_factory_classes_in_module,
)

_TENANT = "US_OZ"


class TestEntityFactories(unittest.TestCase):
    """Tests for identity ingest pipeline entity_factories.py."""

    def test_factories_defined_for_all_classes(self) -> None:
        """Tests that an entity factory has been added for every entity."""
        factory_classes = get_all_entity_factory_classes_in_module(entity_factories)
        found_entity_class_names: Set[str] = set()
        for factory_class in factory_classes:
            entity_class_name = factory_class.__name__[: -len("Factory")]
            deserialize_return_type = factory_class.deserialize.__annotations__[
                "return"
            ]

            self.assertEqual(entity_class_name, deserialize_return_type.__name__)
            self.assertNotIn(entity_class_name, found_entity_class_names)
            found_entity_class_names.add(entity_class_name)

        entity_classes = get_all_entity_classes_in_module(entities)
        expected_entity_class_names = {c.__name__ for c in entity_classes}

        extra_classes = found_entity_class_names.difference(expected_entity_class_names)
        self.assertEqual(set(), extra_classes)
        missing_classes = expected_entity_class_names.difference(
            found_entity_class_names
        )
        self.assertEqual(set(), missing_classes)

    def test_has_tests_for_all_factories(self) -> None:
        """Tests that a unittest has been added to this class for every expected entity
        factory.
        """
        test_names = {t for t in dir(self) if t.startswith("test")}

        expected_tests = {
            f"test_deserialize_{entity_cls.__name__}"
            for entity_cls in get_all_entity_classes_in_module(entities)
        }

        missing_tests = expected_tests.difference(test_names)
        if missing_tests:
            self.fail(f"Found missing expected tests: {missing_tests}")

    def test_deserialize_IdentityExternalId(self) -> None:
        result = entity_factories.IdentityExternalIdFactory.deserialize(
            tenant=_TENANT,
            external_id="EXT_001",
            id_type="US_OZ_ID_TYPE",
        )

        expected = entities.IdentityExternalId(
            tenant=_TENANT,
            external_id="EXT_001",
            id_type="US_OZ_ID_TYPE",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityName(self) -> None:
        result = entity_factories.IdentityNameFactory.deserialize(
            tenant=_TENANT,
            given_name="John",
            preferred_name="Johnny",
            surname="Doe",
            middle_name="Q",
            name_suffix="Jr",
        )

        expected = entities.IdentityName(
            tenant=_TENANT,
            given_name="JOHN",
            preferred_name="JOHNNY",
            surname="DOE",
            middle_name="Q",
            name_suffix="JR",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityGender(self) -> None:
        result = entity_factories.IdentityGenderFactory.deserialize(
            tenant=_TENANT,
            gender=Gender.MALE,
            gender_raw_text="M",
        )

        expected = entities.IdentityGender(
            tenant=_TENANT,
            gender=Gender.MALE,
            gender_raw_text="M",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentitySex(self) -> None:
        result = entity_factories.IdentitySexFactory.deserialize(
            tenant=_TENANT,
            sex=Sex.MALE,
            sex_raw_text="M",
        )

        expected = entities.IdentitySex(
            tenant=_TENANT,
            sex=Sex.MALE,
            sex_raw_text="M",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityRace(self) -> None:
        result = entity_factories.IdentityRaceFactory.deserialize(
            tenant=_TENANT,
            race=Race.WHITE,
            race_raw_text="W",
        )

        expected = entities.IdentityRace(
            tenant=_TENANT,
            race=Race.WHITE,
            race_raw_text="W",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityEthnicity(self) -> None:
        result = entity_factories.IdentityEthnicityFactory.deserialize(
            tenant=_TENANT,
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="H",
        )

        expected = entities.IdentityEthnicity(
            tenant=_TENANT,
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="H",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityPhoneNumber(self) -> None:
        result = entity_factories.IdentityPhoneNumberFactory.deserialize(
            tenant=_TENANT,
            number="5550100001",
        )

        expected = entities.IdentityPhoneNumber(
            tenant=_TENANT,
            number="5550100001",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityEmail(self) -> None:
        result = entity_factories.IdentityEmailFactory.deserialize(
            tenant=_TENANT,
            address="john@example.com",
        )

        expected = entities.IdentityEmail(
            tenant=_TENANT,
            address="JOHN@EXAMPLE.COM",
        )

        self.assertEqual(expected, result)

    def test_deserialize_IdentityAttributes(self) -> None:
        result = entity_factories.IdentityAttributesFactory.deserialize(
            tenant=_TENANT,
            person_type=PersonType.JII,
        )

        expected = entities.IdentityAttributes(
            tenant=_TENANT, person_type=PersonType.JII
        )
        self.assertEqual(expected, result)

    def test_deserialize_IdentityFragment(self) -> None:
        attributes = entities.IdentityAttributes(
            tenant=_TENANT, person_type=PersonType.JII
        )
        result = entity_factories.IdentityFragmentFactory.deserialize(
            tenant=_TENANT,
            attributes=attributes,  # type: ignore[arg-type]
        )

        expected = entities.IdentityFragment(
            tenant=_TENANT,
            external_ids=[],
            attributes=attributes,
        )
        self.assertEqual(expected, result)
