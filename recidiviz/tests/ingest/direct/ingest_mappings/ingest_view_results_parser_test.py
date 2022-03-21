# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for IngestViewFileParser."""
import csv
import datetime
import os
import unittest
from enum import Enum
from typing import Callable, Dict, List, Optional, Type

import jsonschema

from recidiviz.common.constants.enum_parser import EnumParsingError
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser import (
    IngestViewResultsParser,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    EntityT,
    entity_deserialize,
)
from recidiviz.tests.ingest.direct.ingest_mappings.fixtures.ingest_view_file_parser import (
    custom_python,
    ingest_view_files,
    manifests,
)
from recidiviz.tests.ingest.direct.ingest_mappings.fixtures.ingest_view_file_parser.fake_schema.entities import (
    FakeAgent,
    FakeCharge,
    FakeGender,
    FakePerson,
    FakePersonAlias,
    FakePersonExternalId,
    FakePersonRace,
    FakeRace,
    FakeSentence,
)

#### Start Fake Schema Factories ####


class FakePersonExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakePersonExternalId:
        return entity_deserialize(
            cls=FakePersonExternalId, converter_overrides={}, defaults={}, **kwargs
        )


class FakePersonFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakePerson:
        return entity_deserialize(
            cls=FakePerson, converter_overrides={}, defaults={}, **kwargs
        )


class FakePersonAliasFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakePersonAlias:
        return entity_deserialize(
            cls=FakePersonAlias, converter_overrides={}, defaults={}, **kwargs
        )


class FakePersonRaceFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakePersonRace:
        return entity_deserialize(
            cls=FakePersonRace, converter_overrides={}, defaults={}, **kwargs
        )


class FakeAgentFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakeAgent:
        return entity_deserialize(
            cls=FakeAgent, converter_overrides={}, defaults={}, **kwargs
        )


class FakeSentenceFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakeSentence:
        return entity_deserialize(
            cls=FakeSentence, converter_overrides={}, defaults={}, **kwargs
        )


class FakeChargeFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: DeserializableEntityFieldValue) -> FakeCharge:
        return entity_deserialize(
            cls=FakeCharge, converter_overrides={}, defaults={}, **kwargs
        )


#### End Fake Schema Factories ####


class FakeSchemaIngestViewResultsParserDelegate(IngestViewResultsParserDelegate):
    """Fake implementation of IngestViewFileParserDelegate for parser unittests."""

    def __init__(self, ingest_instance: DirectIngestInstance, is_production: bool):
        self.ingest_instance = ingest_instance
        self.is_production = is_production

    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        return os.path.join(
            os.path.dirname(manifests.__file__), f"{ingest_view_name}.yaml"
        )

    def get_common_args(self) -> Dict[str, DeserializableEntityFieldValue]:
        return {"fake_state_code": StateCode.US_XX.value}

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        if entity_cls_name == FakePerson.__name__:
            return FakePersonFactory
        if entity_cls_name == FakePersonExternalId.__name__:
            return FakePersonExternalIdFactory
        if entity_cls_name == FakePersonAlias.__name__:
            return FakePersonAliasFactory
        if entity_cls_name == FakePersonRace.__name__:
            return FakePersonRaceFactory
        if entity_cls_name == FakeAgent.__name__:
            return FakeAgentFactory
        if entity_cls_name == FakeSentence.__name__:
            return FakeSentenceFactory
        if entity_cls_name == FakeCharge.__name__:
            return FakeChargeFactory
        raise ValueError(f"Unexpected class name [{entity_cls_name}]")

    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        if entity_cls_name == FakePerson.__name__:
            return FakePerson
        if entity_cls_name == FakePersonExternalId.__name__:
            return FakePersonExternalId
        if entity_cls_name == FakePersonAlias.__name__:
            return FakePersonAlias
        if entity_cls_name == FakePersonRace.__name__:
            return FakePersonRace
        if entity_cls_name == FakeAgent.__name__:
            return FakeAgent
        if entity_cls_name == FakeSentence.__name__:
            return FakeSentence
        if entity_cls_name == FakeCharge.__name__:
            return FakeCharge
        raise ValueError(f"Unexpected class name [{entity_cls_name}]")

    def get_enum_cls(self, enum_cls_name: str) -> Type[Enum]:
        if enum_cls_name == FakeRace.__name__:
            return FakeRace
        if enum_cls_name == FakeGender.__name__:
            return FakeGender
        raise ValueError(f"Unexpected class name [{enum_cls_name}]")

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        return CustomFunctionRegistry(custom_functions_root_module=custom_python)

    def get_env_property(self, property_name: str) -> bool:
        if property_name == "test_is_production":
            return self.is_production
        if property_name == "test_is_primary_instance":
            return self.ingest_instance == DirectIngestInstance.PRIMARY
        raise ValueError(f"Unexpected test env property: {property_name}")

    def get_filter_predicate(
        self, entity_cls: Type[EntityT]
    ) -> Optional[Callable[[EntityT], bool]]:
        if issubclass(entity_cls, FakePersonAlias):

            def fake_person_alias_filter_predicate(e: EntityT) -> bool:
                return getattr(e, "full_name") is None

            return fake_person_alias_filter_predicate
        return None


class IngestViewFileParserTest(unittest.TestCase):
    """Tests for IngestViewFileParser."""

    @staticmethod
    def _run_parse_for_ingest_view(
        ingest_view_name: str,
        ingest_instance: DirectIngestInstance = DirectIngestInstance.SECONDARY,
        is_production: bool = False,
    ) -> List[Entity]:
        """Runs a single parsing test for a fixture ingest view with the given name,
        returning the parsed entities.
        """
        parser = IngestViewResultsParser(
            FakeSchemaIngestViewResultsParserDelegate(ingest_instance, is_production)
        )
        contents_handle = LocalFileContentsHandle(
            os.path.join(
                os.path.dirname(ingest_view_files.__file__),
                f"{ingest_view_name}.csv",
            ),
            # This is a fixture file checked into our codebase - do not delete it when
            # we are done with this contents handle.
            cleanup_file=False,
        )

        return parser.parse(
            ingest_view_name=ingest_view_name,
            contents_iterator=csv.DictReader(contents_handle.get_contents_iterator()),
        )

    def _run_parse_manifest_for_ingest_view(
        self,
        ingest_view_name: str,
        ingest_instance: DirectIngestInstance = DirectIngestInstance.SECONDARY,
        is_production: bool = False,
    ) -> EntityTreeManifest:
        delegate = FakeSchemaIngestViewResultsParserDelegate(
            ingest_instance, is_production
        )
        parser = IngestViewResultsParser(delegate)
        manifest_ast, _ = parser.parse_manifest(
            manifest_path=delegate.get_ingest_view_manifest_path(ingest_view_name),
        )
        return manifest_ast

    def test_simple_output(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                birthdate=datetime.date(1962, 1, 29),
                external_ids=[],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                birthdate=datetime.date(1954, 4, 29),
                external_ids=[],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="COSMOS KRAMER",
                external_ids=[],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_person")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_simple_non_person_output(self) -> None:
        # Arrange
        expected_output = [
            FakeAgent(
                external_id="A123",
                fake_state_code="US_XX",
                name="NEWMAN",
            ),
            FakeAgent(
                external_id="B456",
                fake_state_code="US_XX",
                name="STEINBRENNER",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_agent")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_list_field(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="ABC123",
                        id_type="A",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="123ABC",
                        id_type="B",
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="XYZ987",
                        id_type="A",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="111000",
                        id_type="C",
                    ),
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("list_field")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_field(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ALICE",
                birthdate=datetime.date(1962, 1, 29),
                is_dead=True,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="BOB",
                birthdate=datetime.date(1954, 4, 29),
                is_dead=False,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="CHARLIE",
                is_dead=False,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("boolean_field")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_unpack_list_into_field(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="123",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="456",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="789",
                        id_type="US_XX_ID_TYPE",
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="999",
                        id_type="US_XX_ID_TYPE",
                    )
                ],
            ),
            FakePerson(fake_state_code="US_XX", name="COSMOS KRAMER", external_ids=[]),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("list_field_from_list_col")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_unpack_multiple_lists_into_field(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="123",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="456",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="789",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="111",
                        id_type="US_XX_DOC_ID",
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="999",
                        id_type="US_XX_ID_TYPE",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="222",
                        id_type="US_XX_DOC_ID",
                    ),
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="333",
                        id_type="US_XX_DOC_ID",
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "list_field_from_multiple_list_col"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_non_list_child_field(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                external_ids=[],
                current_officer=FakeAgent(
                    external_id="A123",
                    fake_state_code="US_XX",
                    name="NEWMAN",
                ),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                external_ids=[],
                current_officer=FakeAgent(
                    external_id="B456",
                    fake_state_code="US_XX",
                    name="STEINBRENNER",
                ),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="COSMOS KRAMER",
                external_ids=[],
                current_officer=FakeAgent(
                    external_id="D000",
                    fake_state_code="US_XX",
                ),
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("single_child_field")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_reuse_column(self) -> None:
        """Tests that you can reuse a column value in multiple fields."""
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="123", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(fake_state_code="US_XX", full_name="ELAINE BENES")
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="456", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(fake_state_code="US_XX", full_name="JERRY SEINFELD")
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="COSMOS KRAMER",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="789", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(fake_state_code="US_XX", full_name="COSMOS KRAMER")
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("reused_column")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_simple_enum_parsing(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MA",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="2",
                        id_type="ID_TYPE",
                    )
                ],
            ),
            # No parsed gender for this person because the gender is in the ignores list.
            # Gender raw text is still hydrated.
            FakePerson(
                fake_state_code="US_XX",
                gender=None,
                gender_raw_text="U",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            # No gender for this person because they had a null gender in the input CSV.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_enums")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_simple_enum_parsing_no_ignores(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            # No gender for this person because they had a null gender in the input CSV.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_enums_no_ignores")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_parsing_complex_capitalization(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="FEMALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.TRANS_FEMALE,
                gender_raw_text="TRANS-FEMALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender_raw_text="X",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=None,
                gender_raw_text="!",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enums_complex_capitalization")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_simple_enum_literal(self) -> None:
        expected_output = [
            FakePerson(fake_state_code="US_XX", name="ANNA", gender=FakeGender.FEMALE),
            FakePerson(fake_state_code="US_XX", name="JULIA", gender=FakeGender.FEMALE),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_enum_literal")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_literal_in_conditional(self) -> None:
        expected_output = [
            FakePerson(fake_state_code="US_XX", name="ANNA", gender=FakeGender.FEMALE),
            FakePerson(fake_state_code="US_XX", name="COLIN", gender=FakeGender.MALE),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enum_literal_in_conditional")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_parsing_ignores_caps_mismatch(self) -> None:
        # Act
        with self.assertRaisesRegex(
            EnumParsingError, "Could not parse X when building <enum 'FakeGender'>"
        ):
            _ = self._run_parse_for_ingest_view("enums_ignores_caps_mismatch")

    def test_enum_parsing_mappings_caps_mismatch(self) -> None:
        # Act
        with self.assertRaisesRegex(
            # The expected FakeGender value is 'Male' and case must match
            EnumParsingError,
            "Could not parse MALE when building <enum 'FakeGender'>",
        ):
            _ = self._run_parse_for_ingest_view("enums_mappings_caps_mismatch")

    def test_simple_enum_entity_parsing(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.WHITE, race_raw_text="W"
                    )
                ],
            ),
            # This person had a race value in the ignores list so no FakePersonRace
            # object is hydrated.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            # This person had a null race value so no FakePersonRace object is hydrated.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_enum_entity")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_entity_list_parsing(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.ASIAN, race_raw_text="A"
                    ),
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.WHITE, race_raw_text="W"
                    )
                ],
            ),
            # This person had one race value in the ignores list so no FakePersonRace
            # object is hydrated for that value
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    )
                ],
            ),
            # This person had a null races value so no FakePersonRace objects are
            # hydrated.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enum_entity_list")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_custom_parser_parsing(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.WHITE, race_raw_text="B"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="W"
                    )
                ],
            ),
            # This person had a race value in the ignores list so no FakePersonRace
            # object is hydrated.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            # This person had a null race value so no FakePersonRace object is hydrated.
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enum_custom_parser")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_custom_parser_concatenated_values_parsing(self) -> None:
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX",
                        race=FakeRace.BLACK,
                        # Value is sent to uppercase before it is stored in raw text.
                        race_raw_text="B$$X",
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX",
                        race=FakeRace.WHITE,
                        race_raw_text="W$$Y",
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "enum_custom_parser_concatenated_raw"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_custom_parser_bad_return_type(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected manifest node type: \[<enum 'FakeRace'>\]. "
            r"Expected result_type: \[<enum 'FakeGender'>\].",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "enum_custom_parser_bad_return_type"
            )

    def test_enum_custom_parser_bad_arg_name(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"Found extra, unexpected arguments for function "
            r"\[fake_custom_enum_parsers.enum_parser_bad_arg_name\] in module \[[a-z_\.]+\]: {'bad_arg'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "enum_custom_parser_bad_arg_name"
            )

    def test_enum_custom_parser_extra_arg(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"Found extra, unexpected arguments for function "
            r"\[fake_custom_enum_parsers.enum_parser_extra_arg\] in module \[[a-z_\.]+\]: {'another_arg'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view("enum_custom_parser_extra_arg")

    def test_enum_custom_parser_missing_arg(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"Missing expected arguments for function \[fake_custom_enum_parsers.enum_parser_missing_arg\] "
            r"in module \[[a-z_\.]+\]: {'raw_text'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "enum_custom_parser_missing_arg"
            )

    def test_enum_custom_parser_bad_arg_type(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected type for argument \[raw_text\] in function "
            r"\[fake_custom_enum_parsers.enum_parser_bad_arg_type\] in module \[[a-z_\.]+\]. Expected "
            r"\[<class 'str'>], found \[<class 'bool'>\].",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "enum_custom_parser_bad_arg_type"
            )

    def test_serialize_json(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="1",
                        id_type="ID_TYPE",
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name='{"GIVEN_NAMES": "JERRY", "SURNAME": "SEINFELD"}',
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name='{"GIVEN_NAMES": "ELAINE", "SURNAME": "BENES"}',
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name='{"GIVEN_NAMES": "", "SURNAME": "KRAMER"}',
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("serialize_json")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_serialize_json_complex(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="1",
                        id_type="ID_TYPE",
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "JERRY", "MIDDLE_NAMES": "JIMMY-JOHN", '
                            '"SUFFIX": "SR", "SURNAME": "SEINFELD"}'
                        ),
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "ELAINE", "MIDDLE_NAMES": "SALLY-SUE", '
                            '"SUFFIX": "SR", "SURNAME": "BENES"}'
                        ),
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "", "MIDDLE_NAMES": "NONE-NONE", '
                            '"SUFFIX": "SR", "SURNAME": "KRAMER"}'
                        ),
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("serialize_json_complex")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_concatenate_values(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1-A", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2-B", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3-C", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4-NONE", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="NONE-E", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("concatenate_values")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_concatenate_values_custom_separator(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1@@A", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2@@B", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3@@C", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="4@@NONE",
                        id_type="ID_TYPE",
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX",
                        external_id="NONE@@E",
                        id_type="ID_TYPE",
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "concatenate_values_custom_separator"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_concatenate_values_filter_nulls(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="1-A",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="2",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="C",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name=None,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "concatenate_values_filter_nulls"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_concatenate_values_enum_raw_text(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="F-1",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M-0",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MA-0",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender_raw_text="U-2",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender_raw_text="U-NONE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender_raw_text="NONE-NONE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "concatenate_values_enum_raw_text"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_person_name_simple(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "ELAINE", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "BENES"}',
            ),
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "JERRY", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "SEINFELD"}',
            ),
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "KRAMER"}',
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("person_name_simple")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_person_name_complex(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "ELAINE SALLY", "MIDDLE_NAMES": "SUE", "NAME_SUFFIX": "SR", "SURNAME": "BENES"}',
            ),
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "JERRY JIMMY", "MIDDLE_NAMES": "JOHN", "NAME_SUFFIX": "JR", "SURNAME": "SEINFELD"}',
            ),
            FakePerson(
                fake_state_code="US_XX",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name='{"GIVEN_NAMES": "", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "KRAMER"}',
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("person_name_complex")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_physical_address(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA",
                current_address="123 FOURTH ST, APT 100, SAN FRANCISCO, CA 10000",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="BOB",
                current_address="234 FIFTH AVE, NEW YORK, NY 20000",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="CLARA",
                current_address="SEATTLE, WA 30000",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="DEV",
                current_address="345 SIXTH WAY, PORTLAND",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="ESTHER",
                current_address="99999",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("physical_address")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA ROSE",
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH ROSE",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("boolean_condition")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_env_property_production_secondary(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA ROSE",
                gender=FakeGender.FEMALE,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH ROSE",
                gender=FakeGender.FEMALE,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA ROSE",
                gender=FakeGender.FEMALE,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_env_property",
            ingest_instance=DirectIngestInstance.SECONDARY,
            is_production=True,
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_env_property_staging_primary(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA",
                gender=FakeGender.FEMALE,
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX", name="HANNAH", gender=FakeGender.FEMALE
            ),
            FakePerson(fake_state_code="US_XX", name="JULIA", gender=FakeGender.FEMALE),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_env_property",
            ingest_instance=DirectIngestInstance.PRIMARY,
            is_production=False,
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_env_property_production_primary(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA ROSE",
                birthdate=datetime.date(1962, 1, 29),
                gender=FakeGender.MALE,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH ROSE",
                birthdate=None,
                gender=FakeGender.MALE,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA ROSE",
                birthdate=None,
                gender=FakeGender.MALE,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_env_property",
            ingest_instance=DirectIngestInstance.PRIMARY,
            is_production=True,
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_enum_raw_text(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MA",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MALE",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_enum_raw_text"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_enum_mappings(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="MA",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="4", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_enum_mappings"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_entity_tree(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                current_officer=FakeAgent(
                    external_id="A123", fake_state_code="US_XX", name="NEWMAN"
                ),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                current_officer=FakeAgent(
                    external_id="B456", fake_state_code="US_XX", name="STEINBRENNER"
                ),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="COSMOS KRAMER",
                birthdate=None,
                current_officer=None,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("boolean_condition_entity_tree")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_simple_null(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ALBERT",
                birthdate=datetime.date(2010, 1, 1),
                gender=FakeGender.MALE,
                gender_raw_text="M",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name=None,
                birthdate=None,
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="CHARLES",
                birthdate=datetime.date(2015, 2, 2),
                gender=FakeGender.MALE,
                gender_raw_text="MM",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("boolean_condition_simple_null")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_complex_null(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(fake_state_code="US_XX", name="ANNA"),
            FakePerson(fake_state_code="US_XX", name="BERTHA"),
            FakePerson(fake_state_code="US_XX", name="BOTH NULL"),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_complex_null"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_boolean_equals_and_or(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ROSIE",
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH",
                birthdate=datetime.date(1999, 2, 2),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA",
                birthdate=datetime.date(1989, 5, 5),
            ),
            FakePerson(
                fake_state_code="US_XX", name=None, birthdate=datetime.date(1997, 10, 5)
            ),
            FakePerson(fake_state_code="US_XX", name="ROSIE", birthdate=None),
            FakePerson(fake_state_code="US_XX", name=None, birthdate=None),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_equals_and_or"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_boolean_condition_multi_branch(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA",
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH",
                birthdate=datetime.date(1980, 1, 1),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA",
                birthdate=datetime.date(1980, 1, 1),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="SALLY",
                birthdate=datetime.date(1997, 10, 5),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="ROSIE",
                birthdate=datetime.date(1970, 1, 1),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name=None,
                birthdate=datetime.date(1970, 1, 1),
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view(
            "boolean_condition_multi_branch"
        )

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_custom_parsers(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ALBERT",
                current_address="123 FOURTH ST, SAN FRANCISCO, CA 94110",
                ssn=111223333,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="BERTHA",
                current_address="100 MAIN RD, NEW YORK, NY 10000",
                ssn=444556666,
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="CHARLES",
                current_address="INVALID",
                ssn=777889999,
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("custom_parsers")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_custom_conditional(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(fake_state_code="US_XX", name="ALBERT", ssn=None),
            FakePerson(fake_state_code="US_XX", name="BERTHA", ssn=123456789),
            FakePerson(fake_state_code="US_XX", name="CHARLES", ssn=987654321),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("custom_conditional")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_entity_enum_literal(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ALICE",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.ASIAN, race_raw_text=None
                    )
                ],
            ),
            FakePerson(fake_state_code="US_XX", name="BOB", races=[]),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enum_entity_enum_literal")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_foreach_conditional(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ALICE",
                races=[
                    FakePersonRace(fake_state_code="US_XX", race=FakeRace.ASIAN),
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="BOB",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.WHITE, race_raw_text="W"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="CHARLIE",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    )
                ],
            ),
            FakePerson(fake_state_code="US_XX", name="DEV", races=[]),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("foreach_conditional")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_json_extract_value(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.ASIAN, race_raw_text="A"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("json_extract_value")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_json_list(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ELAINE BENES",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.ASIAN, race_raw_text="A"
                    ),
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    ),
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JERRY SEINFELD",
                races=[
                    FakePersonRace(
                        fake_state_code="US_XX", race=FakeRace.BLACK, race_raw_text="B"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("json_list")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_filter_predicate(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "JERRY", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "SEINFELD"}'
                        ),
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "ELAINE", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "BENES"}'
                        ),
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                aliases=[],
            ),
            FakePerson(
                fake_state_code="US_XX",
                aliases=[
                    FakePersonAlias(
                        fake_state_code="US_XX",
                        full_name=(
                            '{"GIVEN_NAMES": "", "MIDDLE_NAMES": "", "NAME_SUFFIX": "", "SURNAME": "KRAMER"}'
                        ),
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("filter_predicate")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_simple_variables(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA ROSE",
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH ROSE",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("simple_variables")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_chained_variables(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                name="ANNA ROSE",
                birthdate=datetime.date(1962, 1, 29),
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="HANNAH ROSE",
            ),
            FakePerson(
                fake_state_code="US_XX",
                name="JULIA",
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("chained_variables")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_custom_function_variable(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(fake_state_code="US_XX", name="ALBERT", ssn=None),
            FakePerson(fake_state_code="US_XX", name="BERTHA", ssn=123456789),
            FakePerson(fake_state_code="US_XX", name="CHARLES", ssn=987654321),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("custom_function_variable")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enum_variable(self) -> None:
        # Arrange
        expected_output = [
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="1", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                gender=FakeGender.MALE,
                gender_raw_text="M",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="2", id_type="ID_TYPE"
                    )
                ],
            ),
            FakePerson(
                fake_state_code="US_XX",
                external_ids=[
                    FakePersonExternalId(
                        fake_state_code="US_XX", external_id="3", id_type="ID_TYPE"
                    )
                ],
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_ingest_view("enum_variable")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_enums_bad_mapping_mixed_enums(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Enum \$mappings should only contain mappings for one enum type but found "
            r"multiple: \['FakeGender', 'FakeRace'\]",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "enums_bad_mapping_mixed_enums"
            )

    def test_bad_variable_type_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Unexpected manifest node type: \[<class 'bool'>\]\. "
            r"Expected result_type: \[<class 'str'>\]\.",
        ):
            _ = self._run_parse_for_ingest_view("bad_variable_type")

    def test_nested_foreach(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected \$iter_item key value in row: \{.*\}. Nested loops not supported.$",
        ):
            _ = self._run_parse_for_ingest_view("nested_foreach")

    def test_no_unused_columns(self) -> None:
        # Shouldn't crash
        _ = self._run_parse_manifest_for_ingest_view("no_unused_columns")

    def test_does_not_use_all_columns_in_input_cols(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found columns listed in |input_columns| that are not referenced in "
            r"|output| or listed in |unused_columns|: {'SSN'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view("unused_input_column")

    def test_referenced_col_not_listed(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found columns referenced in |output| that are not listed in "
            r"|input_columns|: {'AGENTNAME'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view("unlisted_referenced_column")

    def test_duplicate_input_col(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found item listed multiple times in |input_columns|: \[PERSONNAME\]",
        ):
            _ = self._run_parse_manifest_for_ingest_view("duplicate_input_column")

    def test_duplicate_unused_col(self) -> None:
        with self.assertRaisesRegex(
            jsonschema.exceptions.ValidationError,
            r"Failed validating 'uniqueItems' in schema\['properties'\]\['unused_columns'\]",
        ):
            _ = self._run_parse_manifest_for_ingest_view("duplicate_unused_column")

    def test_csv_does_not_have_input_column(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found columns in manifest |input_columns| list that are missing from "
            r"file row [0]: {'DOB'}",
        ):
            _ = self._run_parse_for_ingest_view("input_col_not_in_csv")

    def test_unused_col_not_in_csv(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found columns in manifest |input_columns| list that are missing from "
            r"file row [0]: {'DOB'}",
        ):
            _ = self._run_parse_for_ingest_view("unused_col_not_in_csv")

    def test_extra_column_in_csv(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found columns in input file row [0] not present in manifest "
            r"|input_columns| list: {'SSN'}",
        ):
            _ = self._run_parse_for_ingest_view("extra_csv_column")

    def test_unused_col_not_in_input_cols_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found values listed in |unused_columns| that were not also listed in "
            r"|input_columns|: {'SSN'}",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "unused_col_not_in_input_columns"
            )

    def test_input_cols_do_not_start_with_dollar_sign(self) -> None:
        with self.assertRaisesRegex(
            jsonschema.exceptions.ValidationError,
            r"'\$DOB' is not of type",
        ):
            _ = self._run_parse_manifest_for_ingest_view(
                "column_starts_with_dollar_sign"
            )

    def test_throws_if_primary_key_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot set autogenerated database primary key field \[fake_agent_id\] in "
            r"the ingest manifest. Did you mean to set the 'external_id' field?",
        ):
            self._run_parse_manifest_for_ingest_view("set_primary_key_has_external_id")
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot set autogenerated database primary key field \[fake_person_id\] "
            r"in the ingest manifest.$",
        ):
            self._run_parse_manifest_for_ingest_view("set_primary_key_no_external_id")
