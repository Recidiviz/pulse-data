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

import datetime
import os
import unittest
from typing import Dict, List, Optional, Type, Union

import attr

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.common import attr_validators
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.ingest_view_file_parser import (
    FileFormat,
    IngestViewFileParser,
    IngestViewFileParserDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entity_deserialize import (
    EntityFactory,
    entity_deserialize,
)
from recidiviz.tests.ingest.direct.controllers.fixtures.ingest_view_file_parser import (
    ingest_view_files,
    manifests,
)

#### Start Fake Schema ####


@attr.s(eq=False)
class FakePerson(Entity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    name: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    birthdate: Optional[datetime.date] = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    # Fake primary key field
    person_pk: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    external_ids: List["FakePersonExternalId"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    aliases: List["FakePersonAlias"] = attr.ib(
        factory=list, validator=attr_validators.is_list
    )
    current_officer: Optional["FakeAgent"] = attr.ib(default=None)


@attr.s(eq=False)
class FakePersonAlias(Entity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    full_name: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Fake primary key field
    person_alias_pk: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Back edge relationship
    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.s(eq=False)
class FakePersonExternalId(Entity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    external_id: str = attr.ib(validator=attr_validators.is_str)
    id_type: str = attr.ib(validator=attr_validators.is_str)

    # Fake primary key field
    person_external_id_pk: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )

    # Back edge relationship
    person: Optional["FakePerson"] = attr.ib(default=None)


@attr.s(eq=False)
class FakeAgent(ExternalIdEntity):
    fake_state_code: str = attr.ib(validator=attr_validators.is_str)

    name: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Fake primary key field
    agent_pk: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )


#### End Fake Schema ####

#### Start Fake Schema Factories ####


class FakePersonExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> FakePersonExternalId:
        return entity_deserialize(
            cls=FakePersonExternalId, converter_overrides={}, **kwargs
        )


class FakePersonFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> FakePerson:
        return entity_deserialize(cls=FakePerson, converter_overrides={}, **kwargs)


class FakePersonAliasFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> FakePersonAlias:
        return entity_deserialize(cls=FakePersonAlias, converter_overrides={}, **kwargs)


class FakeAgentFactory(EntityFactory):
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> FakeAgent:
        return entity_deserialize(cls=FakeAgent, converter_overrides={}, **kwargs)


#### End Fake Schema Factories ####


class FakeSchemaIngestViewFileParserDelegate(IngestViewFileParserDelegate):
    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        return os.path.join(os.path.dirname(manifests.__file__), f"{file_tag}.yaml")

    def get_common_args(self) -> Dict[str, Union[str, EnumParser]]:
        return {"fake_state_code": StateCode.US_XX.value}

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        if entity_cls_name == FakePerson.__name__:
            return FakePersonFactory
        if entity_cls_name == FakePersonExternalId.__name__:
            return FakePersonExternalIdFactory
        if entity_cls_name == FakePersonAlias.__name__:
            return FakePersonAliasFactory
        if entity_cls_name == FakeAgent.__name__:
            return FakeAgentFactory
        raise ValueError(f"Unexpected class name [{entity_cls_name}]")

    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        if entity_cls_name == FakePerson.__name__:
            return FakePerson
        if entity_cls_name == FakePersonExternalId.__name__:
            return FakePersonExternalId
        if entity_cls_name == FakePersonAlias.__name__:
            return FakePersonAlias
        if entity_cls_name == FakeAgent.__name__:
            return FakeAgent
        raise ValueError(f"Unexpected class name [{entity_cls_name}]")


class IngestViewFileParserTest(unittest.TestCase):
    """Tests for IngestViewFileParser."""

    @staticmethod
    def _run_parse_for_tag(file_tag: str) -> List[Entity]:
        """Runs a single parsing test for the given fixture file tag, returning the
        parsed entities.
        """
        parser = IngestViewFileParser(FakeSchemaIngestViewFileParserDelegate())
        return parser.parse(
            file_tag=file_tag,
            contents_handle=GcsfsFileContentsHandle(
                os.path.join(
                    os.path.dirname(ingest_view_files.__file__), f"{file_tag}.csv"
                ),
                cleanup_file=False,
            ),
            file_format=FileFormat.CSV,
        )

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
        parsed_output = self._run_parse_for_tag("simple_person")

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
        parsed_output = self._run_parse_for_tag("simple_agent")

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
        parsed_output = self._run_parse_for_tag("list_field")

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
        parsed_output = self._run_parse_for_tag("list_field_from_list_col")

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
        parsed_output = self._run_parse_for_tag("list_field_from_multiple_list_col")

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
                    external_id=None,
                    fake_state_code="US_XX",
                ),
            ),
        ]

        # Act
        parsed_output = self._run_parse_for_tag("single_child_field")

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
        parsed_output = self._run_parse_for_tag("reused_column")

        # Assert
        self.assertEqual(expected_output, parsed_output)

    def test_nested_foreach(self) -> None:
        # TODO(#8958): Fill this out - should fail
        pass

    def test_no_unused_columns(self) -> None:
        # TODO(#8957): Fill this out - should pass
        pass

    def test_does_not_use_all_columns_in_input_cols(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_input_col_not_listed(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_csv_does_not_have_input_column(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_unused_col_not_in_csv(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_unused_col_not_in_input_cols_list(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_input_cols_do_not_start_with_dollar_sign(self) -> None:
        # TODO(#8957): Fill this out - should fail
        pass

    def test_multiple_rows_same_person(self) -> None:
        # TODO(#8908): Fill this out - should pass,
        #  but not merge trees.
        pass
