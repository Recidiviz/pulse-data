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
"""Factories for deserializing entities in state/entities.py from ingested values."""

from typing import Union

from recidiviz.common.constants.enum_parser import (
    EnumParser,
    get_parser_for_enum_with_default,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.str_field_utils import normalize_flat_json
from recidiviz.persistence.entity.entity_deserialize import (
    entity_deserialize,
    EntityFieldConverter,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    parse_residency_status,
)


class StatePersonExternalIdFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> entities.StatePersonExternalId:
        return entity_deserialize(
            cls=entities.StatePersonExternalId, converter_overrides={}, **kwargs
        )


class StatePersonFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> entities.StatePerson:
        return entity_deserialize(
            cls=entities.StatePerson,
            converter_overrides={
                "residency_status": EntityFieldConverter(str, parse_residency_status),
                "full_name": EntityFieldConverter(str, normalize_flat_json),
            },
            **kwargs
        )


class StatePersonRaceFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> entities.StatePersonRace:
        return entity_deserialize(
            cls=entities.StatePersonRace, converter_overrides={}, **kwargs
        )


class StatePersonEthnicityFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> entities.StatePersonEthnicity:
        return entity_deserialize(
            cls=entities.StatePersonEthnicity, converter_overrides={}, **kwargs
        )


class StateSentenceGroupFactory:
    @staticmethod
    def deserialize(**kwargs: Union[str, EnumParser]) -> entities.StateSentenceGroup:
        return entity_deserialize(
            cls=entities.StateSentenceGroup,
            converter_overrides={
                "status": EntityFieldConverter(
                    EnumParser,
                    get_parser_for_enum_with_default(
                        StateSentenceStatus.PRESENT_WITHOUT_INFO
                    ),
                ),
            },
            **kwargs
        )
