# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Util functions shared across multiple types of hooks in the direct
ingest controllers."""
import logging
from typing import Dict, List, TypeVar

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumMapper, EnumIgnorePredicate
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.ingest.models.ingest_info import IngestObject
from recidiviz.utils import environment
from recidiviz.utils.regions import Region

EnumType = TypeVar('EnumType', bound=EntityEnum)


def invert_enum_to_str_mappings(overrides: Dict[EnumType, List[str]]) -> Dict[str, EnumType]:
    """Inverts a given dictionary, that maps a target enum from a list of parseable strings, into another dictionary,
    that maps each parseable string to its target enum."""
    inverted_overrides: Dict[str, EnumType] = {}
    for mapped_enum, text_tokens in overrides.items():
        for text_token in text_tokens:
            if text_token in inverted_overrides:
                raise ValueError(f'Unexpected, text_token [{text_token}] is used for both '
                                 f'[{mapped_enum}] and [{inverted_overrides[text_token]}]')
            inverted_overrides[text_token] = mapped_enum

    return inverted_overrides


def invert_str_to_str_mappings(overrides: Dict[str, List[str]]) -> Dict[str, str]:
    """The same as invert_enum_to_str_mappings, but for raw target strings instead of typed enums."""
    inverted_overrides: Dict[str, str] = {}
    for mapped_str, text_tokens in overrides.items():
        for text_token in text_tokens:
            if text_token in inverted_overrides:
                raise ValueError(f'Unexpected, text_token [{text_token}] is used for both '
                                 f'[{mapped_str}] and [{inverted_overrides[text_token]}]')
            inverted_overrides[text_token] = mapped_str

    return inverted_overrides


def update_overrides_from_maps(
        base_enum_overrides: EnumOverrides,
        overrides: Dict[EntityEnum, List[str]],
        ignores: Dict[EntityEnumMeta, List[str]],
        override_mappers: Dict[EntityEnumMeta, EnumMapper],
        ignore_predicates: Dict[EntityEnumMeta, EnumIgnorePredicate]) -> EnumOverrides:
    overrides_builder = base_enum_overrides.to_builder()

    for mapped_enum, text_tokens in overrides.items():
        for text_token in text_tokens:
            overrides_builder.add(text_token, mapped_enum)

    for ignored_enum, text_tokens in ignores.items():
        for text_token in text_tokens:
            overrides_builder.ignore(text_token, ignored_enum)

    for mapped_enum_cls, mapper in override_mappers.items():
        overrides_builder.add_mapper(mapper, mapped_enum_cls)

    for ignored_enum_cls, ignore_predicate in ignore_predicates.items():
        overrides_builder.ignore_with_predicate(ignore_predicate, ignored_enum_cls)

    return overrides_builder.build()


def create_if_not_exists(obj: IngestObject,
                         parent_obj: IngestObject,
                         objs_field_name: str) -> None:
    """Create an object on |parent_obj| if an identical object does not
    already exist.
    """

    existing_objects = getattr(parent_obj, objs_field_name) or []
    if isinstance(existing_objects, IngestObject):
        existing_objects = [existing_objects]

    for existing_obj in existing_objects:
        if obj == existing_obj:
            return
    create_func = getattr(parent_obj, f'create_{obj.class_name()}')
    create_func(**obj.__dict__)


def check_is_region_launched_in_env(region: Region) -> None:
    """Checks if direct ingest has been launched for the provided |region| in the current GAE env and throws if it has
    not."""
    if not region.is_ingest_launched_in_env():
        gae_env = environment.get_gae_environment()
        error_msg = f'Bad environment [{gae_env}] for region [{region.region_code}].'
        logging.error(error_msg)
        raise DirectIngestError(msg=error_msg, error_type=DirectIngestErrorType.ENVIRONMENT_ERROR)
