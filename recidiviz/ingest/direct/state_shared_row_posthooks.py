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
"""Row posthooks shared between multiple direct ingest controllers.

All functions added to this file should have an associated to-do outlining how
we might update our yaml mapping support to handle this case more generally.
"""
import importlib
import logging
from typing import Dict, List, Callable, Type, Optional, Union

from recidiviz.common.constants.state.state_person_alias import \
    StatePersonAliasType
from recidiviz.ingest.direct.direct_ingest_controller_utils import \
    create_if_not_exists
from recidiviz.ingest.models.ingest_info import IngestObject, StateAlias, \
    StatePerson, StatePersonExternalId, StateSentenceGroup, \
    StateSupervisionSentence, StateIncarcerationSentence, IngestInfo
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


# TODO(1882): This should no-longer be necessary once you can map a column
#  value to multiple fields on the ingested object.
def copy_name_to_alias(_file_tag: str,
                       _row: Dict[str, str],
                       extracted_objects: List[IngestObject],
                       _cache: IngestObjectCache):
    """Copy all name fields stored on a StatePerson object to a new StateAlias
    child object.
    """
    for extracted_object in extracted_objects:
        if isinstance(extracted_object, StatePerson):
            alias_to_create = StateAlias(
                full_name=extracted_object.full_name,
                surname=extracted_object.surname,
                given_names=extracted_object.given_names,
                middle_names=extracted_object.middle_names,
                name_suffix=extracted_object.name_suffix,
                alias_type=StatePersonAliasType.GIVEN_NAME.value
            )

            create_if_not_exists(alias_to_create,
                                 extracted_object,
                                 'state_aliases')


# TODO(1882): If yaml format supported raw values, this would no-longer be
#  necessary.
def gen_label_single_external_id_hook(external_id_type: str) -> Callable:
    """Generates a row post-hook that will hydrate the id_type field on the
    singular StatePersonExternalId in the extracted objects. Will throw if
    there is more than one external id to label.
    """

    def _label_external_id(_file_tag: str,
                           _row: Dict[str, str],
                           extracted_objects: List[IngestObject],
                           _cache: IngestObjectCache):
        found = False
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                if found:
                    raise ValueError(
                        'Already found object of type StatePersonExternalId')

                extracted_object.__setattr__('id_type', external_id_type)
                found = True

    return _label_external_id


def _load_county_codes_map_for_state(state_code: str) -> Dict[str, str]:
    module_name = \
        f'recidiviz.ingest.direct.regions.{state_code.lower()}' \
        f'.{state_code.lower()}_county_code_reference'

    module = importlib.import_module(module_name)

    return getattr(module, 'COUNTY_CODES')


def _default_normalized_county_code(
        county_code: Optional[str],
        county_codes_map: Dict[str, str]) -> Optional[str]:
    if not county_code:
        return None

    normalized_code = county_codes_map.get(county_code.upper())
    if not normalized_code:
        logging.warning("Found new county code not in reference cache: [%s]",
                        county_code)
        return county_code

    return normalized_code


def gen_normalize_county_codes_posthook(
        state_code: str,
        col_name: str,
        ingest_type: Type[IngestObject],
        custom_normalize_fn: Optional[Callable[[str,
                                                Dict[str, str]],
                                               Optional[str]]] = None
) -> Callable:
    normalize_fn = custom_normalize_fn \
        if custom_normalize_fn is not None \
        else _default_normalized_county_code

    county_codes_map = _load_county_codes_map_for_state(state_code)

    def _normalize_county_codes(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):

        county_code = row[col_name]
        normalized_code = normalize_fn(county_code, county_codes_map)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, ingest_type):
                extracted_object.__setattr__('county_code', normalized_code)

    return _normalize_county_codes


def gen_map_ymd_counts_to_max_length_field_posthook(
        years_col_name: str,
        months_col_name: str,
        days_col_name: str,
        ingest_type: Type[Union[StateSentenceGroup,
                                StateIncarcerationSentence,
                                StateSupervisionSentence]],
        test_for_fallback: Optional[Callable] = None,
        fallback_parser: Optional[Callable] = None
):
    """Generates a row post-hook that will hydrate the max_length field as
    formatted duration string on objects with the specified sentence type,
    based on the values in separate year, month, and day counts columns.

    Example output of posthook: '10Y 5M 0D'

    The two optional callables are used for state-specific fallback parsing to
    be used in the event that the generated length string is unsuitable. If the
    |test_for_fallback| is given and it returns False on the generated length
    string, then it will pass the length string into the given |fallback_parser|
    and use that output instead.
    """

    def _normalize_ymd_codes(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):

        length_str = get_normalized_ymd_str(
            years_col_name, months_col_name, days_col_name, row)
        if test_for_fallback and not test_for_fallback(length_str):
            if fallback_parser:
                length_str = fallback_parser(length_str)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, ingest_type):
                extracted_object.__setattr__('max_length', length_str)

    return _normalize_ymd_codes


def get_normalized_ymd_str(
        years_col_name: str,
        months_col_name: str,
        days_col_name: str,
        row: Dict[str, str]):
    """Given a |row| and column names that correspond to fields with year,
    month, and day information, returns a single normalized string representing
    this information.

    Example output: '10Y 5M 0D'
    """
    def normalize_numerical_str(numerical_str: str) -> str:
        no_commas_str = numerical_str.replace(',', '')

        if not no_commas_str:
            return '0'

        return no_commas_str

    years = normalize_numerical_str(row[years_col_name])
    months = normalize_numerical_str(row[months_col_name])
    days = normalize_numerical_str(row[days_col_name])

    return f'{years}Y {months}M {days}D'


def gen_set_is_life_sentence_hook(
        sen_calc_type_col: str,
        is_life_val: str,
        ingest_type: Type[Union[StateSentenceGroup,
                                StateIncarcerationSentence]]) -> Callable:
    def _set_is_life_sentence(_file_tag: str,
                              row: Dict[str, str],
                              extracted_objects: List[IngestObject],
                              _cache: IngestObjectCache):
        is_life_sentence = row[sen_calc_type_col] == is_life_val

        if not is_life_sentence and ingest_type == StateSentenceGroup:
            # Don't set is_life on the StateSentenceGroup rollup unless we
            # have positive confirmation.
            return

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, ingest_type):
                extracted_object.__setattr__('is_life', str(is_life_sentence))

    return _set_is_life_sentence


def gen_convert_person_ids_to_external_id_objects(
        get_id_type: Callable[[str], Optional[str]]):

    def _convert_person_ids_to_external_id_objects(
            file_tag: str,
            ingest_info: IngestInfo,
            cache: Optional[IngestObjectCache]):
        id_type = get_id_type(file_tag)
        if not id_type:
            return

        if cache is None:
            raise ValueError("Ingest object cache is unexpectedly None")

        for state_person in ingest_info.state_people:
            state_person_id = state_person.state_person_id
            if state_person_id is None:
                continue

            existing_external_id = \
                state_person.get_state_person_external_id_by_id(state_person_id)

            if existing_external_id is None:
                state_person.create_state_person_external_id(
                    state_person_external_id_id=state_person_id,
                    id_type=id_type)

    return _convert_person_ids_to_external_id_objects
