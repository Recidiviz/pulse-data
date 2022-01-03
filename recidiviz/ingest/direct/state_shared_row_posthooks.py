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
from enum import Enum
from typing import Callable, Dict, List, Optional, Type, Union

import attr

from recidiviz.common.constants.person_characteristics import Ethnicity
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.ingest.direct.direct_ingest_controller_utils import create_if_not_exists
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.extractor.csv_data_extractor import (
    FilePostprocessorCallable,
    RowPosthookCallable,
)
from recidiviz.ingest.models.ingest_info import (
    IngestInfo,
    IngestObject,
    StateAgent,
    StateAlias,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StateSupervisionSentence,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


@attr.s(frozen=True, kw_only=True)
class IngestGatingContext:
    file_tag: str = attr.ib()
    ingest_instance: DirectIngestInstance = attr.ib()


# TODO(#8905): This should no-longer be necessary once all ingest views are migrated
# to v2 mappings.
def copy_name_to_alias(
    _gating_context: IngestGatingContext,
    _row: Dict[str, str],
    extracted_objects: List[IngestObject],
    _cache: IngestObjectCache,
) -> None:
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
                alias_type=StatePersonAliasType.GIVEN_NAME.value,
            )

            create_if_not_exists(alias_to_create, extracted_object, "state_aliases")


def gen_rationalize_race_and_ethnicity(
    enum_overrides: Dict[Enum, List[str]]
) -> RowPosthookCallable:
    """Generates a row post-hook that will identify provided races which are actually ethnicities, and record
    them as ethnicities instead of races."""

    def _rationalize_race_and_ethnicity(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        ethnicity_override_values = []
        for ethnicity in Ethnicity:
            ethnicity_override_values.extend(enum_overrides.get(ethnicity, []))

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                updated_person_races = []
                for person_race in obj.state_person_races:
                    if person_race.race in ethnicity_override_values:
                        ethnicity_to_create = StatePersonEthnicity(
                            ethnicity=person_race.race
                        )
                        create_if_not_exists(
                            ethnicity_to_create, obj, "state_person_ethnicities"
                        )
                    else:
                        updated_person_races.append(person_race)
                obj.state_person_races = updated_person_races

    return _rationalize_race_and_ethnicity


# TODO(#8905): This row post hook should not be necessary all views are migrated to
# ingest mappings v2.
def gen_label_single_external_id_hook(external_id_type: str) -> RowPosthookCallable:
    """Generates a row post-hook that will hydrate the id_type field on the
    singular StatePersonExternalId in the extracted objects. Will throw if
    there is more than one external id to label.
    """

    def _label_external_id(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        found = False
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                if found:
                    raise ValueError(
                        "Already found object of type StatePersonExternalId"
                    )

                extracted_object.__setattr__("id_type", external_id_type)
                found = True

    return _label_external_id


def _load_county_codes_map_for_state(state_code: str) -> Dict[str, str]:
    module_name = (
        f"recidiviz.ingest.direct.regions.{state_code.lower()}"
        f".{state_code.lower()}_county_code_reference"
    )

    module = importlib.import_module(module_name)

    return getattr(module, "COUNTY_CODES")


def _default_normalized_county_code(
    county_code: Optional[str], county_codes_map: Dict[str, str]
) -> Optional[str]:
    if not county_code:
        return None

    normalized_code = county_codes_map.get(county_code.upper())
    if not normalized_code:
        logging.warning(
            "Found new county code not in reference cache: [%s]", county_code
        )
        return county_code

    return normalized_code


def gen_normalize_county_codes_posthook(
    state_code: str,
    col_name: str,
    ingest_type: Type[IngestObject],
    custom_normalize_fn: Optional[
        Callable[[str, Dict[str, str]], Optional[str]]
    ] = None,
) -> RowPosthookCallable:
    """Returns a function that can normalize county codes during ingest as a data extractor post-hook."""
    normalize_fn = (
        custom_normalize_fn
        if custom_normalize_fn is not None
        else _default_normalized_county_code
    )

    county_codes_map = _load_county_codes_map_for_state(state_code)

    def _normalize_county_codes(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        county_code = row[col_name]
        normalized_code = normalize_fn(county_code, county_codes_map)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, ingest_type):
                extracted_object.__setattr__("county_code", normalized_code)

    return _normalize_county_codes


def gen_map_ymd_counts_to_max_length_field_posthook(
    years_col_name: str,
    months_col_name: str,
    days_col_name: str,
    ingest_type: Type[Union[StateIncarcerationSentence, StateSupervisionSentence]],
    test_for_fallback: Optional[Callable] = None,
    fallback_parser: Optional[Callable] = None,
) -> RowPosthookCallable:
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
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        length_str = get_normalized_ymd_str_from_row(
            years_col_name, months_col_name, days_col_name, row
        )
        if test_for_fallback and not test_for_fallback(length_str):
            if fallback_parser:
                length_str = fallback_parser(length_str)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, ingest_type):
                extracted_object.__setattr__("max_length", length_str)

    return _normalize_ymd_codes


def get_normalized_ymd_str(
    years_numerical_str: str, months_numerical_str: str, days_numerical_str: str
) -> str:
    """Given year, month, and day values that correspond to fields with year,
    month, and day information, returns a single normalized string representing
    this information.

    Example output: '10Y 5M 0D'
    """

    def normalize_numerical_str(numerical_str: str) -> str:
        no_commas_str = numerical_str.replace(",", "")

        if not no_commas_str:
            return "0"

        return no_commas_str

    years = normalize_numerical_str(years_numerical_str)
    months = normalize_numerical_str(months_numerical_str)
    days = normalize_numerical_str(days_numerical_str)

    return f"{years}Y {months}M {days}D"


def get_normalized_ymd_str_from_row(
    years_col_name: str, months_col_name: str, days_col_name: str, row: Dict[str, str]
) -> str:
    """Given a |row| and column names that correspond to fields with year,
    month, and day information, returns a single normalized string representing
    this information.

    Example output: '10Y 5M 0D'
    """
    return get_normalized_ymd_str(
        row[years_col_name], row[months_col_name], row[days_col_name]
    )


def gen_set_is_life_sentence_hook(
    sen_calc_type_col: str,
    is_life_val: str,
) -> RowPosthookCallable:
    def _set_is_life_sentence(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        is_life_sentence = row[sen_calc_type_col] == is_life_val

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationSentence):
                extracted_object.__setattr__("is_life", str(is_life_sentence))

    return _set_is_life_sentence


def gen_convert_person_ids_to_external_id_objects(
    get_id_type: Callable[[str], Optional[str]]
) -> FilePostprocessorCallable:
    def _convert_person_ids_to_external_id_objects(
        gating_context: IngestGatingContext,
        ingest_info: IngestInfo,
        cache: Optional[IngestObjectCache],
    ) -> None:
        id_type = get_id_type(gating_context.file_tag)
        if not id_type:
            return

        if cache is None:
            raise ValueError("Ingest object cache is unexpectedly None")

        for state_person in ingest_info.state_people:
            state_person_id = state_person.state_person_id
            if state_person_id is None:
                continue

            existing_external_id = state_person.get_state_person_external_id_by_id(
                state_person_id
            )

            if existing_external_id is None:
                state_person.create_state_person_external_id(
                    state_person_external_id_id=state_person_id, id_type=id_type
                )

    return _convert_person_ids_to_external_id_objects


def _concatenate_col_values(
    row: Dict[str, str], cols: List[str], separator: str = "-"
) -> str:
    values = []
    for col in cols:
        col_value = row.get(col, None)
        if col_value:
            values.append(col_value)
    return separator.join(values)


def gen_set_field_as_concatenated_values_hook(
    obj_cls: Type[IngestObject], field_name: str, cols_to_concatenate: List[str]
) -> RowPosthookCallable:
    """
    Generates a row post-hook that sets a field on all extracted objects of a
    certain type by concatenating the values of |cols_to_concatenate| with a '-'
    separator.

    Notes:
    1) The values will be concatenated in the same order as their respective
        columns in cols_to_concatenate
    2) None or empty string values will be omitted from the concatenation.

    """

    def set_field_as_concatenated_values_hook(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        field_value = _concatenate_col_values(row, cols_to_concatenate)
        for obj in extracted_objects:
            if isinstance(obj, obj_cls):
                obj.__setattr__(field_name, field_value)

    return set_field_as_concatenated_values_hook


def gen_set_agent_type(agent_type: StateAgentType) -> RowPosthookCallable:
    """Generates a row post-hook that sets the StateAgentType.agent_type field to the given argument."""

    def _set_judge_agent_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAgent):
                extracted_object.agent_type = agent_type.value

    return _set_judge_agent_type
