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
"""Direct ingest controller implementation for US_MO."""
import datetime
from typing import Optional, List, Callable, Dict

from recidiviz.common.constants.entity_enum import EntityEnumMeta, EntityEnum
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_SID, US_MO_FBI, US_MO_OLN
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.date import munge_date_string
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces, \
    parse_yyyymmdd_date, parse_days
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import \
    create_if_not_exists, update_overrides_from_maps
from recidiviz.ingest.direct.regions.us_mo.us_mo_column_constants import \
    INCARCERATION_SENTENCE_MIN_RELEASE_TYPE, \
    INCARCERATION_SENTENCE_LENGTH_YEARS, INCARCERATION_SENTENCE_LENGTH_MONTHS, \
    INCARCERATION_SENTENCE_LENGTH_DAYS, CHARGE_COUNTY_CODE, \
    SENTENCE_COUNTY_CODE, INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS, \
    INCARCERATION_SENTENCE_START_DATE, SENTENCE_OFFENSE_DATE, \
    SENTENCE_COMPLETED_FLAG, SENTENCE_STATUS_CODE, STATE_ID, FBI_ID, \
    LICENSE_ID, SUPERVISION_SENTENCE_LENGTH_YEARS, \
    SUPERVISION_SENTENCE_LENGTH_MONTHS, SUPERVISION_SENTENCE_LENGTH_DAYS
from recidiviz.ingest.direct.state_shared_row_posthooks import \
    copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_normalize_county_codes_posthook, \
    gen_map_ymd_counts_to_max_length_field_posthook, \
    gen_set_is_life_sentence_hook, gen_convert_person_ids_to_external_id_objects
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, \
    StatePersonExternalId, StateSentenceGroup, StateCharge, \
    StateIncarcerationSentence, StateSupervisionSentence
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsMoController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_MO."""

    FILE_TAGS = [
        'tak001_offender_identification',
        'tak040_offender_cycles',
        'tak022_tak023_tak025_tak026_offender_sentence_institution',
        'tak022_tak024_tak025_tak026_offender_sentence_probation',
    ]

    PRIMARY_COL_PREFIXES_BY_FILE_TAG = {
        'tak001_offender_identification': 'EK',
        'tak040_offender_cycles': 'DQ',
        'tak022_tak023_tak025_tak026_offender_sentence_institution': 'BS',
        'tak022_tak024_tak025_tak026_offender_sentence_probation': 'BS',
    }

    SUSPENDED_SENTENCE_STATUS_CODES = {
        '35I3500',  # Bond Supv-Pb Suspended-Revisit
        '65O2015',  # Court Probation Suspension
        '65O3015',  # Court Parole Suspension
        '95O3500',  # Bond Supv-Pb Susp-Completion
        '95O3505',  # Bond Supv-Pb Susp-Bond Forfeit
        '95O3600',  # Bond Supv-Pb Susp-Trm-Tech
        '95O7145',  # DATA ERROR-Suspended
    }

    COMMUTED_SENTENCE_STATUS_CODES = {
        '90O1020',  # Institutional Commutation Comp
        '95O1025',  # Field Commutation
        '99O1020',  # Institutional Commutation
        '99O1025',  # Field Commutation
    }

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        StateChargeClassificationType.INFRACTION: ['L'],  # Local/ordinance

        StateSupervisionType.PROBATION: [
            'BND',  # Bond Supervision (no longer used)
            'CPR',  # Court Parole (a form of probation)
            'DFP',  # Deferred Prosecution
            'IPB',  # Interstate Compact Probation
            'IPR',  # Interstate Compact Probation
            'SES',  # Suspended Execution of Sentence (Probation)
            'SIS',  # Suspended Imposition of Sentence (Probation)
        ],
    }

    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {
        StateSupervisionType: ['INT'],  # Unknown meaning, rare
    }

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super(UsMoController, self).__init__(
            'us_mo',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)

        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {}
        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'tak001_offender_identification': [
                copy_name_to_alias,
                # When first parsed, the info object just has a single
                # external id - the DOC id.
                gen_label_single_external_id_hook(US_MO_DOC),
                self.tak001_offender_identification_hydrate_alternate_ids,
                self.normalize_sentence_group_ids,
            ],
            'tak040_offender_cycles': [
                gen_label_single_external_id_hook(US_MO_DOC),
                self.normalize_sentence_group_ids,
            ],
            'tak022_tak023_tak025_tak026_offender_sentence_institution': [
                gen_normalize_county_codes_posthook(self.region.region_code,
                                                    CHARGE_COUNTY_CODE,
                                                    StateCharge),
                gen_normalize_county_codes_posthook(self.region.region_code,
                                                    SENTENCE_COUNTY_CODE,
                                                    StateIncarcerationSentence),
                gen_map_ymd_counts_to_max_length_field_posthook(
                    INCARCERATION_SENTENCE_LENGTH_YEARS,
                    INCARCERATION_SENTENCE_LENGTH_MONTHS,
                    INCARCERATION_SENTENCE_LENGTH_DAYS,
                    StateIncarcerationSentence,
                    test_for_fallback=self._test_length_string,
                    fallback_parser=self._parse_days_with_long_range
                ),
                gen_set_is_life_sentence_hook(
                    INCARCERATION_SENTENCE_MIN_RELEASE_TYPE,
                    'LIF',
                    StateIncarcerationSentence),
                self.set_sentence_status,
                self._clear_zero_date_string,
                self.tak022_tak023_set_parole_eligibility_date
            ],
            'tak022_tak024_tak025_tak026_offender_sentence_probation': [
                gen_normalize_county_codes_posthook(self.region.region_code,
                                                    CHARGE_COUNTY_CODE,
                                                    StateCharge),
                gen_normalize_county_codes_posthook(self.region.region_code,
                                                    SENTENCE_COUNTY_CODE,
                                                    StateSupervisionSentence),
                gen_map_ymd_counts_to_max_length_field_posthook(
                    SUPERVISION_SENTENCE_LENGTH_YEARS,
                    SUPERVISION_SENTENCE_LENGTH_MONTHS,
                    SUPERVISION_SENTENCE_LENGTH_DAYS,
                    StateSupervisionSentence,
                    test_for_fallback=self._test_length_string,
                    fallback_parser=self._parse_days_with_long_range
                ),
                self.set_sentence_status,
                self._clear_zero_date_string
            ]
        }

        self.primary_key_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._generate_incarceration_sentence_id_coords,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._generate_supervision_sentence_id_coords
        }

        self.ancestor_chain_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._sentence_group_ancestor_chain_override,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._sentence_group_ancestor_chain_override
        }

    def _get_file_tag_rank_list(self) -> List[str]:
        return self.FILE_TAGS

    def _get_row_pre_processors_for_file(self,
                                         file_tag: str) -> List[Callable]:
        return self.row_pre_processors_by_file.get(file_tag, [])

    def _get_row_post_processors_for_file(self,
                                          file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(
            self, _file_tag: str) -> List[Callable]:
        post_processors: List[Callable] = [
            gen_convert_person_ids_to_external_id_objects(self._get_id_type),
        ]
        return post_processors

    def _get_primary_key_override_for_file(
            self, file_tag: str) -> Optional[Callable]:
        return self.primary_key_override_by_file.get(file_tag, None)

    def _get_ancestor_chain_overrides_callback_for_file(
            self, file_tag: str) -> Optional[Callable]:
        return self.ancestor_chain_override_by_file.get(file_tag, None)

    def get_enum_overrides(self) -> EnumOverrides:
        """Provides Missouri-specific overrides for enum mappings."""
        base_overrides = super(UsMoController, self).get_enum_overrides()
        return update_overrides_from_maps(base_overrides,
                                          self.ENUM_OVERRIDES,
                                          self.ENUM_IGNORES)

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
                'tak022_tak023_tak025_tak026_offender_sentence_institution',
                'tak022_tak024_tak025_tak026_offender_sentence_probation'
        ]:
            return US_MO_DOC

        return None

    # TODO(1882): If yaml format supported raw values and multiple children of
    #  the same type, then this would be no-longer necessary.
    @staticmethod
    def tak001_offender_identification_hydrate_alternate_ids(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                external_ids_to_create = [
                    StatePersonExternalId(
                        state_person_external_id_id=_row[STATE_ID],
                        id_type=US_MO_SID),
                    StatePersonExternalId(
                        state_person_external_id_id=_row[FBI_ID],
                        id_type=US_MO_FBI),
                    StatePersonExternalId(
                        state_person_external_id_id=_row[LICENSE_ID],
                        id_type=US_MO_OLN)
                    ]

                for id_to_create in external_ids_to_create:
                    create_if_not_exists(
                        id_to_create,
                        extracted_object,
                        'state_person_external_ids')

    @classmethod
    def tak022_tak023_set_parole_eligibility_date(
            cls,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        sentence_start_date = parse_yyyymmdd_date(
            row[INCARCERATION_SENTENCE_START_DATE])
        if not sentence_start_date:
            return

        parole_ineligible_days = parse_days_from_duration_pieces(
            years_str=row[INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS])

        date = sentence_start_date + \
            datetime.timedelta(days=parole_ineligible_days)

        date_iso = date.isoformat()
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                obj.__setattr__('parole_eligibility_date', date_iso)

    @classmethod
    def set_sentence_status(cls,
                            _file_tag: str,
                            row: Dict[str, str],
                            extracted_objects: List[IngestObject],
                            _cache: IngestObjectCache):

        status_enum_str = \
            cls._sentence_status_enum_str_from_row(row)
        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence,
                                StateSupervisionSentence)):
                obj.__setattr__('status', status_enum_str)

    @classmethod
    def _sentence_status_enum_str_from_row(cls, row: Dict[str, str]) -> str:
        sentence_completed_flag = row[SENTENCE_COMPLETED_FLAG]

        if sentence_completed_flag == 'Y':
            return StateSentenceStatus.COMPLETED.value

        raw_status_str = row[SENTENCE_STATUS_CODE]

        if raw_status_str in cls.SUSPENDED_SENTENCE_STATUS_CODES:
            return StateSentenceStatus.SUSPENDED.value
        if raw_status_str in cls.COMMUTED_SENTENCE_STATUS_CODES:
            return StateSentenceStatus.COMMUTED.value

        if sentence_completed_flag == 'N':
            return StateSentenceStatus.SERVING.value

        return StateSentenceStatus.EXTERNAL_UNKNOWN.value

    @classmethod
    def _clear_zero_date_string(cls,
                                _file_tag: str,
                                row: Dict[str, str],
                                extracted_objects: List[IngestObject],
                                _cache: IngestObjectCache):
        offense_date = row.get(SENTENCE_OFFENSE_DATE, None)

        if offense_date and offense_date == '0':
            for obj in extracted_objects:
                if isinstance(obj, StateCharge):
                    obj.__setattr__('offense_date', None)

    @classmethod
    def _sentence_group_ancestor_chain_override(cls,
                                                file_tag: str,
                                                row: Dict[str, str]):
        coords = cls._generate_sentence_group_id_coords(file_tag, row)
        return {
            coords.class_name: coords.field_value
        }

    @classmethod
    def normalize_sentence_group_ids(
            cls,
            file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        for obj in extracted_objects:
            if isinstance(obj, StateSentenceGroup):
                obj.__setattr__(
                    'state_sentence_group_id',
                    cls._generate_sentence_group_id(col_prefix, row))

    @classmethod
    def _generate_sentence_group_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_sentence_group',
            'state_sentence_group_id',
            cls._generate_sentence_group_id(col_prefix, row))

    @classmethod
    def _generate_supervision_sentence_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_supervision_sentence',
            'state_supervision_sentence_id',
            cls._generate_sentence_id(col_prefix, row))

    @classmethod
    def _generate_incarceration_sentence_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_incarceration_sentence',
            'state_incarceration_sentence_id',
            cls._generate_sentence_id(col_prefix, row))

    @classmethod
    def _generate_sentence_group_id(cls,
                                    col_prefix: str,
                                    row: Dict[str, str]) -> str:
        doc_id = row[f'{col_prefix}$DOC']
        cyc_id = row[f'{col_prefix}$CYC']
        return f'{doc_id}-{cyc_id}'

    @classmethod
    def _generate_sentence_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)
        sen_seq_num = row[f'{col_prefix}$SEO']
        return f'{sentence_group_id}-{sen_seq_num}'

    @classmethod
    def primary_col_prefix_for_file_tag(cls, file_tag: str) -> str:
        return cls.PRIMARY_COL_PREFIXES_BY_FILE_TAG[file_tag]

    @classmethod
    def _test_length_string(cls, time_string: str) -> bool:
        """Tests the length string to see if it will cause an overflow beyond
        the Python MAXYEAR."""
        try:
            parse_days(time_string)
            return True
        except ValueError:
            return False

    @classmethod
    def _parse_days_with_long_range(cls, time_string: str) -> str:
        """Parses a time string that we assume to have a range long enough that
        it cannot be parsed by our standard Python date parsing utilities.

        Some length strings in Missouri sentence data, particularly for life
        sentences, will have very long ranges, like '9999Y 99M 99D'. These
        cannot be natively interpreted by the Python date parser, which has a
        MAXYEAR setting that cannot be altered. So we check for these kinds of
        strings and parse them using basic, approximate arithmetic to generate
        a usable value.

        If there is a structural issue that this function cannot handle, it
        returns the given time string unaltered.
        """
        try:
            date_string = munge_date_string(time_string)
            components = date_string.split(' ')
            total_days = 0.0

            for component in components:
                if 'year' in component:
                    year_count_str = component.split('year')[0]
                    try:
                        year_count = int(year_count_str)
                        total_days += year_count * 365.25
                    except ValueError:
                        pass
                elif 'month' in component:
                    month_count_str = component.split('month')[0]
                    try:
                        month_count = int(month_count_str)
                        total_days += month_count * 30.5
                    except ValueError:
                        pass
                elif 'day' in component:
                    day_count_str = component.split('day')[0]
                    try:
                        day_count = int(day_count_str)
                        total_days += day_count
                    except ValueError:
                        pass

            return str(int(total_days))
        except ValueError:
            return time_string
