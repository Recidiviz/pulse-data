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
"""Direct ingest controller implementation for US_ID."""
from typing import List, Dict, Optional, Callable

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumMapper, EnumIgnorePredicate
from recidiviz.common.constants.person_characteristics import Race, Ethnicity, Gender
from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity
from recidiviz.ingest.models.ingest_info import IngestObject, StateAssessment, StateIncarcerationSentence, \
    StateCharge, StateAgent, StateCourtCase, StateSentenceGroup, StateSupervisionSentence
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


# TODO(2999): Consider adding completion date to sentences even if the sentence is completed and we have a
#  `projected_completion_date` that has passed.
class UsIdController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_ID."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super(UsIdController, self).__init__(
            'us_id',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)
        self.enum_overrides = self.generate_enum_overrides()
        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender': [
                copy_name_to_alias,
                # When first parsed, the info object just has a single external id - the DOC id.
                gen_label_single_external_id_hook(US_ID_DOC),
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            ],
            'ofndr_tst_ofndr_tst_cert': [
                self._add_lsir_to_assessments,
                gen_label_single_external_id_hook(US_ID_DOC),
            ],
            'mittimus_judge_sentence_offense_sentprob_incarceration_sentences': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_statute_to_charge,
                self._add_judge_to_court_cases,
                self._set_extra_sentence_fields,
                self._set_generated_ids,
            ],
            'mittimus_judge_sentence_offense_sentprob_supervision_sentences': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_statute_to_charge,
                self._add_judge_to_court_cases,
                self._set_extra_sentence_fields,
                self._set_generated_ids,
            ],
        }
        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender': [],
            'ofndr_tst_ofndr_tst_cert': [],
            'mittimus_judge_sentence_offense_sentprob_incarceration_sentences': [],
            'mittimus_judge_sentence_offense_sentprob_supervision_sentences': [],
        }

    FILE_TAGS = [
        'offender',
        'ofndr_tst_ofndr_tst_cert',
        'mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
        'mittimus_judge_sentence_offense_sentprob_supervision_sentences',
    ]

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: ['A'],
        Race.BLACK: ['B'],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ['I'],
        Race.OTHER: ['O'],
        Race.EXTERNAL_UNKNOWN: ['U'],
        Race.WHITE: ['W'],

        Ethnicity.HISPANIC: ['H'],

        Gender.MALE: ['M'],
        Gender.FEMALE: ['F'],
        Gender.TRANS_MALE: ['X'],
        Gender.TRANS_FEMALE: ['Y'],
        Gender.EXTERNAL_UNKNOWN: ['U'],

        StateAssessmentLevel.LOW: ['Minimum'],
        StateAssessmentLevel.LOW_MEDIUM: ['Low-Medium'],
        StateAssessmentLevel.MEDIUM_HIGH: ['High-Medium'],
        StateAssessmentLevel.HIGH: ['Maximum'],

        StateSentenceStatus.COMMUTED: [
            'M',    # Commuted
        ],
        # TODO(2999): Consider breaking out these sentence status enums in our schema (
        #  vacated, sealed, early discharge, expired, etc)
        StateSentenceStatus.COMPLETED: [
            'C',    # Completed
            'D',    # Discharged
            'E',    # Expired
            'F',    # Parole Early Discharge
            'G',    # Dismissed
            'H',    # Post conviction relief.
            'L',    # Sealed
            'Q',    # Vacated conviction
            'S',    # Satisfied
            'V',    # Vacated Sentence
            'X',    # Rule 35 - Reduction of illegal or overly harsh sentence.
            'Z',    # Reduced to misdemeanor TODO(2999): When is this used?
        ],
        StateSentenceStatus.REVOKED: [
            'K',    # Revoked
        ],
        StateSentenceStatus.SERVING: [
            'I',    # Imposed
            'J',    # RJ To Court - Used for probation after treatment
            'N',    # Interstate Parole
            'O',    # Correctional Compact - TODO(2999): Get more info from ID.
            'P',    # Bond Appeal - unused, but present in ID status table.
            'R',    # Court Retains Jurisdiction - used when a person on a rider. TODO(2999): Whats the difference
                    # between this and 'W'?
            'T',    # Interstate probation - unused, but present in ID status table.
            'U',    # Unsupervised - probation
            'W',    # Witheld judgement - used when a person is on a rider.
            'Y',    # Drug Court - TODO(2999): Consider adding this as a court type.
        ],
        StateSentenceStatus.SUSPENDED: [
            'B',  # Suspended sentence - probation
        ],
    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def _get_file_tag_rank_list(self) -> List[str]:
        return self.FILE_TAGS

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Idaho-specific overrides for enum mappings."""
        base_overrides = super(UsIdController, self).get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides, self.ENUM_OVERRIDES, self.ENUM_IGNORES, self.ENUM_MAPPERS, self.ENUM_IGNORE_PREDICATES)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    @staticmethod
    def _add_lsir_to_assessments(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                obj.assessment_type = StateAssessmentType.LSIR.value

    @staticmethod
    def _add_statute_to_charge(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        statute_title = row.get('off_stat_title', '')
        statute_section = row.get('off_stat_sect', '')

        if not statute_title or not statute_section:
            return
        statute = f'{statute_title}-{statute_section}'

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.statute = statute

    @staticmethod
    def _add_judge_to_court_cases(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        judge_id = row.get('judge_cd', '')
        judge_name = row.get('judge_name', '')

        if not judge_id or not judge_name:
            return

        judge_to_create = StateAgent(
            state_agent_id=judge_id,
            full_name=judge_name,
            agent_type=StateAgentType.JUDGE.value)

        for obj in extracted_objects:
            if isinstance(obj, StateCourtCase):
                create_if_not_exists(judge_to_create, obj, 'judge')

    @staticmethod
    def _set_extra_sentence_fields(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        is_life = bool(row.get('lifer'))
        max_years = row.get('sent_max_yr')
        max_months = row.get('sent_max_mo')
        max_days = row.get('sent_max_da')
        min_years = row.get('sent_min_yr')
        min_months = row.get('sent_min_mo')
        min_days = row.get('sent_min_da')

        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence, StateSupervisionSentence)):
                start_date = obj.start_date
                max_time = parse_days_from_duration_pieces(
                    years_str=max_years, months_str=max_months, days_str=max_days, start_dt_str=start_date)
                min_time = parse_days_from_duration_pieces(
                    years_str=min_years, months_str=min_months, days_str=min_days, start_dt_str=start_date)

                if max_time:
                    obj.max_length = str(max_time)
                if min_time:
                    obj.min_length = str(min_time)
            if isinstance(obj, StateIncarcerationSentence):
                obj.is_life = str(is_life)
            if isinstance(obj, StateSupervisionSentence):
                obj.supervision_type = StateSupervisionType.PROBATION.value

    @staticmethod
    def _set_generated_ids(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        person_id = row.get('docno', '')
        sentence_group_id = row.get('incrno', '')
        sentence_id = row.get('sent_no', '')
        court_case_id = row.get('caseno', '')

        for obj in extracted_objects:
            if isinstance(obj, StateSentenceGroup):
                obj.state_sentence_group_id = f'{person_id}-{sentence_group_id}'

            if isinstance(obj, StateIncarcerationSentence):
                obj.state_incarceration_sentence_id = f'{person_id}-{sentence_id}'
            if isinstance(obj, StateSupervisionSentence):
                obj.state_supervision_sentence_id = f'{person_id}-{sentence_id}'

            # Only one charge per sentence so recycle sentence id for the charge.
            if isinstance(obj, StateCharge):
                obj.state_charge_id = f'{person_id}-{sentence_id}'

            if isinstance(obj, StateCourtCase):
                obj.state_court_case_id = f'{person_id}-{court_case_id}'
