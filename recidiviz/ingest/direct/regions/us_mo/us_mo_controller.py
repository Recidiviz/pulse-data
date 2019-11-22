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
from typing import Optional, List, Callable, Dict, Type, Iterable

from recidiviz.common.constants.entity_enum import EntityEnumMeta, EntityEnum
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_SID, US_MO_FBI, US_MO_OLN
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecidingBodyType, \
    StateSupervisionViolationResponseType, \
    StateSupervisionViolationResponseDecision
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
    SUPERVISION_SENTENCE_LENGTH_MONTHS, SUPERVISION_SENTENCE_LENGTH_DAYS, \
    PERIOD_OPEN_CODE, PERIOD_CASE_TYPE_CURRENT, PERIOD_CASE_TYPE_OPENING, \
    INCARCERATION_SENTENCE_PROJECTED_MIN_DATE, \
    INCARCERATION_SENTENCE_PROJECTED_MAX_DATE, \
    SUPERVISION_SENTENCE_PROJECTED_COMPLETION_DATE, PERIOD_RELEASE_DATE, \
    PERIOD_PURPOSE_FOR_INCARCERATION, PERIOD_OPEN_TYPE, PERIOD_START_DATE
from recidiviz.ingest.direct.state_shared_row_posthooks import \
    copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_normalize_county_codes_posthook, \
    gen_map_ymd_counts_to_max_length_field_posthook, \
    gen_set_is_life_sentence_hook, gen_convert_person_ids_to_external_id_objects
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, \
    StatePersonExternalId, StateSentenceGroup, StateCharge, \
    StateIncarcerationSentence, StateSupervisionSentence, \
    StateIncarcerationPeriod, StateSupervisionPeriod
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsMoController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_MO."""

    PERIOD_SEQUENCE_PRIMARY_COL_PREFIX = 'F1'
    RELEASE_TO_PAROLE = 'IT'
    RELEASE_TO_PROBATION = 'IC'

    FILE_TAGS = [
        'tak001_offender_identification',
        'tak040_offender_cycles',
        'tak022_tak023_tak025_tak026_offender_sentence_institution',
        'tak022_tak024_tak025_tak026_offender_sentence_probation',
        'tak158_tak023_incarceration_period_from_incarceration_sentence',
        'tak158_tak023_supervision_period_from_incarceration_sentence',
        'tak158_tak024_incarceration_period_from_supervision_sentence',
        'tak158_tak024_supervision_period_from_supervision_sentence',
    ]

    PRIMARY_COL_PREFIXES_BY_FILE_TAG = {
        'tak001_offender_identification': 'EK',
        'tak040_offender_cycles': 'DQ',
        'tak022_tak023_tak025_tak026_offender_sentence_institution': 'BS',
        'tak022_tak024_tak025_tak026_offender_sentence_probation': 'BS',
        'tak158_tak023_incarceration_period_from_incarceration_sentence': 'BT',
        'tak158_tak023_supervision_period_from_incarceration_sentence': 'BT',
        'tak158_tak024_incarceration_period_from_supervision_sentence': 'BU',
        'tak158_tak024_supervision_period_from_supervision_sentence': 'BU',
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

    # TODO(2604): Figure out if we should do anything special with these
    SENTENCE_MAGICAL_DATES = [
        '0', '20000000', '88888888', '99999999'
    ]
    PERIOD_MAGICAL_DATES = [
        '0', '99999999'
    ]

    SUPERVISION_CASE_TYPE_CODES: Dict[str, StateSupervisionType] = {
        'BP': StateSupervisionType.PAROLE,  # Board Parole
        'CR': StateSupervisionType.PAROLE,  # Conditional Release
        'IN': StateSupervisionType.PAROLE,  # Inmate
        'IP': StateSupervisionType.PAROLE,  # Interstate Parole
        'NA': StateSupervisionType.PAROLE,  # New Admission
        'DV': StateSupervisionType.PROBATION,  # Diversion
        'IC': StateSupervisionType.PROBATION,  # Interstate Probation
        'PB': StateSupervisionType.PROBATION,  # Former Probation Case
    }

    CASE_TYPE_CODES_THAT_FALL_BACK_TO_PROBATION: List[str] = [
        # These are kinds of cases that appear to be probation unless there's an
        # open reason or an opening case type that indicate parole

        'FC', 'MC', 'LC', 'UC', ' C'  # The space in the last one is intentional
    ]

    REVOCATION_OPEN_REASON_CODES: List[str] = [
        'FB', 'FF', 'FM', 'FN', 'FT'  # Various forms of field violations
    ]

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

        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            'XX',  # Unknown (Not Associated)
        ],
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
            'IB',  # Institutional Administrative
            'NA',  # New Admission
            'TR',  # Other State
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
            'IE',  # Institutional Escape
            'IW',  # Institutional Walkaway
        ],

        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
            'BP',  # Board Parole
            'FB',  # Field Administrative
            'CR',  # Conditional Release
            'IT',  # Institutional Release to Supervision
            'IC',  # Institutional Release to Probation
            'RT',  # Board Return
        ],
        StateIncarcerationPeriodReleaseReason.COURT_ORDER: [
            'IB',  # Institutional Administrative
            'OR',  # Off Records; Suspension
        ],
        StateIncarcerationPeriodReleaseReason.DEATH: [
            'DE',  # Death
        ],
        StateIncarcerationPeriodReleaseReason.ESCAPE: [
            'IE',  # Institutional Escape
            'IW',  # Institutional Walkaway
        ],
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
            'XX',  # Unknown (Not Associated)
            '??',  # Code Unknown
        ],
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
            'DC',  # Discharge
            'ID',  # Institutional Discharge
        ],

        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE: [
            'IB',  # Institutional Administrative
            'IC',  # Institutional Release to Probation
            'IT',  # Institutional Release to Supervision
        ],
        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
            'NA',  # New Admission
            'RT',  # Reinstatement
            'TR',  # Other State
        ],
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION: [
            'CI',  # Captured from Absconsion
        ],
        StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            '??',  # Code Unknown
        ],

        StateSupervisionPeriodTerminationReason.ABSCONSION: [
            'FA',  # Field Absconder
        ],
        StateSupervisionPeriodTerminationReason.DEATH: [
            'DE',  # Death
        ],
        StateSupervisionPeriodTerminationReason.DISCHARGE: [
            'CR',  # Conditional Release
            'DC',  # Discharge
            'EM',  # Inmate Release to EMP
            'ID',  # Institutional Discharge
            'RF',  # Inmate Release to RF
        ],
        StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: [
            'XX',  # Unknown (Not Associated)
            '??',  # Code Unknown
        ],
        StateSupervisionPeriodTerminationReason.REVOCATION: [
            'BB',  # Field to DAI-New Charge
            'BP',  # Board Parole
            'CN',  # Committed New Charge- No Vio
            'CR',  # Conditional Release (odd, yes, but appears to be true)
            'FB',  # Field Administrative
            'RT',  # Board Return
            'RV',  # Revocation
        ],
        StateSupervisionPeriodTerminationReason.SUSPENSION: [
            'OR',  # Off Records; Suspension
        ],
        StateSupervisionViolationResponseRevocationType.REINCARCERATION: [
            'S',  # Serve a Sentence
        ],
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION: [
            'O',  # 120-Day Shock
            'R',  # Regimented Disc Program
        ],
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON: [
            'A',  # Assessment
            'I',  # Inst Treatment Center
            'L',  # Long Term Drug Treatment
        ],
        StateSupervisionViolationResponseDecidingBodyType.COURT: [
            'CT',  # New Court Commitment
            'IS',  # Interstate Case
        ],
        StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD: [
            'BH',  # Board Holdover
            'BP',  # Board Parole
        ],
    }

    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {
        StateSupervisionType: ['INT'],  # Unknown meaning, rare

        # TODO(2647): Test to see if these omissions have calculation impact
        # Note, there are 312 records (as of 11-6-19) without a release reason
        # but with a release date
        StateIncarcerationPeriodReleaseReason: [
            'CN',  # Committed New Charge- No Vio: seems erroneous
            'RV',  # Revoked: seems erroneous
        ],

        StateSupervisionPeriodTerminationReason: [
            'IB',  # Institutional Administrative: seems erroneous
            'IT',  # Institutional Release to Supervision: seems erroneous
        ]
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
                self._gen_clear_magical_date_value(
                    'projected_max_release_date',
                    INCARCERATION_SENTENCE_PROJECTED_MAX_DATE,
                    self.SENTENCE_MAGICAL_DATES,
                    StateIncarcerationSentence),
                self._gen_clear_magical_date_value(
                    'projected_min_release_date',
                    INCARCERATION_SENTENCE_PROJECTED_MIN_DATE,
                    self.SENTENCE_MAGICAL_DATES,
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
                self._gen_clear_magical_date_value(
                    'projected_completion_date',
                    SUPERVISION_SENTENCE_PROJECTED_COMPLETION_DATE,
                    self.SENTENCE_MAGICAL_DATES,
                    StateSupervisionSentence),
                self.set_sentence_status,
                self._clear_zero_date_string
            ],
            'tak158_tak023_incarceration_period_from_incarceration_sentence': [
                self._gen_clear_magical_date_value(
                    'release_date',
                    PERIOD_RELEASE_DATE,
                    self.PERIOD_MAGICAL_DATES,
                    StateIncarcerationPeriod),
                self._set_incarceration_period_status,
                self._set_revocation_admission_reason,
                self._create_source_violation_response,
            ],
            'tak158_tak024_incarceration_period_from_supervision_sentence': [
                self._gen_clear_magical_date_value(
                    'release_date',
                    PERIOD_RELEASE_DATE,
                    self.PERIOD_MAGICAL_DATES,
                    StateIncarcerationPeriod),
                self._set_incarceration_period_status,
                self._set_revocation_admission_reason,
                self._create_source_violation_response,
            ],
            'tak158_tak023_supervision_period_from_incarceration_sentence': [
                self._gen_clear_magical_date_value(
                    'termination_date',
                    PERIOD_RELEASE_DATE,
                    self.PERIOD_MAGICAL_DATES,
                    StateSupervisionPeriod),
                self._set_supervision_period_status,
                self._set_supervision_type
            ],
            'tak158_tak024_supervision_period_from_supervision_sentence': [
                self._gen_clear_magical_date_value(
                    'termination_date',
                    PERIOD_RELEASE_DATE,
                    self.PERIOD_MAGICAL_DATES,
                    StateSupervisionPeriod),
                self._set_supervision_period_status,
                self._set_supervision_type
            ],
        }

        self.primary_key_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._generate_incarceration_sentence_id_coords,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._generate_supervision_sentence_id_coords,
            'tak158_tak023_incarceration_period_from_incarceration_sentence':
                self._generate_incarceration_period_id_coords,
            'tak158_tak024_incarceration_period_from_supervision_sentence':
                self._generate_incarceration_period_id_coords,
            'tak158_tak023_supervision_period_from_incarceration_sentence':
                self._generate_supervision_period_id_coords,
            'tak158_tak024_supervision_period_from_supervision_sentence':
                self._generate_supervision_period_id_coords,
        }

        self.ancestor_chain_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._sentence_group_ancestor_chain_override,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._sentence_group_ancestor_chain_override,
            'tak158_tak023_incarceration_period_from_incarceration_sentence':
                self._incarceration_sentence_ancestor_chain_override,
            'tak158_tak024_incarceration_period_from_supervision_sentence':
                self._supervision_sentence_ancestor_chain_override,
            'tak158_tak023_supervision_period_from_incarceration_sentence':
                self._incarceration_sentence_ancestor_chain_override,
            'tak158_tak024_supervision_period_from_supervision_sentence':
                self._supervision_sentence_ancestor_chain_override,
        }

    def _file_meets_file_line_limit(self, contents: Iterable[str]):
        """TODO(2634): Remove this MO-specific workaround in favor of a general
        solution."""
        file_size = sum(1 for _ in contents)
        return file_size <= self.file_split_line_limit + 1

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
        # pylint: disable=line-too-long
        if file_tag in [
                'tak022_tak023_tak025_tak026_offender_sentence_institution',
                'tak022_tak024_tak025_tak026_offender_sentence_probation',
                'tak158_tak023_incarceration_period_from_incarceration_sentence',
                'tak158_tak023_supervision_period_from_incarceration_sentence',
                'tak158_tak024_incarceration_period_from_supervision_sentence',
                'tak158_tak024_supervision_period_from_supervision_sentence'
        ]:
            return US_MO_DOC

        return None

    # TODO(1882): If yaml format supported raw values and multiple children of
    #  the same type, then this would be no-longer necessary.
    @staticmethod
    def tak001_offender_identification_hydrate_alternate_ids(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache) -> None:
        """Hydrates the alternate, non-DOC external ids for each person."""
        sid = row.get(STATE_ID, '').strip()
        fbi = row.get(FBI_ID, '').strip()
        oln = row.get(LICENSE_ID, '').strip()

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                external_ids_to_create = []
                if sid:
                    external_ids_to_create.append(
                        StatePersonExternalId(
                            state_person_external_id_id=sid,
                            id_type=US_MO_SID)
                    )
                if fbi:
                    external_ids_to_create.append(
                        StatePersonExternalId(
                            state_person_external_id_id=fbi,
                            id_type=US_MO_FBI)
                    )
                if oln:
                    external_ids_to_create.append(
                        StatePersonExternalId(
                            state_person_external_id_id=oln,
                            id_type=US_MO_OLN)
                    )

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
    def _gen_clear_magical_date_value(cls,
                                      field_name: str,
                                      column_code: str,
                                      magical_dates: List[str],
                                      sentence_type: Type[IngestObject]):

        def _clear_magical_date_values(_file_tag: str,
                                       row: Dict[str, str],
                                       extracted_objects: List[IngestObject],
                                       _cache: IngestObjectCache):
            date_str = row.get(column_code, None)

            for obj in extracted_objects:
                if isinstance(obj, sentence_type):
                    if obj.__getattribute__(field_name):
                        if date_str in magical_dates:
                            obj.__setattr__(field_name, None)

        return _clear_magical_date_values

    @classmethod
    def _set_revocation_admission_reason(cls,
                                         _file_tag: str,
                                         row: Dict[str, str],
                                         extracted_objects: List[IngestObject],
                                         _cache: IngestObjectCache):
        admission_reason = row.get(PERIOD_OPEN_CODE, None)
        open_type = row.get(PERIOD_OPEN_TYPE, None)

        if cls._is_due_to_revocation(admission_reason):
            for obj in extracted_objects:
                if isinstance(obj, StateIncarcerationPeriod):
                    revocation_reason = cls._revocation_reason(open_type)
                    if revocation_reason:
                        obj.__setattr__('admission_reason',
                                        revocation_reason.value)

    @classmethod
    def _revocation_reason(cls, open_type) \
            -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if open_type in ['BH', 'BP', 'CR']:
            return \
                StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
        if open_type in ['PB', 'PR']:
            return StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
        # TODO(2663): Set this accurately and don't assume PROBATION
        # for these minority cases (around 1% of all cases, it appears)
        return StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

    @classmethod
    def _create_source_violation_response(cls,
                                          _file_tag: str,
                                          row: Dict[str, str],
                                          extracted_objects: List[IngestObject],
                                          _cache: IngestObjectCache):
        """Creates a source supervision violation response directly on
        the incarceration period created in the given row, if appropriate."""
        admission_reason = row.get(PERIOD_OPEN_CODE, None)
        open_type = row.get(PERIOD_OPEN_TYPE, None)

        if cls._is_due_to_revocation(admission_reason):
            for obj in extracted_objects:
                if isinstance(obj, StateIncarcerationPeriod):
                    revocation_type = row.get(PERIOD_PURPOSE_FOR_INCARCERATION,
                                              None)
                    deciding_body_type = cls._deciding_body_type(open_type)

                    violation_response = \
                        obj.create_state_supervision_violation_response(
                            response_type=StateSupervisionViolationResponseType.
                            PERMANENT_DECISION.value,
                            decision=StateSupervisionViolationResponseDecision.
                            REVOCATION.value,
                            revocation_type=revocation_type,
                        )

                    if deciding_body_type == \
                            StateSupervisionViolationResponseDecidingBodyType.\
                            PAROLE_BOARD:
                        violation_response.deciding_body_type = \
                            deciding_body_type.value
                        # Only set response date if it's a parole board decision
                        # Otherwise, we don't know when it actually happens
                        violation_response.response_date = \
                            row.get(PERIOD_START_DATE, None)

    @classmethod
    def _is_due_to_revocation(cls, admission_reason) -> bool:
        return admission_reason \
               and admission_reason in cls.REVOCATION_OPEN_REASON_CODES

    @classmethod
    def _deciding_body_type(cls, open_type) \
            -> Optional[StateSupervisionViolationResponseDecidingBodyType]:
        if open_type in ['BH', 'BP']:
            return \
                StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD
        if open_type in ['CT', 'IS']:
            return StateSupervisionViolationResponseDecidingBodyType.COURT
        return None

    @classmethod
    def _set_incarceration_period_status(cls,
                                         _file_tag: str,
                                         _row: Dict[str, str],
                                         extracted_objects: List[IngestObject],
                                         _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date:
                    obj.__setattr__(
                        'status',
                        StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value)
                else:
                    obj.__setattr__(
                        'status',
                        StateIncarcerationPeriodStatus.IN_CUSTODY.value)

    @classmethod
    def _set_supervision_period_status(cls,
                                       _file_tag: str,
                                       _row: Dict[str, str],
                                       extracted_objects: List[IngestObject],
                                       _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if obj.termination_date:
                    obj.__setattr__(
                        'status',
                        StateSupervisionPeriodStatus.TERMINATED.value)
                else:
                    obj.__setattr__(
                        'status',
                        StateSupervisionPeriodStatus.UNDER_SUPERVISION.value)

    @classmethod
    def _set_supervision_type(cls,
                              _file_tag: str,
                              row: Dict[str, str],
                              extracted_objects: List[IngestObject],
                              _cache: IngestObjectCache):
        """
        Derives the proper supervision type from the various codes on the
        supervision period row data. The algorithm is as follows:

        1. If the row's open code is explicitly parole or probation,
        choose that.

        2. Otherwise, check the current case type. If the case type maps to a
        specific supervision type, choose that.

        3. Otherwise, check if the current case type is one that falls back to
        probation. If so, check the opening case type to see if it maps to a
        specific supervision type: if so choose that. Otherwise, choose
        probation.
        """
        def _set_type(final_supervision_type: StateSupervisionType):
            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionPeriod):
                    obj.__setattr__('supervision_type',
                                    final_supervision_type.value)

        supervision_type_from_open_code = None
        open_code = row.get(PERIOD_OPEN_CODE)

        if open_code == cls.RELEASE_TO_PAROLE:
            supervision_type_from_open_code = StateSupervisionType.PAROLE
        elif open_code == cls.RELEASE_TO_PROBATION:
            supervision_type_from_open_code = StateSupervisionType.PROBATION

        if supervision_type_from_open_code:
            _set_type(supervision_type_from_open_code)
            return

        supervision_type_from_case_type = None
        case_type_current = row.get(PERIOD_CASE_TYPE_CURRENT)

        if case_type_current:
            supervision_type_from_case_type = \
                cls.SUPERVISION_CASE_TYPE_CODES.get(case_type_current)

        if supervision_type_from_case_type:
            _set_type(supervision_type_from_case_type)
            return

        if case_type_current and case_type_current in \
                cls.CASE_TYPE_CODES_THAT_FALL_BACK_TO_PROBATION:
            case_type_opening = row.get(PERIOD_CASE_TYPE_OPENING)
            if not case_type_opening:
                return

            supervision_type_at_opening = cls.SUPERVISION_CASE_TYPE_CODES.get(
                case_type_opening)

            if supervision_type_at_opening == StateSupervisionType.PAROLE:
                _set_type(supervision_type_at_opening)
                return

            _set_type(StateSupervisionType.PROBATION)

    @classmethod
    def _sentence_group_ancestor_chain_override(cls,
                                                file_tag: str,
                                                row: Dict[str, str]):
        coords = cls._generate_sentence_group_id_coords(file_tag, row)
        return {
            coords.class_name: coords.field_value
        }

    @classmethod
    def _incarceration_sentence_ancestor_chain_override(cls,
                                                        file_tag: str,
                                                        row: Dict[str, str]):
        group_coords = cls._generate_sentence_group_id_coords(file_tag, row)
        sentence_coords = cls._generate_incarceration_sentence_id_coords(
            file_tag, row)

        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value
        }

    @classmethod
    def _supervision_sentence_ancestor_chain_override(cls,
                                                      file_tag: str,
                                                      row: Dict[str, str]):
        group_coords = cls._generate_sentence_group_id_coords(file_tag, row)
        sentence_coords = cls._generate_supervision_sentence_id_coords(
            file_tag, row)

        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value
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
    def _generate_supervision_period_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_supervision_period',
            'state_supervision_period_id',
            cls._generate_period_id(col_prefix, row))

    @classmethod
    def _generate_incarceration_period_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_incarceration_period',
            'state_incarceration_period_id',
            cls._generate_period_id(col_prefix, row))

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
    def _generate_period_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)
        period_seq_num = row[f'{cls.PERIOD_SEQUENCE_PRIMARY_COL_PREFIX}$SQN']
        return f'{sentence_group_id}-{period_seq_num}'

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
