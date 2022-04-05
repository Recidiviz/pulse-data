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
from typing import Optional, List, Callable, Dict, Type
import datetime
import logging
import re

from recidiviz.common.constants.entity_enum import EntityEnumMeta, EntityEnum
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_SID, US_MO_FBI, US_MO_OLN
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateIncarcerationPeriodStatus, \
    StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecidingBodyType, \
    StateSupervisionViolationResponseDecision
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType
from recidiviz.common.date import munge_date_string
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces, \
    parse_yyyymmdd_date, parse_days
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import \
    create_if_not_exists, update_overrides_from_maps
from recidiviz.ingest.direct.regions.us_mo.us_mo_constants import \
    INCARCERATION_SENTENCE_MIN_RELEASE_TYPE, \
    INCARCERATION_SENTENCE_LENGTH_YEARS, INCARCERATION_SENTENCE_LENGTH_MONTHS, \
    INCARCERATION_SENTENCE_LENGTH_DAYS, CHARGE_COUNTY_CODE, \
    SENTENCE_COUNTY_CODE, INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS, \
    INCARCERATION_SENTENCE_START_DATE, SENTENCE_OFFENSE_DATE, \
    SENTENCE_COMPLETED_FLAG, SENTENCE_STATUS_CODE, STATE_ID, FBI_ID, \
    LICENSE_ID, SUPERVISION_SENTENCE_LENGTH_YEARS, \
    TAK076_PREFIX, TAK291_PREFIX, VIOLATION_REPORT_ID_PREFIX, \
    VIOLATION_KEY_SEQ, CITATION_ID_PREFIX, CITATION_KEY_SEQ, DOC_ID, CYCLE_ID, \
    SENTENCE_KEY_SEQ, FIELD_KEY_SEQ, \
    SUPERVISION_SENTENCE_LENGTH_MONTHS, SUPERVISION_SENTENCE_LENGTH_DAYS, \
    PERIOD_OPEN_CODE, INCARCERATION_SENTENCE_PROJECTED_MIN_DATE, \
    INCARCERATION_SENTENCE_PROJECTED_MAX_DATE, \
    SUPERVISION_SENTENCE_PROJECTED_COMPLETION_DATE, PERIOD_RELEASE_DATE, \
    PERIOD_PURPOSE_FOR_INCARCERATION, PERIOD_OPEN_CODE_SUBTYPE, \
    PERIOD_START_DATE, SUPERVISION_VIOLATION_VIOLATED_CONDITIONS, \
    SUPERVISION_VIOLATION_TYPES, SUPERVISION_VIOLATION_RECOMMENDATIONS, \
    PERIOD_CLOSE_CODE_SUBTYPE, PERIOD_CLOSE_CODE, PERIOD_CASE_TYPE_CURRENT
from recidiviz.ingest.direct.state_shared_row_posthooks import \
    copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_normalize_county_codes_posthook, \
    gen_map_ymd_counts_to_max_length_field_posthook, \
    gen_set_is_life_sentence_hook, \
    gen_convert_person_ids_to_external_id_objects, \
    gen_set_field_as_concatenated_values_hook
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, \
    StatePersonExternalId, StateSentenceGroup, StateCharge, \
    StateIncarcerationSentence, StateSupervisionSentence, \
    StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolation, \
    StateSupervisionViolatedConditionEntry, \
    StateSupervisionViolationTypeEntry, \
    StateSupervisionViolationResponseDecisionEntry
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsMoController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_MO."""

    PERIOD_SEQUENCE_PRIMARY_COL_PREFIX = 'F1'

    FILE_TAGS = [
        'tak001_offender_identification',
        'tak040_offender_cycles',
        'tak022_tak023_tak025_tak026_offender_sentence_institution',
        'tak022_tak024_tak025_tak026_offender_sentence_probation',
        'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence',
        'tak158_tak023_tak026_supervision_period_from_incarceration_sentence',
        'tak158_tak024_tak026_incarceration_period_from_supervision_sentence',
        'tak158_tak024_tak026_supervision_period_from_supervision_sentence',
        'tak028_tak042_tak076_tak024_violation_reports',
        'tak291_tak292_tak024_citations',
    ]

    PRIMARY_COL_PREFIXES_BY_FILE_TAG = {
        'tak001_offender_identification': 'EK',
        'tak040_offender_cycles': 'DQ',
        'tak022_tak023_tak025_tak026_offender_sentence_institution': 'BS',
        'tak022_tak024_tak025_tak026_offender_sentence_probation': 'BS',
        # pylint: disable=line-too-long
        'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence': 'BT',
        'tak158_tak023_tak026_supervision_period_from_incarceration_sentence': 'BT',
        'tak158_tak024_tak026_incarceration_period_from_supervision_sentence': 'BU',
        'tak158_tak024_tak026_supervision_period_from_supervision_sentence': 'BU',
        'tak028_tak042_tak076_tak024_violation_reports': 'BY',
        'tak291_tak292_tak024_citations': 'JT',
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

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        StateChargeClassificationType.INFRACTION: ['L'],  # Local/ordinance

        StateSupervisionType.EXTERNAL_UNKNOWN: [
            # Codes originating from F1$CTC - Case Type Current
            'XX',  # Unknown (Not Associated)
        ],
        StateSupervisionType.PAROLE: [
            # Codes originating from F1$CTC - Case Type Current
            'BP',   # Board Parole
            'CR',   # Conditional Release
            'IN',   # Inmate
            'IP',   # Interstate Parole
            'NA',   # New Admission
        ],
        StateSupervisionType.PROBATION: [
            # Sentence-related codes originating from F1$CTC - Case Type Current
            'BND',  # Bond Supervision (no longer used)
            'CPR',  # Court Parole (a form of probation)
            'DFP',  # Deferred Prosecution
            'IPB',  # Interstate Compact Probation
            'IPR',  # Interstate Compact Probation
            'SES',  # Suspended Execution of Sentence (Probation)
            'SIS',  # Suspended Imposition of Sentence (Probation)

            # Period-related codes originating from F1$CTC - Case Type Current
            'DV',   # Diversion
            'IC',   # Interstate Probation
            'PB',   # Former Probation Case
            'FC',   # Felony Court Case
            'LC',   # Local/Ordinance Court Case
            'MC',   # Misdemeanor Court Case
            'UC',   # Unknown Court Case
            ' C',   # Court Case Check Type
        ],
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            'XX',  # Unknown (Not Associated)
            'XX-BP',
            'XX-CR',
            'XX-IW',
            'XX-PB',
            'XX-PR',
            'XX-XX',
        ],
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
            'IB-BP',
            'IB-CR',
            'IB-CT',
            'IB-IB',
            'IB-IS',
            'IB-IW',
            'IB-PB',
            'IB-PR',
            'IB-RF',
            'NA',  # New Admission
            'NA-C',
            'NA-BH',
            'NA-BP',
            'NA-CR',
            'NA-CT',
            'NA-DV',
            'NA-EM',
            'NA-FC',
            'NA-IB',
            'NA-IC',
            'NA-IE',
            'NA-IN',
            'NA-IP',
            'NA-IS',
            'NA-IW',
            'NA-MC',
            'NA-NA',
            'NA-RF',
            'NA-UC',
            'NA-XX',
            'TR',  # Other State
            'TR-BP',
            'TR-IW',
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
            'IE-IE',  # Institutional Escape
            'IW-IE',  # Institutional Walkaway
            'IW-IW',
        ],
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION: [
            'FB-BP',
            'FB-CR',
            'FF-BP',
            'FF-CR',
            'FM-BP',
            'FM-CR',
            'FN-BP',
            'FN-CR',
            'FT-BP',
            'FT-CR',
            'IB-BH',  # Institutional Administrative
        ],
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION: [
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FB-EM',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FB-IE',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FB-IW',
            'FB-PB',
            'FB-PR',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FB-RF',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FF-EM',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FF-IE',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FF-IW',
            'FF-PB',
            'FF-PR',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FF-RF',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FM-IW',
            'FM-PB',
            'FM-PR',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FM-RF',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FN-IE',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FN-IW',
            'FN-PB',
            'FN-PR',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FT-EM',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FT-IE',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FT-IW',
            'FT-PB',
            'FT-PR',
            # TODO(2663): Set this accurately and don't assume PROBATION
            'FT-RF',
            'NA-PB',   # New Admission - Probation Revocation
            'NA-PR',   # New Admission - Probation Revocation Return
        ],
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
            'BP-FF',  # Board Parole
            'BP-FM',
            'BP-FT',
            'FB-TR',  # Field Administrative
            'CR-FF',  # Conditional Release
            'CR-FT',
            'IT-BD',  # Institutional Release to Supervision
            'IT-BH',
            'IT-BP',
            'IT-CD',
            'IT-CR',
            'IT-EM',
            'IT-IC',
            'IT-IN',
            'IT-RF',
            'IC-IC',  # Institutional Release to Probation
            'RT-BH',  # Board Return
        ],
        StateIncarcerationPeriodReleaseReason.COURT_ORDER: [
            'IB-EI',  # Institutional Administrative
            'IB-ER',
            'IB-IB',
            'IB-RB',
            'IB-RR',
            'IB-TR',
            'IB-XX',
            'OR-OR',  # Off Records; Suspension
        ],
        StateIncarcerationPeriodReleaseReason.DEATH: [
            'DE-DE',  # Death
            'DE-XX',
        ],
        StateIncarcerationPeriodReleaseReason.ESCAPE: [
            'IE-IE',  # Institutional Escape
            'IE-XX',
            'IW-IW',  # Institutional Walkaway
            'IW-XX',
        ],
        StateIncarcerationPeriodReleaseReason.EXECUTION: [
            'DE-EX',  # Execution
        ],
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
            'XX-XX',  # Unknown (Not Associated)
            '??-??',  # Code Unknown
            '??-XX',
        ],
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
            'DC-DC',  # Discharge
            'DC-DO',  # Inst. Converted-Inactive
            'DC-XX',  # Discharge - Unknown
            'ID-DC',  # Institutional Discharge - Discharge
            'ID-DO',  # Institutional Discharge - Other
            'ID-DR',  # Institutional Discharge - Director's Release
            'ID-ID',  # Institutional Discharge - Institutional Discharge
            'ID-PD',  # Institutional Discharge - Pardoned
            'ID-RR',  # Institutional Discharge - Reversed and Remanded
            'ID-XX',  # Institutional Discharge - Unknown
        ],

        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION: [
            'O',  # 120-Day Shock
            'R',  # Regimented Disc Program
        ],
        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON: [
            'A',  # Assessment
            'I',  # Inst Treatment Center
            'L',  # Long Term Drug Treatment
        ],

        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE: [
            'IB',  # Institutional Administrative
            'IC',  # Institutional Release to Probation
            'IC-EM',
            'IC-RF',
            'IT',  # Institutional Release to Supervision
            'IT-EM',
            'IT-RF',
        ],
        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
            'NA',  # New Admission
            'NA-NA',
            'TR',  # Other State
            'TR-EM',
        ],
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION: [
            'RT',  # Reinstatement
            'RT-EM',
        ],
        StateSupervisionPeriodAdmissionReason.ABSCONSION: [
            '65O1010',  # Offender declared absconder - from TAK026 BW$SCD
            '65O1020',  # Offender declared absconder - from TAK026 BW$SCD
            '65O1030',  # Offender declared absconder - from TAK026 BW$SCD
            '99O2035',  # Offender declared absconder - from TAK026 BW$SCD
            '65L9100',  # Offender declared absconder - from TAK026 BW$SCD
        ],
        StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION: [
            'CI',  # Captured from Absconsion
            '65N9500',  # Offender re-engaged - from TAK026 BW$SCD
        ],
        StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            '??',  # Code Unknown
        ],

        StateSupervisionPeriodTerminationReason.ABSCONSION: [
            'FA-FA',  # Field Absconder

            '65O1010',  # Offender declared absconder - from TAK026 BW$SCD
            '65O1020',  # Offender declared absconder - from TAK026 BW$SCD
            '65O1030',  # Offender declared absconder - from TAK026 BW$SCD
            '99O2035',  # Offender declared absconder - from TAK026 BW$SCD
            '65L9100',  # Offender declared absconder - from TAK026 BW$SCD
        ],
        StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION: [
            '65N9500',  # Offender re-engaged - from TAK026 BW$SCD
        ],
        StateSupervisionPeriodTerminationReason.DEATH: [
            'DE-DE',  # Death
        ],
        StateSupervisionPeriodTerminationReason.DISCHARGE: [
            # Discharge - encompasses a collection of statuses including those
            # that reference 'commutation', 'discharge', and 'expiration'
            # TODO(2738): Derive the periods tables from TAK026 so we can
            #  capture this nuance.
            'DC-DC',

            # 'Director's Discharge'
            'DC-DR',

            # 'Resentenced-Field Completion'
            'DC-OR',

            # 'Field Pardon'
            'DC-PD',

            # Parole/Conditional Release Completion-Retroactive
            'ID-ID',
        ],
        StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: [
            'XX-XX',  # Unknown (Not Associated)
            '??',  # Code Unknown
            '??-??',
            '??-RR',
        ],
        StateSupervisionPeriodTerminationReason.REVOCATION: [
            'BB-FN',  # Field to DAI-New Charge
            'BP-FF',  # Board Parole
            'BP-FM',
            'BP-FN',
            'BP-FT',
            'CN-FB',  # Committed New Charge- No Vio
            'CN-FN',
            'CN-NV',
            'CR-FF',  # Conditional Release (odd, yes, but appears to be true)
            'CR-FM',
            'CR-FN',
            'CR-FT',
            'FB',     # Field Administrative
            'FB-DC',
            'FB-EI',
            'FB-TR',
            'FB-UK',

            # Parole/Conditional Release Completion-Lifetime Supervision
            # Note: This seems to be immediately followed by a new admission to
            # a lifetime (un-terminated) supervision period
            'ID-DC',

            'RT-BH',  # Board Return
            'RT-ER',
            'RT-FF',
            'RT-FM',
            'RT-FT',
            'RF-FB',  # Return to RF (Residential facility)
            'RF-FF',
            'RF-FM',
            'RF-FT',
            'RV-FF',  # Revocation
            'RV-FM',
            'RV-FN',
            'RV-FT',
            'RV-OR',
            'RV-UK',
        ],
        StateSupervisionPeriodTerminationReason.SUSPENSION: [
            'OR-FA',  # Off Records; Suspension
            'OR-IW',
            'OR-OR',

            # Reverse/Remand Completion or Discharge (case sent back to courts)
            'DC-RR',
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
        StateSupervisionViolationType.ABSCONDED: ['A'],
        StateSupervisionViolationType.ESCAPED: ['E'],
        StateSupervisionViolationType.FELONY: ['F'],
        StateSupervisionViolationType.MISDEMEANOR: ['M'],
        StateSupervisionViolationType.MUNICIPAL: ['O'],
        StateSupervisionViolationType.TECHNICAL: ['T'],

        StateSupervisionViolationResponseDecision.REVOCATION: [
            'A',  # Capias
            'I',  # Inmate Return
            'R',  # Revocation
            'CO',  # Court Ordered Detention Sanction
        ],
        StateSupervisionViolationResponseDecision.CONTINUANCE: ['C'],
        StateSupervisionViolationResponseDecision.DELAYED_ACTION: ['D'],
        StateSupervisionViolationResponseDecision.EXTENSION: ['E'],
        StateSupervisionViolationResponseDecision.SUSPENSION: ['S'],
        StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED: [
            'RN'  # SIS revoke to SES
        ],
        StateSupervisionViolationResponseDecision.SERVICE_TERMINATION: ['T'],
    }

    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {
        StateSupervisionType: ['INT'],  # Unknown meaning, rare

        # TODO(2647): Test to see if these omissions have calculation impact
        # Note, there are 312 records (as of 11-6-19) without a release reason
        # but with a release date
        StateIncarcerationPeriodReleaseReason: [
            'CN-FB',  # Committed New Charge- No Vio: seems erroneous
            'CN-NV',
            'RV-FF',  # Revoked: seems erroneous
            'RV-FM',
            'RV-FT',

            # Inmate Release to EMP (electronic monitoring program). Doesn't
            # make sense as a Supervision termination reason and only 1
            # occurrence of any of these in the last 10 years.
            'EM-FB',
            'EM-FF',
            'EM-FM',
            'EM-FT',
        ],
        StateSpecializedPurposeForIncarceration: [
            'S',  # Serving Sentence
        ],
        StateSupervisionPeriodTerminationReason: [
            'IB-TR',  # Institutional Administrative: seems erroneous
            'IT-FB',  # Institutional Release to Supervision: seems erroneous
        ],
        StateSupervisionViolationResponseDecision: [
            'NOREC',  # No Recommendation
        ],
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

        self.enum_overrides = self.generate_enum_overrides()
        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {}

        incarceration_period_row_posthooks = [
            self._gen_clear_magical_date_value(
                'release_date',
                PERIOD_RELEASE_DATE,
                self.PERIOD_MAGICAL_DATES,
                StateIncarcerationPeriod),
            self._set_incarceration_period_status,
            gen_set_field_as_concatenated_values_hook(
                StateIncarcerationPeriod, 'admission_reason',
                [PERIOD_OPEN_CODE, PERIOD_OPEN_CODE_SUBTYPE]),
            gen_set_field_as_concatenated_values_hook(
                StateIncarcerationPeriod, 'release_reason',
                [PERIOD_CLOSE_CODE, PERIOD_CLOSE_CODE_SUBTYPE]),
            self._adjust_incarceration_period_admission_and_release_reasons,
            self._create_source_violation_response,
        ]
        supervision_period_row_posthooks = [
            self._gen_clear_magical_date_value(
                'termination_date',
                PERIOD_RELEASE_DATE,
                self.PERIOD_MAGICAL_DATES,
                StateSupervisionPeriod),
            self._set_supervision_period_status,
            gen_set_field_as_concatenated_values_hook(
                StateSupervisionPeriod, 'admission_reason',
                [PERIOD_OPEN_CODE, PERIOD_OPEN_CODE_SUBTYPE]),
            gen_set_field_as_concatenated_values_hook(
                StateSupervisionPeriod, 'termination_reason',
                [PERIOD_CLOSE_CODE, PERIOD_CLOSE_CODE_SUBTYPE]),
        ]

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
                self.tak022_tak023_set_parole_eligibility_date,
                self.set_charge_id_from_sentence_id,
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
                self._clear_zero_date_string,
                self.set_charge_id_from_sentence_id,
            ],
            # pylint: disable=line-too-long
            'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence':
                incarceration_period_row_posthooks,
            'tak158_tak024_tak026_incarceration_period_from_supervision_sentence':
                incarceration_period_row_posthooks,
            'tak158_tak023_tak026_supervision_period_from_incarceration_sentence':
                supervision_period_row_posthooks,
            'tak158_tak024_tak026_supervision_period_from_supervision_sentence':
                supervision_period_row_posthooks,
            'tak028_tak042_tak076_tak024_violation_reports': [
                self._gen_violation_response_type_posthook(
                    StateSupervisionViolationResponseType.VIOLATION_REPORT),
                self._set_deciding_body_as_supervising_officer,
                self._set_violated_conditions_on_violation,
                self._set_violation_type_on_violation,
                self._set_recommendations_on_violation_response,
                self._set_violation_response_id_from_violation,
                self._set_finally_formed_date_on_response,
            ],
            'tak291_tak292_tak024_citations': [
                self._gen_violation_response_type_posthook(
                    StateSupervisionViolationResponseType.CITATION),
                self._set_deciding_body_as_supervising_officer,
                self._set_violated_conditions_on_violation,
                self._set_violation_response_id_from_violation,
                self._set_finally_formed_date_on_response,
            ],
        }

        self.primary_key_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._generate_incarceration_sentence_id_coords,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._generate_supervision_sentence_id_coords,
            # pylint: disable=line-too-long
            'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence':
                self._generate_incarceration_period_id_coords,
            'tak158_tak024_tak026_incarceration_period_from_supervision_sentence':
                self._generate_incarceration_period_id_coords,
            'tak158_tak023_tak026_supervision_period_from_incarceration_sentence':
                self._generate_supervision_period_id_coords,
            'tak158_tak024_tak026_supervision_period_from_supervision_sentence':
                self._generate_supervision_period_id_coords,
            'tak028_tak042_tak076_tak024_violation_reports':
                self._generate_supervision_violation_id_coords_for_reports,
            'tak291_tak292_tak024_citations':
                self._generate_supervision_violation_id_coords_for_citations,
        }

        self.ancestor_chain_override_by_file: Dict[str, Callable] = {
            'tak022_tak023_tak025_tak026_offender_sentence_institution':
                self._sentence_group_ancestor_chain_override,
            'tak022_tak024_tak025_tak026_offender_sentence_probation':
                self._sentence_group_ancestor_chain_override,
            # pylint: disable=line-too-long
            'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence':
                self._incarceration_sentence_ancestor_chain_override,
            'tak158_tak024_tak026_incarceration_period_from_supervision_sentence':
                self._supervision_sentence_ancestor_chain_override,
            'tak158_tak023_tak026_supervision_period_from_incarceration_sentence':
                self._incarceration_sentence_ancestor_chain_override,
            'tak158_tak024_tak026_supervision_period_from_supervision_sentence':
                self._supervision_sentence_ancestor_chain_override,
            'tak028_tak042_tak076_tak024_violation_reports':
                self._supervision_violation_report_ancestor_chain_override,
            'tak291_tak292_tak024_citations':
                self._supervision_violation_citation_ancestor_chain_override,
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

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Missouri-specific overrides for enum mappings."""
        base_overrides = super(UsMoController, self).get_enum_overrides()
        return update_overrides_from_maps(base_overrides,
                                          self.ENUM_OVERRIDES,
                                          self.ENUM_IGNORES)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        # pylint: disable=line-too-long
        if file_tag in [
                'tak022_tak023_tak025_tak026_offender_sentence_institution',
                'tak022_tak024_tak025_tak026_offender_sentence_probation',
                'tak158_tak023_tak026_incarceration_period_from_incarceration_sentence',
                'tak158_tak023_tak026_supervision_period_from_incarceration_sentence',
                'tak158_tak024_tak026_incarceration_period_from_supervision_sentence',
                'tak158_tak024_tak026_supervision_period_from_supervision_sentence',
                'tak028_tak042_tak076_tak024_violation_reports',
                'tak291_tak292_tak024_citations',
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
    def set_charge_id_from_sentence_id(
            cls,
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                sentence_id = obj.state_incarceration_sentence_id
                charges = obj.state_charges
            elif isinstance(obj, StateSupervisionSentence):
                sentence_id = obj.state_supervision_sentence_id
                charges = obj.state_charges
            else:
                continue

            if len(charges) > 1:
                raise ValueError(f"Expected a maximum of one charge per "
                                 f"sentence, but found sentence {sentence_id} "
                                 f"with {str(len(charges))} charges")

            if charges:
                charges[0].state_charge_id = sentence_id

    @classmethod
    def _gen_violation_response_type_posthook(
            cls,
            response_type: StateSupervisionViolationResponseType) -> Callable:
        def _set_response_type(
                _file_tag: str,
                _row: Dict[str, str],
                extracted_objects: List[IngestObject],
                _cache: IngestObjectCache):
            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionViolation):
                    for response in obj.state_supervision_violation_responses:
                        response.response_type = response_type.value
        return _set_response_type

    @classmethod
    def _set_deciding_body_as_supervising_officer(
            cls,
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for response in obj.state_supervision_violation_responses:
                    response.deciding_body_type = \
                        StateSupervisionViolationResponseDecidingBodyType.\
                        SUPERVISION_OFFICER.value

    # TODO(2701): Remove posthook in place of general child-id setting solution.
    @classmethod
    def _set_violation_response_id_from_violation(
            cls,
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                violation_id = obj.state_supervision_violation_id
                for response in obj.state_supervision_violation_responses:
                    response.state_supervision_violation_response_id = \
                        violation_id

    @classmethod
    def _set_finally_formed_date_on_response(
            cls,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Finally formed documents are the ones that are no longer in a draft
        state.

        Updates the SupervisionViolationResponses in |extracted_objects| based
        on whether or not a finally formed date is present in the given |row|.
        """
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                julian_date_str = row.get(
                    'FINAL_FORMED_CREATE_DATE', None)
                finally_formed_date = cls.mo_julian_date_to_iso(julian_date_str)

                for response in obj.state_supervision_violation_responses:
                    is_draft = True
                    if finally_formed_date:
                        response.response_date = finally_formed_date
                        is_draft = False
                    response.is_draft = str(is_draft)

    @classmethod
    def _set_violated_conditions_on_violation(
            cls,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Manually adds StateSupervisionViolatedConditionEntries to
        StateSupervisionViolations.
        """
        conditions_txt = row.get(SUPERVISION_VIOLATION_VIOLATED_CONDITIONS, '')
        if conditions_txt == '':
            return
        conditions = conditions_txt.split(',')

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for condition in conditions:
                    obj.violated_conditions = None
                    vc = StateSupervisionViolatedConditionEntry(
                        condition=condition)
                    create_if_not_exists(
                        vc, obj, 'state_supervision_violated_conditions')

    @classmethod
    def _set_violation_type_on_violation(
            cls,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Manually adds StateSupervisionViolationTypeEntries to
        StateSupervisionViolations.
        """
        violation_types_txt = row.get(SUPERVISION_VIOLATION_TYPES, '')
        if violation_types_txt == '':
            return
        violation_types = list(violation_types_txt)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for violation_type in violation_types:
                    vt = StateSupervisionViolationTypeEntry(
                        violation_type=violation_type)
                    create_if_not_exists(
                        vt, obj, 'state_supervision_violation_types')

    def _set_recommendations_on_violation_response(
            self,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Manually adds StateSupervisionViolationResponses to
        StateSupervisionViolations.
        """
        recommendation_txt = row.get(SUPERVISION_VIOLATION_RECOMMENDATIONS, '')
        # Return if there is no recommendation, or if the text explicitly refers
        # to either "No Recommendation".
        if recommendation_txt in ('', 'NOREC'):
            return

        if recommendation_txt in ('CO', 'RN'):
            # CO and RN are the only recommendations we process that are more
            # than one letter, and they do not occur with any other
            # recommendations.
            recommendations = [recommendation_txt]
        else:
            # If not one of the above recommendations, any number of single
            # character recommendations can be provided.
            recommendations = list(recommendation_txt)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for response in obj.state_supervision_violation_responses:
                    for recommendation in recommendations:
                        revocation_type = \
                            self._revocation_type_str_from_recommendation(
                                recommendation)
                        rec = \
                            StateSupervisionViolationResponseDecisionEntry(
                                decision=recommendation,
                                revocation_type=revocation_type)
                        create_if_not_exists(
                            rec,
                            response,
                            'state_supervision_violation_response_decisions')

    def _revocation_type_str_from_recommendation(
            self,
            recommendation: str
    ) -> Optional[str]:
        """Returns a str value of a
        StateSupervisionViolationResponseRevocationType corresponding to the
        given recommendation, or None if one does not apply.
        """

        revocation_type = None
        if recommendation in ('A', 'I', 'R'):
            revocation_type = \
                StateSupervisionViolationResponseRevocationType \
                .REINCARCERATION.value
        elif recommendation == 'CO':
            revocation_type = \
                StateSupervisionViolationResponseRevocationType \
                .TREATMENT_IN_PRISON.value

        recommendation_is_revocation = \
            self.enum_overrides.parse(
                recommendation,
                StateSupervisionViolationResponseDecision) \
            == StateSupervisionViolationResponseDecision.REVOCATION

        if recommendation_is_revocation and not revocation_type:
            raise ValueError(
                f'Unclassified revocation type for REVOCATION '
                f'recommendation [{recommendation}]')

        if not recommendation_is_revocation and revocation_type:
            raise ValueError(
                f'Non-revocation recommendation [{recommendation}] should not '
                f'have revocation type [{revocation_type}]')

        return revocation_type

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

    def _adjust_incarceration_period_admission_and_release_reasons(
            self,
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """
        Incarceration periods may have an 'IB-BH' (Institutional Administrative
        - Board Hold) open status in one of three cases:
            1) if the person is currently being held temporarily, pending a
                parole board decision, or
            2) if the person was held, pending a decision, then not revoked and
                returned to supervision, or
            3) if their parole has been revoked.

        When the person is revoked, the body status table (TAK158) that we
        currently use for period information will not update to have a different
        open reason, but the current case type (F1$CTC) will update to indicate
        that they are no longer being held temporarily pending review. This hook
        makes sure we mark periods that are still board holds as temporary
        holds. If the person has been released and the case type never changed
        away from 'BP', then we just mark them as released from a temporary
        hold.

        For more context, see #2665.
        """
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if self._is_temporary_board_hold_period(row, obj):
                    obj.admission_reason = \
                        StateIncarcerationPeriodAdmissionReason.\
                        TEMPORARY_CUSTODY.value
                    if obj.release_reason:
                        obj.release_reason = \
                            StateIncarcerationPeriodReleaseReason.\
                            RELEASED_FROM_TEMPORARY_CUSTODY.value

    @staticmethod
    def _is_temporary_board_hold_period(row: Dict[str, str],
                                        ip: StateIncarcerationPeriod):
        return ip.admission_reason == 'IB-BH' and \
               row[PERIOD_CASE_TYPE_CURRENT] == 'BP'

    def _create_source_violation_response(self,
                                          _file_tag: str,
                                          row: Dict[str, str],
                                          extracted_objects: List[IngestObject],
                                          _cache: IngestObjectCache):
        """Creates a source supervision violation response directly on
        the incarceration period created in the given row, if appropriate."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                revocation_admission_reason = \
                    self._revocation_admission_reason(obj.admission_reason)
                if revocation_admission_reason:
                    revocation_type = row.get(PERIOD_PURPOSE_FOR_INCARCERATION,
                                              None)
                    deciding_body_type = \
                        self._deciding_body_type(revocation_admission_reason)

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

    def _revocation_admission_reason(
            self,
            ip_admission_reason: Optional[str]
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if not ip_admission_reason:
            return None

        ip_admission_reason_enum = \
            self.get_enum_overrides().parse(
                ip_admission_reason,
                StateIncarcerationPeriodAdmissionReason)

        if ip_admission_reason_enum is None:
            return None

        if not isinstance(ip_admission_reason_enum,
                          StateIncarcerationPeriodAdmissionReason):
            raise ValueError(
                f'Unexpected enum type returned: [{ip_admission_reason_enum}]')

        if ip_admission_reason_enum in {
                StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
        }:
            return ip_admission_reason_enum
        return None

    @classmethod
    def _deciding_body_type(
            cls,
            revocation_admission_reason: StateIncarcerationPeriodAdmissionReason
    ) -> Optional[StateSupervisionViolationResponseDecidingBodyType]:
        if revocation_admission_reason == \
                StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
            return \
                StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD
        if revocation_admission_reason == \
                StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
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
    def _supervision_violation_report_ancestor_chain_override(
            cls,
            file_tag: str,
            row: Dict[str, str]):
        group_coords = cls._generate_sentence_group_id_coords(file_tag, row)
        if row.get(f'{TAK076_PREFIX}${FIELD_KEY_SEQ}', '0') == '0':
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                file_tag, row, TAK076_PREFIX)
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                file_tag, row, TAK076_PREFIX)
        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_violation_citation_ancestor_chain_override(
            cls,
            file_tag: str,
            row: Dict[str, str]):
        group_coords = cls._generate_sentence_group_id_coords(file_tag, row)
        if row.get(f'{TAK291_PREFIX}${FIELD_KEY_SEQ}', '0') == '0':
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                file_tag, row, TAK291_PREFIX)
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                file_tag, row, TAK291_PREFIX)
        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
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
            row: Dict[str, str],
            col_prefix: str = None) -> IngestFieldCoordinates:
        if not col_prefix:
            col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_supervision_sentence',
            'state_supervision_sentence_id',
            cls._generate_sentence_id(col_prefix, row))

    @classmethod
    def _generate_incarceration_sentence_id_coords(
            cls,
            file_tag: str,
            row: Dict[str, str],
            col_prefix: str = None) -> IngestFieldCoordinates:
        if not col_prefix:
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
    def _generate_supervision_violation_id_coords_for_reports(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_supervision_violation',
            'state_supervision_violation_id',
            cls._generate_supervision_violation_id_with_report_prefix(
                col_prefix, row)
        )

    @classmethod
    def _generate_supervision_violation_id_coords_for_citations(
            cls,
            file_tag: str,
            row: Dict[str, str]) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(file_tag)
        return IngestFieldCoordinates(
            'state_supervision_violation',
            'state_supervision_violation_id',
            cls._generate_supervision_violation_id_with_citation_prefix(
                col_prefix, row)
        )

    @classmethod
    def _generate_sentence_group_id(cls,
                                    col_prefix: str,
                                    row: Dict[str, str]) -> str:
        doc_id = row[f'{col_prefix}${DOC_ID}']
        cyc_id = row[f'{col_prefix}${CYCLE_ID}']
        return f'{doc_id}-{cyc_id}'

    @classmethod
    def _generate_sentence_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)
        sen_seq_num = row[f'{col_prefix}${SENTENCE_KEY_SEQ}']
        return f'{sentence_group_id}-{sen_seq_num}'

    @classmethod
    def _generate_period_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)

        # TODO(2728): The SQN is potentially not a stable ID if status
        #  information gets backdated and the SQN numbers generated in the
        #  roll-up shift.
        subcycle_seq_num = row[f'{cls.PERIOD_SEQUENCE_PRIMARY_COL_PREFIX}$SQN']
        start_status_seq_num = row['START_STATUS_SEQ_NUM']
        return f'{sentence_group_id}-{subcycle_seq_num}-{start_status_seq_num}'

    @classmethod
    def _generate_supervision_violation_id_with_report_prefix(
            cls, col_prefix: str, row: Dict[str, str]) -> str:
        return cls._generate_supervision_violation_id_with_prefix(
            col_prefix, row, TAK076_PREFIX, VIOLATION_REPORT_ID_PREFIX,
            VIOLATION_KEY_SEQ)

    @classmethod
    def _generate_supervision_violation_id_with_citation_prefix(
            cls, col_prefix: str, row: Dict[str, str]) -> str:
        return cls._generate_supervision_violation_id_with_prefix(
            col_prefix, row, TAK291_PREFIX, CITATION_ID_PREFIX,
            CITATION_KEY_SEQ)

    @classmethod
    def _generate_supervision_violation_id_with_prefix(
            cls, col_prefix: str, row: Dict[str, str],
            xref_prefix: str,
            violation_id_prefix: str,
            violation_key_seq: str) -> str:
        violation_seq_num = row[f'{col_prefix}${violation_key_seq}']
        group_id = cls._generate_sentence_group_id(xref_prefix, row)

        # TODO(1883): Remove use of SEO (sentence_seq_id) and FSO (Field Seq No)
        # once extractor supports multiple paths to entities with the same id.
        # Until then, we need to keep all ids for SupervisionViolations unique
        # through the data extractor and proto conversion.
        #
        # Currently, the SEO is removed from the violation ids as a
        # pre-processing hook in entity matching. From there, matching
        # violations can be properly merged together.
        sentence_seq_id = row[f'{xref_prefix}${SENTENCE_KEY_SEQ}']
        field_seq_no = row[f'{xref_prefix}${FIELD_KEY_SEQ}']

        return f'{group_id}-{violation_id_prefix}{violation_seq_num}-' \
               f'{sentence_seq_id}-{field_seq_no}'

    @classmethod
    def _generate_supervision_violation_id(
            cls,
            col_prefix: str,
            row: Dict[str, str],
            violation_id_prefix: str) -> str:
        group_id = cls._generate_sentence_group_id(TAK076_PREFIX, row)
        sentence_seq_id = row[f'{TAK076_PREFIX}${SENTENCE_KEY_SEQ}']
        violation_seq_num = row[f'{col_prefix}${VIOLATION_KEY_SEQ}']
        return f'{group_id}-{violation_id_prefix}{violation_seq_num}-' \
               f'{sentence_seq_id}'

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

    JULIAN_DATE_STR_REGEX = re.compile(r'(\d?\d\d)(\d\d\d)')

    @classmethod
    def mo_julian_date_to_iso(cls, julian_date_str: Optional[str]) \
            -> Optional[str]:
        """
        Parse julian-formatted date strings used by MO in a number of DB
        fields that encode a date using the number of years since 1900
        concatenated with the number of days since Jan 1 of that year
        (1-indexed). Returns the date in ISO date format.

        E.g.:
             85001 -> 1985-01-01
            118365 -> 2018-12-31
        """
        if not julian_date_str or int(julian_date_str) == 0:
            return None

        match = re.match(cls.JULIAN_DATE_STR_REGEX, julian_date_str)
        if match is None:
            logging.warning("Could not parse MO date [%s]", julian_date_str)
            return None

        years_since_1900 = int(match.group(1))
        days_since_jan_1 = int(match.group(2)) - 1

        date = datetime.date(year=(years_since_1900 + 1900), month=1, day=1) + \
            datetime.timedelta(days=days_since_jan_1)
        return date.isoformat()
