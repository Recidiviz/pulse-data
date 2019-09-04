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

"""Direct ingest controller implementation for us_nd."""
import json
import os
import re
from typing import List, Optional, Dict, Callable, cast, Pattern, Tuple

from recidiviz.common import ncic
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.external_id_types import US_ND_ELITE, \
    US_ND_SID
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentClass
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import \
    StateCourtCaseStatus
from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentOutcomeType, StateIncarcerationIncidentType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionLevel, StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseType, \
    StateSupervisionViolationResponseRevocationType
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.extractor.csv_data_extractor import \
    IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestInfo, IngestObject, \
    StatePerson, StateIncarcerationSentence, StateSentenceGroup, \
    StateIncarcerationPeriod, StatePersonExternalId, StateAssessment, \
    StateCharge, StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateAgent, StateIncarcerationIncidentOutcome, StateIncarcerationIncident, \
    StateAlias, StateSupervisionSentence
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache

_SUPERVISION_SENTENCE_ID_SUFFIX = '_SUPERVISION'
_TEMPORARY_PRIMARY_ID = '_TEMPORARY_PRIMARY_ID'
_DOCSTARS_NEGATIVE_PATTERN: Pattern = re.compile(r'^\((?P<value>-?\d+)\)$')


class UsNdController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for us_nd."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None):
        super(UsNdController, self).__init__(
            'us_nd',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path)

        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {
            'elite_offenderidentifier': [self._normalize_id_fields],
            'elite_alias': [self._normalize_id_fields],
            'elite_offendersentences': [self._normalize_id_fields],
            'elite_offendersentenceterms': [self._normalize_id_fields],
            'elite_offenderchargestable': [self._normalize_id_fields],
            'elite_orderstable': [self._normalize_id_fields],
            'elite_externalmovements': [self._normalize_id_fields],
        }

        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'elite_alias': [self._clear_temporary_alias_primary_ids],
            'elite_offenders': [self._rationalize_race_and_ethnicity],
            'elite_offenderidentifier': [self._normalize_external_id],
            'elite_offendersentenceaggs': [self._rationalize_max_length],
            'elite_offendersentences': [self._rationalize_life_sentence],
            'elite_offendersentenceterms': [
                self._concatenate_elite_length_periods,
                self._convert_to_supervision_sentence],
            'elite_offenderchargestable': [
                self._parse_elite_charge_classification,
                self._set_elite_charge_status,
                self._rationalize_controlling_charge
            ],
            'elite_orderstable': [self._set_judge_agent_type],
            'elite_externalmovements': [self._process_external_movement],
            'elite_offense_in_custody_and_pos_report_data': [
                self._rationalize_incident_type,
                self._rationalize_outcome_type,
                self._set_punishment_length_days,
                self._set_location_within_facility,
                self._set_incident_outcome_id],
            'docstars_offenders': [self._rationalize_race_and_ethnicity,
                                   self._label_external_id_as_elite,
                                   self._enrich_addresses,
                                   self._enrich_sorac_assessments],
            'docstars_offendercasestable': [
                self._concatenate_docstars_length_periods,
                self._record_revocation,
                self._set_judge_agent_type],
            'docstars_offensestable': [
                self._parse_docstars_charge_classification
            ],
            'docstars_lsichronology': [self._process_lsir_assessments],
        }

        self.primary_key_override_by_file: Dict[str, Callable] = {
            'elite_offendersentences': _generate_sentence_primary_key,
            'elite_offendersentenceterms': _generate_sentence_primary_key,
            'elite_externalmovements': _generate_period_primary_key,
            'elite_offenderchargestable': _generate_charge_primary_key,
            'elite_alias': _generate_alias_temporary_primary_key,
            'elite_offense_in_custody_and_pos_report_data':
                _generate_incident_primary_key,
        }

        self.ancestor_key_override_by_file: Dict[str, Callable] = {
            'elite_offendersentenceterms':
                _fill_in_incarceration_sentence_parent_id,
            'elite_offenderchargestable':
                _fill_in_incarceration_sentence_parent_id
        }

        self.files_to_set_with_empty_values = [
            'elite_offendersentenceterms'
        ]

    def _get_file_tag_rank_list(self) -> List[str]:
        # NOTE: The order of ingest here is important!
        # Do not change unless you know what you're doing!
        return [
            # Elite - incarceration-focused
            'elite_offenders',
            'elite_offenderidentifier',
            'elite_alias',
            'elite_offenderbookingstable',
            'elite_offendersentenceaggs',
            'elite_offendersentences',
            'elite_offendersentenceterms',
            'elite_offenderchargestable',
            'elite_orderstable',
            'elite_externalmovements',
            'elite_offense_in_custody_and_pos_report_data',

            # Docstars - supervision-focused
            'docstars_offenders',
            'docstars_offendercasestable',
            'docstars_offensestable',
            'docstars_lsichronology',
            # TODO(1918): Integrate bed assignment / location history
        ]

    def _normalize_id_fields(self, row: Dict[str, str]):
        """A number of ID fields come in as comma-separated strings in some of
        the files. This function converts those id column values to a standard
        format without decimals or commas before the row is processed.

        If the ID column is in the proper format, this function will no-op.
        """

        for field_name in {'ROOT_OFFENDER_ID',
                           'ALIAS_OFFENDER_ID',
                           'OFFENDER_ID',
                           'OFFENDER_BOOK_ID',
                           'ORDER_ID'}:
            if field_name in row:
                row[field_name] = \
                    self._decimal_str_as_int_str(row[field_name])

    @staticmethod
    def _decimal_str_as_int_str(dec_str: str) -> str:
        """Converts a comma-separated string representation of an integer into
        a string representing a simple integer with no commas.

        E.g. _decimal_str_as_int_str('1,234.00') -> '1234'
        """
        if not dec_str:
            return dec_str

        return str(int(float(dec_str.replace(',', ''))))

    def _get_row_pre_processors_for_file(self, file: str) -> List[Callable]:
        return self.row_pre_processors_by_file.get(file, [])

    def _get_row_post_processors_for_file(self, file: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file, [])

    def _get_file_post_processors_for_file(
            self, file: str) -> List[Callable]:
        if file.startswith('elite'):
            return [self._convert_elite_person_ids_to_external_id_objects]
        if file.startswith('docstars'):
            return [self._convert_docstars_person_ids_to_external_id_objects]

        raise ValueError(f"File [{file}] doesn't have a known external id "
                         f"post-processor")

    def _get_ancestor_key_override_for_file(
            self, file: str) -> Optional[Callable]:
        return self.ancestor_key_override_by_file.get(file, None)

    def _get_primary_key_override_for_file(
            self, file: str) -> Optional[Callable]:
        return self.primary_key_override_by_file.get(file, None)

    def _get_files_to_set_with_empty_values(self) -> List[str]:
        return self.files_to_set_with_empty_values

    @staticmethod
    def _convert_elite_person_ids_to_external_id_objects(
            ingest_info: IngestInfo,
            cache: Optional[IngestObjectCache],
    ):
        _convert_person_ids_to_external_id_objects(ingest_info,
                                                   cache,
                                                   US_ND_ELITE)

    @staticmethod
    def _convert_docstars_person_ids_to_external_id_objects(
            ingest_info: IngestInfo,
            cache: Optional[IngestObjectCache],
    ):
        _convert_person_ids_to_external_id_objects(ingest_info,
                                                   cache,
                                                   US_ND_SID)

    @staticmethod
    def _clear_temporary_alias_primary_ids(
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAlias):
                if extracted_object.state_alias_id == _TEMPORARY_PRIMARY_ID:
                    obj = \
                        cache.get_object_by_id('state_alias',
                                               _TEMPORARY_PRIMARY_ID)

                    if extracted_object != obj:
                        raise ValueError(
                            'Cached object with temp id differs from extracted '
                            'object')

                    extracted_object.state_alias_id = None
                    cache.clear_object_by_id(
                        'state_alias',
                        _TEMPORARY_PRIMARY_ID)

    @staticmethod
    def _rationalize_race_and_ethnicity(_row: Dict[str, str],
                                        extracted_objects: List[IngestObject],
                                        _cache: IngestObjectCache):
        """For a person whose provided race is HISPANIC, we set the ethnicity to
        HISPANIC, and the race will be set to OTHER by enum overrides."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                for person_race in extracted_object.state_person_races:
                    if person_race.race in {'5', 'HIS'}:
                        extracted_object.create_state_person_ethnicity(
                            ethnicity=Ethnicity.HISPANIC.value)

    @staticmethod
    def _rationalize_life_sentence(row: Dict[str, str],
                                   extracted_objects: List[IngestObject],
                                   _cache: IngestObjectCache):
        is_life_sentence = row['SENTENCE_CALC_TYPE'] == 'LIFE'

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationSentence):
                extracted_object.__setattr__('is_life', str(is_life_sentence))

    @staticmethod
    def _rationalize_max_length(row: Dict[str, str],
                                extracted_objects: List[IngestObject],
                                _cache: IngestObjectCache):
        """PRI means Prison sentence, which is redundant. It is only ever
        used in a small number of erroneous cases long ago that had no data on
        max sentence length available."""
        is_life_sentence = row['MAX_TERM'] == 'LIFE'
        is_redundant_pri = row['MAX_TERM'] == 'PRI'

        for extracted_object in extracted_objects:
            if (is_life_sentence or is_redundant_pri) \
                    and isinstance(extracted_object, StateSentenceGroup):
                extracted_object.__setattr__('max_length', None)
            if is_life_sentence:
                extracted_object.__setattr__('is_life', str(is_life_sentence))

    @staticmethod
    def _normalize_external_id(_row: Dict[str, str],
                               extracted_objects: List[IngestObject],
                               _cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                id_type = f"US_ND_{extracted_object.id_type}"
                extracted_object.__setattr__('id_type', id_type)

    @staticmethod
    def _concatenate_elite_length_periods(row: Dict[str, str],
                                          extracted_objects: List[IngestObject],
                                          _cache: IngestObjectCache):
        """For a sentence row in Elite that contains distinct YEARS, MONTHS,
        and DAYS fields, compose these into a single string and set it on the
        appropriate length field."""
        units = ['Y', 'M', 'D']
        days = row.get('DAYS', None)
        if days is not None:
            days = days.replace(',', '')
        components = [row.get('YEARS', None), row.get('MONTHS', None), days]

        _concatenate_length_periods(units, components, extracted_objects)

    @staticmethod
    def _convert_to_supervision_sentence(row: Dict[str, str],
                                         extracted_objects: List[IngestObject],
                                         cache: IngestObjectCache):
        """Finds any incarceration sentence objects created by a row in the
        sentence terms file of Elite that are actually probation sentences,
        and converts them to supervision sentence objects."""
        is_supervision = row['SENTENCE_TERM_CODE'] == 'SUSP'
        if not is_supervision:
            return

        sentence_group_id = row['OFFENDER_BOOK_ID']
        sentence_group = cache.get_object_by_id('state_sentence_group',
                                                sentence_group_id)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationSentence) \
                    and extracted_object in \
                    sentence_group.state_incarceration_sentences:
                _convert_to_supervision_sentence(extracted_object,
                                                 sentence_group,
                                                 cache)

    @staticmethod
    def _process_external_movement(row: Dict[str, str],
                                   extracted_objects: List[IngestObject],
                                   _cache: IngestObjectCache):
        """Sets admission- or release-specific fields based on whether this
        movement represents an admission into or a release from a particular
        facility."""
        is_admission = row['DIRECTION_CODE'] == 'IN'
        movement_date = row['MOVEMENT_DATE']
        movement_reason = row['MOVEMENT_REASON_CODE']
        to_facility = row['TO_AGY_LOC_ID']
        from_facility = row['FROM_AGY_LOC_ID']

        # TODO(2002): If this edge is a transfer in or out of a hospital or
        #  other non-prison facility, create extra proper edges in and out
        #  of those facilities.
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationPeriod):
                if is_admission:
                    extracted_object.admission_date = movement_date
                    extracted_object.admission_reason = movement_reason
                    extracted_object.facility = to_facility
                else:
                    extracted_object.release_date = movement_date
                    extracted_object.release_reason = movement_reason
                    extracted_object.facility = from_facility

    @staticmethod
    def _label_external_id_as_elite(_row: Dict[str, str],
                                    extracted_objects: List[IngestObject],
                                    _cache: IngestObjectCache):
        """For a person we are seeing in Docstars with an ITAGROOT_ID
        referencing a corresponding record in Elite, mark that external id as
        having type ELITE."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                extracted_object.__setattr__('id_type', US_ND_ELITE)

    @staticmethod
    def _enrich_addresses(row: Dict[str, str],
                          extracted_objects: List[IngestObject],
                          _cache: IngestObjectCache):
        """Concatenate address, city, state, and zip information."""
        city = row.get('CITY', None)
        state = row.get('STATE', None)
        postal = row.get('ZIP', None)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                address = extracted_object.current_address
                full_address = ', '.join(filter(None,
                                                [address, city, state, postal]))
                extracted_object.__setattr__('current_address', full_address)

    @staticmethod
    def _enrich_sorac_assessments(_row: Dict[str, str],
                                  extracted_objects: List[IngestObject],
                                  _cache: IngestObjectCache):
        """For SORAC assessments in Docstars' incoming person data, add metadata
        we can infer from context."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.__setattr__(
                    'assessment_class', StateAssessmentClass.RISK.value)
                extracted_object.__setattr__(
                    'assessment_type', StateAssessmentType.SORAC.value)

    @staticmethod
    def _process_lsir_assessments(row: Dict[str, str],
                                  extracted_objects: List[IngestObject],
                                  _cache: IngestObjectCache):
        """For rich LSIR historical data from Docstars, manually process
        individual domain and question values."""
        domain_labels = ['CHtotal', 'EETotal', 'FnclTotal', 'FMTotal',
                         'AccomTotal', 'LRTotal', 'Cptotal', 'Adtotal',
                         'EPTotal', 'AOTotal']
        total_score, domain_scores = _get_lsir_domain_scores_and_sum(
            row, domain_labels)

        question_labels = ['Q18value', 'Q19value', 'Q20value', 'Q21value',
                           'Q23Value', 'Q24Value', 'Q25Value', 'Q27Value',
                           'Q31Value', 'Q39Value', 'Q40Value', 'Q51value',
                           'Q52Value']
        question_scores = _get_lsir_question_scores(row, question_labels)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                assessment = cast(StateAssessment, extracted_object)
                assessment.assessment_class = StateAssessmentClass.RISK.value
                assessment.assessment_type = StateAssessmentType.LSIR.value
                assessment.assessment_score = str(total_score)

                lsi_metadata = {**domain_scores, **question_scores}
                assessment.assessment_metadata = json.dumps(lsi_metadata)

    @staticmethod
    def _concatenate_docstars_length_periods(
            row: Dict[str, str], extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """For a sentence row in Docstars that contains distinct SENT_YY and
        SENT_MM fields, compose these into a single string of days and set it
        on the appropriate length field."""
        years = row.get('SENT_YY', None)
        months = row.get('SENT_MM', None)

        # It appears a recent change to Docstars files started passing negative
        # values inside of parentheses instead of after a '-' sign
        match = re.match(_DOCSTARS_NEGATIVE_PATTERN, months)
        if match is not None:
            value = match.group('value')
            months = "-" + value

        if not years and not months:
            return

        effective_date = row['PAROLE_FR']

        total_days = parse_days_from_duration_pieces(
            years_str=years,
            months_str=months,
            days_str=None,
            start_dt_str=effective_date)

        for extracted_object in extracted_objects:
            if total_days and isinstance(extracted_object,
                                         StateSupervisionSentence):
                day_string = "{}d".format(total_days)
                extracted_object.__setattr__('max_length', day_string)

    @staticmethod
    def _record_revocation(row: Dict[str, str],
                           extracted_objects: List[IngestObject],
                           _cache: IngestObjectCache):
        """Captures information in a Docstars cases row that indicates a
        revocation, and fills out the surrounding data model accordingly."""
        revocation_occurred = \
            row.get('REV_DATE', None) or row.get('RevoDispo', None)

        # These three flags are either '0' (False) or '-1' (True)
        # That -1 may now be (1) after a recent Docstars change
        revocation_for_new_offense = \
            row.get('REV_NOFF_YN', None) in ['-1', '(1)']
        revocation_for_absconsion = \
            row.get('REV_ABSC_YN', None) in ['-1', '(1)']
        revocation_for_technical = \
            row.get('REV_TECH_YN', None) in ['-1', '(1)']

        terminating_officer_id = row.get('TERMINATING_OFFICER', None)

        def _get_ncic_codes():
            first = row.get('NEW_OFF', None)
            second = row.get('NEW_OFF2', None)
            third = row.get('NEW_OFF3', None)

            return [code for code in [first, second, third] if code]

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionViolation):
                extracted_object = cast(
                    StateSupervisionViolation, extracted_object)
                if revocation_occurred:
                    # TODO(1763): Add support for multiple violation types
                    # on one violation
                    violation_type = None
                    if revocation_for_new_offense:
                        violation_type = \
                            StateSupervisionViolationType.FELONY.value

                        ncic_codes = _get_ncic_codes()
                        violent_flags = [ncic.get_is_violent(code)
                                         for code in ncic_codes]
                        extracted_object.is_violent = str(any(violent_flags))
                    elif revocation_for_absconsion:
                        violation_type = \
                            StateSupervisionViolationType.ABSCONDED.value
                    elif revocation_for_technical:
                        violation_type = \
                            StateSupervisionViolationType.TECHNICAL.value
                    extracted_object.violation_type = violation_type
                else:
                    extracted_object.violation_type = None

            if isinstance(extracted_object, StateSupervisionViolationResponse):
                extracted_object = cast(
                    StateSupervisionViolationResponse, extracted_object)
                if revocation_occurred:
                    extracted_object.response_type = \
                        StateSupervisionViolationResponseType\
                        .PERMANENT_DECISION.value
                    extracted_object.decision = \
                        StateSupervisionViolationResponseDecision\
                        .REVOCATION.value
                    if terminating_officer_id:
                        extracted_object.create_state_agent(
                            state_agent_id=terminating_officer_id,
                            agent_type=StateAgentType.SUPERVISION_OFFICER.value
                        )

    @staticmethod
    def _rationalize_incident_type(_row: Dict[str, str],
                                   extracted_objects: List[IngestObject],
                                   _cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncident):
                if extracted_object.incident_type == 'MISC':
                    # TODO(1948): Infer incident type from offense code cols
                    # when marked as MISC
                    extracted_object.incident_type = None

    @staticmethod
    def _rationalize_outcome_type(row: Dict[str, str],
                                  extracted_objects: List[IngestObject],
                                  _cache: IngestObjectCache):
        finding = row['FINDING_DESCRIPTION']

        if finding == 'NOT GUILTY':
            for extracted_object in extracted_objects:
                if isinstance(extracted_object,
                              StateIncarcerationIncidentOutcome):
                    extracted_object.outcome_type = \
                        StateIncarcerationIncidentOutcomeType.NOT_GUILTY.value

    @staticmethod
    def _set_punishment_length_days(row: Dict[str, str],
                                    extracted_objects: List[IngestObject],
                                    _cache: IngestObjectCache):

        months = row['SANCTION_MONTHS']
        days = row['SANCTION_DAYS']

        if not months and not days:
            # No punishment length info
            return

        effective_date = row['EFFECTIVE_DATE']

        total_days = parse_days_from_duration_pieces(
            years_str=None,
            months_str=months,
            days_str=days,
            start_dt_str=effective_date)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncidentOutcome):
                extracted_object.punishment_length_days = str(total_days)

    @staticmethod
    def _set_location_within_facility(row: Dict[str, str],
                                      extracted_objects: List[IngestObject],
                                      _cache: IngestObjectCache):

        facility = row['AGY_LOC_ID']
        facility_with_loc = row['OMS_OWNER_V_OIC_INCIDENTS_INT_LOC_DESCRIPTION']

        if not facility or not facility_with_loc:
            return

        facility_prefix = f'{facility}-'
        if not facility_with_loc.startswith(facility_prefix):
            return

        # Strip facility prefix
        location = facility_with_loc[len(facility_prefix):]
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncident):
                extracted_object.location_within_facility = location

    @staticmethod
    def _set_incident_outcome_id(row: Dict[str, str],
                                 extracted_objects: List[IngestObject],
                                 _cache: IngestObjectCache):
        incident_id = _generate_incident_id(row)
        sanction_seq = row['SANCTION_SEQ']

        if not incident_id or not sanction_seq:
            return

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncidentOutcome):
                extracted_object.state_incarceration_incident_outcome_id = \
                    '-'.join([incident_id, sanction_seq])

    @staticmethod
    def _set_judge_agent_type(_row: Dict[str, str],
                              extracted_objects: List[IngestObject],
                              _cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAgent):
                extracted_object.agent_type = StateAgentType.JUDGE.value

    @staticmethod
    def _set_elite_charge_status(_row: Dict[str, str],
                                 extracted_objects: List[IngestObject],
                                 _cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                # Note: If we hear about a charge in Elite, the person has
                # already been sentenced.
                extracted_object.status = ChargeStatus.SENTENCED.value

    @staticmethod
    def _rationalize_controlling_charge(row: Dict[str, str],
                                        extracted_objects: List[IngestObject],
                                        _cache: IngestObjectCache):
        status = row.get('CHARGE_STATUS', None)
        is_controlling = status and status.upper() in ['C', 'CT']

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                extracted_object.is_controlling = str(is_controlling)

    @staticmethod
    def _parse_elite_charge_classification(
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        classification_str = row['OFFENCE_TYPE']
        _parse_charge_classification(classification_str,
                                     extracted_objects)

    @staticmethod
    def _parse_docstars_charge_classification(
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        classification_str = row['LEVEL']
        _parse_charge_classification(classification_str,
                                     extracted_objects)

    def get_enum_overrides(self) -> EnumOverrides:
        """Provides North Dakota-specific overrides for enum mappings.

        The keys herein are raw strings directly from the source data, and the
        values are the enums that they are mapped to within our schema. The
        values are a list because a particular string may be used in multiple
        distinct columns in the source data.
        """
        overrides: Dict[EntityEnum, List[str]] = {
            Gender.FEMALE: ['2'],
            Gender.MALE: ['1'],

            Race.WHITE: ['1'],
            Race.BLACK: ['2'],
            Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ['3', 'NAT'],
            Race.ASIAN: ['4'],
            Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: ['6', 'HAW'],
            Race.OTHER: ['5', 'HIS', 'MUL'],

            StateSentenceStatus.COMPLETED: ['C'],
            StateSentenceStatus.SERVING: ['O'],

            StateChargeClassificationType.FELONY: ['IF'],
            StateChargeClassificationType.MISDEMEANOR: ['IM'],

            StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN:
                ['OTHER'],
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION:
                ['PREA', 'RAB', 'DEF'],
            StateIncarcerationPeriodAdmissionReason.
            RETURN_FROM_ERRONEOUS_RELEASE: ['READMN'],
            StateIncarcerationPeriodAdmissionReason.TRANSFER:
                ['CONF', 'FED', 'HOSP', 'HOSPS', 'HOSPU', 'INT', 'JOB',
                 'OOS', 'PROG', 'RB', 'SUPL'],
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
                ['PARL'],
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
                ['NPRB', 'NPROB', 'RPRB'],

            StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: ['OTHER'],
            StateIncarcerationPeriodReleaseReason.COMPASSIONATE: ['COM'],
            StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE:
                ['PRB', 'PV'],
            StateIncarcerationPeriodReleaseReason.COURT_ORDER: ['CO'],
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: ['XSNT'],
            StateIncarcerationPeriodReleaseReason.TRANSFER:
                ['CONF', 'HOSP', 'HOSPS', 'HOSPU', 'INT', 'JOB', 'PROG', 'RB',
                 'SUPL'],

            StateSupervisionType.HALFWAY_HOUSE: ['COMMUNITY PLACEMENT PGRM'],
            StateSupervisionType.PAROLE: ['SSOP'],

            StateSupervisionViolationResponseRevocationType.REINCARCERATION:
                ['COUNTY JAIL SENTENCE',
                 'COUNTY JAIL SENTENCE FOLLOWED BY PROBATION',
                 'DOCR INMATE SENTENCE',
                 'DOCR INMATE SENTENCE FOLLOWED BY PROBATION',
                 'RESENTENCED TO FIVE YEARS MORE'],

            StateIncarcerationIncidentType.DISORDERLY_CONDUCT:
                ['DAMAGE', 'DISCON', 'ESCAPE_ATT', 'INS', 'SEXCONTACT',
                 'UNAUTH', 'NON'],
            StateIncarcerationIncidentType.CONTRABAND:
                ['CONT', 'GANG', 'GANGREL', 'PROP', 'TOB'],
            StateIncarcerationIncidentType.MINOR_OFFENSE: ['SWIFT'],
            StateIncarcerationIncidentType.POSITIVE: ['POSREPORT'],
            StateIncarcerationIncidentType.REPORT: ['STAFFREP'],
            StateIncarcerationIncidentType.PRESENT_WITHOUT_INFO: ['CONV'],
            StateIncarcerationIncidentType.VIOLENCE:
                ['IIASSAULT', 'IIASSAULTINJ', 'IIFIGHT', 'FGHT', 'IISUBNOINJ',
                 'ISASSAULT', 'ISASSAULTINJ', 'ISSUBNOINJ', 'SEXUAL', 'THREAT'],

            StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS:
                ['LCP', 'LOR', 'LCO', 'LVPVV', 'LOP', 'LVP', 'LPJES', 'FREM',
                 'RTQ', 'UREST', 'LPH', 'LSE', 'CCF', 'SREM'],
            StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY:
                ['RES', 'PAY', 'FIN', 'PRO', 'LJB'],
            StateIncarcerationIncidentOutcomeType.SOLITARY:
                ['SEG', 'DD', 'RAS'],
            StateIncarcerationIncidentOutcomeType.TREATMENT: ['RTX'],
            StateIncarcerationIncidentOutcomeType.DISMISSED: ['DSM'],
            StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION: ['RSA'],
            StateIncarcerationIncidentOutcomeType.MISCELLANEOUS:
                ['COMB', 'DELETED', 'RED', 'TRA'],
            StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR: ['EXD'],
            StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS: ['LG', 'STP'],
            StateIncarcerationIncidentOutcomeType.WARNING: ['WAR', 'NS'],

            StateSupervisionLevel.MINIMUM: ['1'],
            StateSupervisionLevel.MEDIUM: ['2'],
            # 6 is Drug Court which is max with specific rules
            StateSupervisionLevel.MAXIMUM: ['3', '6'],
            StateSupervisionLevel.DIVERSION: ['7'],
            StateSupervisionLevel.INTERSTATE_COMPACT: ['9'],
            # 0 means no calculated level, 5 means not classified yet
            StateSupervisionLevel.EXTERNAL_UNKNOWN: ['0', '5'],

            StateSupervisionPeriodTerminationReason.ABSCONSION: ['13'],
            StateSupervisionPeriodTerminationReason.DEATH: ['11'],
            StateSupervisionPeriodTerminationReason.DISCHARGE:
                ['1', '2', '3', '8', '12', '15', '16', '17', '18'],
            StateSupervisionPeriodTerminationReason.EXPIRATION:
                ['4', '7', '19', '20'],
            StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: ['14'],
            StateSupervisionPeriodTerminationReason.REVOCATION: ['9', '10'],
            StateSupervisionPeriodTerminationReason.SUSPENSION: ['5', '6'],
        }

        ignores: Dict[EntityEnumMeta, List[str]] = {
            # TODO(2305): What are the appropriate court case statuses?
            StateCourtCaseStatus: ['A', 'STEP'],
            StateIncarcerationPeriodAdmissionReason:
                ['COM', 'CONT', 'CONV', 'NTAD'],
            StateIncarcerationPeriodReleaseReason:
                ['ADMN', 'CONT', 'CONV', 'REC', '4139'],
        }

        return self._create_overrides(overrides, ignores)

    def _create_overrides(self,
                          overrides: Dict[EntityEnum, List[str]],
                          ignores: Dict[EntityEnumMeta, List[str]]) \
            -> EnumOverrides:
        overrides_builder = super(UsNdController, self) \
            .get_enum_overrides().to_builder()

        for mapped_enum, text_tokens in overrides.items():
            for text_token in text_tokens:
                overrides_builder.add(text_token, mapped_enum)

        for ignored_enum, text_tokens in ignores.items():
            for text_token in text_tokens:
                overrides_builder.ignore(text_token, ignored_enum)

        return overrides_builder.build()


def _yaml_filepath(filename):
    return os.path.join(os.path.dirname(__file__), filename)


def _generate_sentence_primary_key(row: Dict[str, str]) \
        -> IngestFieldCoordinates:
    return IngestFieldCoordinates('state_incarceration_sentence',
                                  'state_incarceration_sentence_id',
                                  _generate_sentence_id(row))


def _generate_sentence_id(row: Dict[str, str]) -> str:
    sentence_group_id = row['OFFENDER_BOOK_ID']
    sentence_seq = row['SENTENCE_SEQ']

    term_code = row.get('SENTENCE_TERM_CODE', None)

    sentence_id = '-'.join([sentence_group_id, sentence_seq])
    if term_code == 'SUSP':
        sentence_id = sentence_id + _SUPERVISION_SENTENCE_ID_SUFFIX

    return sentence_id


def _generate_incident_primary_key(row: Dict[str, str]) \
        -> IngestFieldCoordinates:
    return IngestFieldCoordinates('state_incarceration_incident',
                                  'state_incarceration_incident_id',
                                  _generate_incident_id(row))


def _generate_incident_id(row: Dict[str, str]) -> str:
    overall_incident_id = row['AGENCY_INCIDENT_ID']
    person_incident_id = row['OIC_INCIDENT_ID']

    return '-'.join([overall_incident_id, person_incident_id])


def _convert_to_supervision_sentence(just_updated: StateIncarcerationSentence,
                                     sentence_group: StateSentenceGroup,
                                     cache: IngestObjectCache):
    obj_id = just_updated.state_incarceration_sentence_id

    if obj_id is None or len(obj_id) < len(_SUPERVISION_SENTENCE_ID_SUFFIX):
        raise ValueError(f"Unexpected obj_id [{obj_id}]")

    cache.clear_object_by_id('state_incarceration_sentence', obj_id)

    corrected_obj_id = obj_id[:-len(_SUPERVISION_SENTENCE_ID_SUFFIX)]
    supervision_sentence = sentence_group.create_state_supervision_sentence(
        state_supervision_sentence_id=corrected_obj_id,
        supervision_type=StateSupervisionType.PROBATION.value,
        max_length=just_updated.max_length
    )
    sentence_group.state_incarceration_sentences.remove(just_updated)

    cache.cache_object_by_id('state_supervision_sentence',
                             corrected_obj_id, supervision_sentence)


def _generate_period_primary_key(row: Dict[str, str]) -> IngestFieldCoordinates:
    return IngestFieldCoordinates('state_incarceration_period',
                                  'state_incarceration_period_id',
                                  _generate_period_id(row))


def _generate_period_id(row: Dict[str, str]) -> str:
    sentence_group_id = row['OFFENDER_BOOK_ID']
    movement_seq = row['MOVEMENT_SEQ']

    return '-'.join([sentence_group_id, movement_seq])


def _recover_movement_sequence(period_id: str) -> str:
    period_id_components = period_id.split('.')
    if len(period_id_components) < 2:
        raise ValueError(f"Expected period id [{period_id}] to have multiple "
                         f"components separated by a period")
    return period_id_components[1]


def _generate_charge_primary_key(row: Dict[str, str]) -> IngestFieldCoordinates:
    return IngestFieldCoordinates('state_charge',
                                  'state_charge_id',
                                  _generate_charge_id(row))


def _generate_alias_temporary_primary_key(
        _row: Dict[str, str]) -> IngestFieldCoordinates:
    return IngestFieldCoordinates('state_alias',
                                  'state_alias_id',
                                  _TEMPORARY_PRIMARY_ID)


def _generate_charge_id(row: Dict[str, str]) -> str:
    sentence_group_id = row['OFFENDER_BOOK_ID']
    charge_seq = row['CHARGE_SEQ']

    return '-'.join([sentence_group_id, charge_seq])


def _fill_in_incarceration_sentence_parent_id(
        primary_coordinates: IngestFieldCoordinates) -> Dict[str, str]:
    return {'state_incarceration_sentence': primary_coordinates.field_value}


def _concatenate_length_periods(units: List[str],
                                components: List[Optional[str]],
                                extracted_objects: List[IngestObject]):
    for i, component in enumerate(components):
        if component:
            component += units[i]

    items = ['{}{}'.format(component, units[i])
             for i, component in enumerate(components)
             if component]
    length = ' '.join(filter(None, items))

    for extracted_object in extracted_objects:
        if length and hasattr(extracted_object, 'max_length'):
            extracted_object.__setattr__('max_length', length)


def _convert_person_ids_to_external_id_objects(
        ingest_info: IngestInfo,
        cache: Optional[IngestObjectCache],
        id_type: str):
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


def _parse_charge_classification(classification_str: Optional[str],
                                 extracted_objects: List[IngestObject]):
    if not classification_str:
        return

    upper_classification_str = classification_str.upper()

    classification_subtype = None
    if upper_classification_str[0] in {'F', 'M'}:
        classification_type = upper_classification_str[0]

        if upper_classification_str[1:]:
            classification_subtype = upper_classification_str[1:]
    elif upper_classification_str in {'IF', 'IM'}:
        classification_type = upper_classification_str[1]
    else:
        raise ValueError(
            f"Cannot parse classification string: [{classification_str}]")

    for extracted_object in extracted_objects:
        if isinstance(extracted_object, StateCharge):
            extracted_object.classification_type = \
                classification_type
            extracted_object.classification_subtype = \
                classification_subtype


_LSIR_DOMAINS: Dict[str, str] = {
    'chtotal': 'domain_criminal_history',
    'eetotal': 'domain_education_employment',
    'fncltotal': 'domain_financial',
    'fmtotal': 'domain_family_marital',
    'accomtotal': 'domain_accommodation',
    'lrtotal': 'domain_leisure_recreation',
    'cptotal': 'domain_companions',
    'adtotal': 'domain_alcohol_drug_problems',
    'eptotal': 'domain_emotional_personal',
    'aototal': 'domain_attitudes_orientation'
}


def _get_lsir_domain_score(row: Dict[str, str], domain: str) -> int:
    domain_score = row.get(domain, '0')
    if domain_score.strip():
        return int(domain_score)
    return 0


def _get_lsir_domain_scores_and_sum(
        row: Dict[str, str], domains: List[str]) \
        -> Tuple[int, Dict[str, int]]:
    total_score = 0
    domain_scores = {}

    for domain in domains:
        domain_score = _get_lsir_domain_score(row, domain)
        total_score += domain_score

        domain_label = _LSIR_DOMAINS[domain.lower()]
        domain_scores[domain_label] = domain_score

    return total_score, domain_scores


_LSIR_QUESTIONS: Dict[str, str] = {
    'q18value': 'question_18',
    'q19value': 'question_19',
    'q20value': 'question_20',
    'q21value': 'question_21',
    'q23value': 'question_23',
    'q24value': 'question_24',
    'q25value': 'question_25',
    'q27value': 'question_27',
    'q31value': 'question_31',
    'q39value': 'question_39',
    'q40value': 'question_40',
    'q51value': 'question_51',
    'q52value': 'question_52'
}


def _get_lsir_question_score(row: Dict[str, str], question: str) -> int:
    question_score = row.get(question, '0')
    if question_score.strip():
        return int(question_score)
    return 0


def _get_lsir_question_scores(row: Dict[str, str], questions: List[str]) \
        -> Dict[str, int]:
    question_scores = {}

    for question in questions:
        question_score = _get_lsir_question_score(row, question)
        question_label = _LSIR_QUESTIONS[question.lower()]
        question_scores[question_label] = question_score

    return question_scores
