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
import re
from typing import Dict, List, Optional, Pattern, Tuple, cast

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common import ncic
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Ethnicity
from recidiviz.common.constants.state.external_id_types import US_ND_ELITE, US_ND_SID
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import (
    parse_days_from_duration_pieces,
    safe_parse_days_from_duration_str,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    IngestRowPrehookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import create_if_not_exists
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    normalized_county_code,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_enum_helpers import (
    generate_enum_overrides,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_judicial_district_code_reference import (
    normalized_judicial_district_code,
)
from recidiviz.ingest.direct.state_shared_row_posthooks import (
    IngestGatingContext,
    copy_name_to_alias,
    gen_convert_person_ids_to_external_id_objects,
    gen_label_single_external_id_hook,
    gen_normalize_county_codes_posthook,
    gen_set_agent_type,
    gen_set_is_life_sentence_hook,
    get_normalized_ymd_str,
)
from recidiviz.ingest.extractor.csv_data_extractor import (
    AncestorChainOverridesCallable,
    IngestFieldCoordinates,
)
from recidiviz.ingest.models.ingest_info import (
    IngestInfo,
    IngestObject,
    StateAgent,
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StateProgramAssignment,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment

_DOCSTARS_NEGATIVE_PATTERN: Pattern = re.compile(r"^\((?P<value>-?\d+)\)$")


# TODO(#8901): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsNdController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for us_nd."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_ND.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)

        self.enum_overrides = generate_enum_overrides()

        self.row_pre_processors_by_file: Dict[str, List[IngestRowPrehookCallable]] = {
            "elite_offenderidentifier": [self._normalize_id_fields],
            "elite_alias": [self._normalize_id_fields],
            "elite_offendersentences": [self._normalize_id_fields],
            "elite_offendersentenceterms": [self._normalize_id_fields],
            "elite_offenderchargestable": [self._normalize_id_fields],
            "elite_orderstable": [self._normalize_id_fields],
            "elite_externalmovements": [self._normalize_id_fields],
        }
        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            "elite_offenders": [copy_name_to_alias],
            "elite_offenderidentifier": [self._normalize_external_id_type],
            "elite_offendersentenceaggs": [
                gen_set_is_life_sentence_hook("MAX_TERM", "LIFE", StateSentenceGroup),
                self._set_sentence_group_length_max_length,
            ],
            "elite_offendersentences": [
                gen_set_is_life_sentence_hook(
                    "SENTENCE_CALC_TYPE", "LIFE", StateIncarcerationSentence
                )
            ],
            "elite_offendersentenceterms": [self._add_sentence_children],
            "elite_offenderchargestable": [
                self._parse_elite_charge_classification,
                self._set_elite_charge_status,
                self._rationalize_controlling_charge,
                self._rationalize_violent_charge,
            ],
            "elite_orderstable": [
                gen_set_agent_type(StateAgentType.JUDGE),
                gen_normalize_county_codes_posthook(
                    self.region.region_code,
                    "COUNTY_CODE",
                    StateCourtCase,
                    normalized_county_code,
                ),
                self._normalize_judicial_district_code,
            ],
            "elite_externalmovements": [self._process_external_movement],
            "elite_offense_in_custody_and_pos_report_data": [
                self._rationalize_incident_type,
                self._rationalize_outcome_type,
                self._set_punishment_length_days,
                self._set_location_within_facility,
                self._set_incident_outcome_id,
            ],
            "docstars_offenders": [
                # For a person we are seeing in Docstars with an ITAGROOT_ID referencing a corresponding record in
                # Elite, mark that external id as having type ELITE.
                gen_label_single_external_id_hook(US_ND_ELITE),
                self._enrich_addresses,
                self._enrich_sorac_assessments,
                copy_name_to_alias,
                self._add_supervising_officer,
                self._add_case_type_external_id,
            ],
            "docstars_offendercasestable_with_officers": [
                self._concatenate_docstars_length_periods,
                self._add_officer_to_supervision_periods,
                self._record_revocation,
                gen_set_agent_type(StateAgentType.JUDGE),
                self._hydrate_supervision_period_sentence_shared_date_fields,
                gen_normalize_county_codes_posthook(
                    self.region.region_code,
                    "TB_CTY",
                    StateSupervisionPeriod,
                    normalized_county_code,
                ),
            ],
            "docstars_offensestable": [
                self._parse_docstars_charge_classification,
                self._parse_docstars_charge_offense,
                gen_normalize_county_codes_posthook(
                    self.region.region_code,
                    "COUNTY",
                    StateCharge,
                    normalized_county_code,
                ),
            ],
            "docstars_lsi_chronology": [self._process_lsir_assessments],
            "docstars_ftr_episode": [self._process_ftr_episode],
            "docstars_contacts": [
                self._add_supervision_officer_to_contact,
                gen_set_agent_type(StateAgentType.SUPERVISION_OFFICER),
                self._add_location_to_contact,
                self._add_status_to_contact,
            ],
        }

        self.primary_key_override_hook_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            "elite_offendersentences": _generate_sentence_primary_key,
            "elite_externalmovements": _generate_period_primary_key,
            "elite_offenderchargestable": _generate_charge_primary_key,
            "elite_offense_in_custody_and_pos_report_data": _generate_incident_primary_key,
        }

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, AncestorChainOverridesCallable
        ] = {"elite_offenderchargestable": _state_charge_ancestor_chain_overrides}

        self.files_to_set_with_empty_values = ["elite_offendersentenceterms"]

    def get_file_tag_rank_list(self) -> List[str]:
        # NOTE: The order of ingest here is important! Do not change unless you know what you're doing!
        tags = [
            # Elite - incarceration-focused
            "elite_offenderidentifier",
            "elite_offenders",
            "elite_alias",
            "elite_offenderbookingstable",
            "elite_offendersentenceaggs",
            "elite_offendersentences",
            "elite_offendersentenceterms",
            "elite_offenderchargestable",
            "elite_orderstable",
            "elite_externalmovements",
        ]

        if not environment.in_gcp():
            # TODO(#2399): Once we are capable of handling historical and nightly ingest of
            #  'elite_offense_in_custody_and_pos_report_data', remove this check.
            tags.append("elite_offense_in_custody_and_pos_report_data")

        tags += [
            # Docstars - supervision-focused
            "docstars_offenders",
            "docstars_offendercasestable_with_officers",
            "docstars_offensestable",
            "docstars_ftr_episode",
            "docstars_lsi_chronology",
            "docstars_contacts",
            # TODO(#1918): Integrate bed assignment / location history
        ]

        return tags

    def _normalize_id_fields(
        self, _gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> None:
        """A number of ID fields come in as comma-separated strings in some of the files. This function converts
        those id column values to a standard format without decimals or commas before the row is processed.

        If the ID column is in the proper format, this function will no-op.
        """

        for field_name in [
            "ROOT_OFFENDER_ID",
            "ALIAS_OFFENDER_ID",
            "OFFENDER_ID",
            "OFFENDER_BOOK_ID",
            "ORDER_ID",
        ]:
            if field_name in row:
                row[field_name] = self._decimal_str_as_int_str(row[field_name])

    @staticmethod
    def _decimal_str_as_int_str(dec_str: str) -> str:
        """Converts a comma-separated string representation of an integer into a string representing a simple integer
        with no commas.

        E.g. _decimal_str_as_int_str('1,234.00') -> '1234'
        """
        if not dec_str:
            return dec_str

        return str(int(float(dec_str.replace(",", ""))))

    # TODO(#8901): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_row_pre_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPrehookCallable]:
        return self.row_pre_processors_by_file.get(file_tag, [])

    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(
        self, _file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        post_processors: List[IngestFilePostprocessorCallable] = [
            self._rationalize_race_and_ethnicity,
            gen_convert_person_ids_to_external_id_objects(self._get_id_type),
        ]
        return post_processors

    def get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    def get_primary_key_override_for_file(
        self, file: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return self.files_to_set_with_empty_values

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag.startswith("elite"):
            return US_ND_ELITE
        if file_tag.startswith("docstars"):
            return US_ND_SID

        raise ValueError(f"File [{file_tag}] doesn't have a known external id type")

    @staticmethod
    def _add_supervision_officer_to_contact(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the current supervising officer onto the extracted supervision contact."""
        supervising_officer_last_name = row.get("LNAME")
        supervising_officer_first_name = row.get("FNAME")
        supervising_officer_id = row.get("OFFICER")
        if (
            not supervising_officer_last_name
            or not supervising_officer_first_name
            or not supervising_officer_id
        ):
            return
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionContact):
                agent_to_create = StateAgent(
                    full_name=f"{supervising_officer_first_name} {supervising_officer_last_name}",
                    state_agent_id=supervising_officer_id,
                    agent_type=StateAgentType.SUPERVISION_OFFICER.value,
                )
                create_if_not_exists(
                    agent_to_create, extracted_object, "contacted_agent"
                )

    def _add_location_to_contact(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds a contact location to the extracted supervision contact."""
        contact_location_from_code = row.get("CONTACT_CODE")
        if contact_location_from_code:
            contact_location = self.get_enum_overrides().parse(
                contact_location_from_code, StateSupervisionContactLocation
            )
            if contact_location is not None:
                for extracted_object in extracted_objects:
                    if isinstance(extracted_object, StateSupervisionContact):
                        extracted_object.location = contact_location.value

    def _add_status_to_contact(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        # TODO(#1882): Specify this mapping in the YAML once a single csv column can be mapped to multiple fields.
        """Adds a contact status to the extracted supervision contact."""
        contact_status_from_code = row.get("CONTACT_CODE")
        if contact_status_from_code:
            contact_status = self.get_enum_overrides().parse(
                contact_status_from_code, StateSupervisionContactStatus
            )
            if contact_status is None:
                raise ValueError(
                    f"Unable to parse [{contact_status_from_code}] into known StateSupervisionContactStatus value"
                )
            for extracted_object in extracted_objects:
                if isinstance(extracted_object, StateSupervisionContact):
                    extracted_object.status = contact_status.value

    @staticmethod
    def _add_supervising_officer(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the current supervising officer onto the extracted person."""
        supervising_officer_id = row.get("AGENT")
        if not supervising_officer_id:
            return
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                agent_to_create = StateAgent(
                    state_agent_id=supervising_officer_id,
                    agent_type=StateAgentType.SUPERVISION_OFFICER.value,
                )
                create_if_not_exists(
                    agent_to_create, extracted_object, "supervising_officer"
                )

    @staticmethod
    def _add_case_type_external_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the person external_id to the extracted supervision case type."""
        external_id = row.get("SID")
        if not external_id:
            raise ValueError(
                f"File [{_gating_context.file_tag}] is missing an SID external id"
            )
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionCaseTypeEntry):
                extracted_object.state_supervision_case_type_entry_id = external_id

    @staticmethod
    def _rationalize_race_and_ethnicity(
        _gating_context: IngestGatingContext,
        _ingest_info: IngestInfo,
        cache: Optional[IngestObjectCache],
    ) -> None:
        """For a person whose provided race is HISPANIC, we set the ethnicity to HISPANIC, and the race will be
        cleared.
        """
        if cache is None:
            raise ValueError("Ingest object cache is unexpectedly None")

        for person in cache.get_objects_of_type("state_person"):
            updated_person_races = []
            for person_race in person.state_person_races:
                if person_race.race in {"5", "HIS"}:
                    ethnicity_to_create = StatePersonEthnicity(
                        ethnicity=Ethnicity.HISPANIC.value
                    )
                    create_if_not_exists(
                        ethnicity_to_create, person, "state_person_ethnicities"
                    )
                else:
                    updated_person_races.append(person_race)
            person.state_person_races = updated_person_races

    @classmethod
    def _set_sentence_group_length_max_length(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """ Parses the sentence group max length from the MAX_TERM field."""

        max_term = row["MAX_TERM"]

        # Means Prison sentence, which is redundant. It is only ever used in a small number of erroneous cases long ago
        # that had no data on max sentence length available.
        is_redundant_pri = max_term == "PRI"

        # Indicates a life sentence.
        is_life_sentence = max_term == "LIFE"

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSentenceGroup):
                if not (is_life_sentence or is_redundant_pri):
                    max_duration_str = max_term.strip()
                    if max_duration_str:
                        max_length_days = safe_parse_days_from_duration_str(
                            duration_str=max_duration_str,
                            start_dt_str=extracted_object.date_imposed,
                        )
                        if max_length_days is not None:
                            extracted_object.max_length = str(max_length_days)

    @staticmethod
    def _normalize_external_id_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                id_type = f"US_ND_{extracted_object.id_type}"
                extracted_object.__setattr__("id_type", id_type)

    @staticmethod
    def _process_external_movement(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets admission- or release-specific fields based on whether this movement represents an admission into or a
        release from a particular facility."""
        direction_code = row["DIRECTION_CODE"]
        is_admission = direction_code == "IN"
        movement_date = row["MOVEMENT_DATE"]
        movement_reason = row["MOVEMENT_REASON_CODE"]
        to_facility = row["TO_AGY_LOC_ID"]
        from_facility = row["FROM_AGY_LOC_ID"]
        active_flag = row["ACTIVE_FLAG"]

        # TODO(#2002): If this edge is a transfer in or out of a hospital or other non-prison facility, create extra
        #  proper edges in and out of those facilities.
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationPeriod):
                extracted_object.status = f"{direction_code}-{active_flag}"
                if is_admission:
                    extracted_object.admission_date = movement_date
                    extracted_object.admission_reason = movement_reason
                    extracted_object.facility = to_facility
                else:
                    extracted_object.release_date = movement_date
                    extracted_object.release_reason = movement_reason
                    extracted_object.facility = from_facility

                if extracted_object.facility:
                    facility = extracted_object.facility.upper()
                    if facility == "NTAD":
                        # Could be a county jail or another state's facility
                        extracted_object.incarceration_type = (
                            StateIncarcerationType.EXTERNAL_UNKNOWN.value
                        )
                    elif facility == "CJ":
                        extracted_object.incarceration_type = (
                            StateIncarcerationType.COUNTY_JAIL.value
                        )
                    elif facility == "DEFP":
                        extracted_object.incarceration_type = (
                            StateIncarcerationType.COUNTY_JAIL.value
                        )

    @staticmethod
    def _enrich_addresses(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Concatenate address, city, state, and zip information."""
        city = row.get("CITY", None)
        state = row.get("STATE", None)
        postal = row.get("ZIP", None)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                address = extracted_object.current_address
                full_address = ", ".join(filter(None, [address, city, state, postal]))
                extracted_object.__setattr__("current_address", full_address)

    @staticmethod
    def _enrich_sorac_assessments(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """For SORAC assessments in Docstars' incoming person data, add metadata we can infer from context."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.assessment_class = StateAssessmentClass.RISK.value
                extracted_object.assessment_type = StateAssessmentType.SORAC.value

    @staticmethod
    def _process_lsir_assessments(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """For rich LSIR historical data from Docstars, manually process individual domain and question values."""
        # Cast the LSIR keys to lower case to avoid casing inconsistencies
        lower_case_row = {key.lower(): value for key, value in row.items()}
        domain_labels = [
            "CHtotal",
            "EETotal",
            "FnclTotal",
            "FMTotal",
            "AccomTotal",
            "LRTotal",
            "Cptotal",
            "Adtotal",
            "EPTotal",
            "AOTotal",
        ]
        total_score, domain_scores = _get_lsir_domain_scores_and_sum(
            lower_case_row, domain_labels
        )

        question_labels = [
            "Q18value",
            "Q19value",
            "Q20value",
            "Q21value",
            "Q23Value",
            "Q24Value",
            "Q25Value",
            "Q27Value",
            "Q31Value",
            "Q39Value",
            "Q40Value",
            "Q51value",
            "Q52Value",
        ]
        question_scores = _get_lsir_question_scores(lower_case_row, question_labels)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                assessment = cast(StateAssessment, extracted_object)
                assessment.assessment_class = StateAssessmentClass.RISK.value
                assessment.assessment_type = StateAssessmentType.LSIR.value
                assessment.assessment_score = str(total_score)

                lsi_metadata = {**domain_scores, **question_scores}
                assessment.assessment_metadata = json.dumps(lsi_metadata)

    @staticmethod
    def _process_ftr_episode(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Manually add referral_metadata and discharge_date to ProgramAssignment entities, if applicable."""

        referral_field_labels: List[str] = [
            "PREFERRED_PROVIDER_ID",
            "PREFERRED_LOCATION_ID",
            "STRENGTHS",
            "NEEDS",
            "IS_CLINICAL_ASSESSMENT",
            "FUNCTIONAL_IMPAIRMENTS",
            "ASSESSMENT_LOCATION",
            "REFERRAL_REASON",
            "SPECIALIST_FIRST_NAME",
            "SPECIALIST_LAST_NAME",
            "SPECIALIST_INITIAL",
            "SUBMITTED_BY",
            "SUBMITTED_BY_NAME",
        ]
        referral_metadata = get_program_referral_fields(row, referral_field_labels)
        status_date = row.get("STATUS_DATE", None)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateProgramAssignment):
                extracted_object.referral_metadata = json.dumps(referral_metadata)
                status = extracted_object.participation_status
                # All other statuses have their dates automatically set
                if status == "Discharged":
                    extracted_object.discharge_date = status_date

    @staticmethod
    def _add_terminating_officer_to_supervision_periods(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """When present, adds supervising officer to the extracted SupervisionPeriods."""
        terminating_officer_id = row.get("TERMINATING_OFFICER", None)
        if not terminating_officer_id:
            return

        agent_to_create = StateAgent(
            state_agent_id=terminating_officer_id,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        )

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionPeriod):
                create_if_not_exists(
                    agent_to_create, extracted_object, "supervising_officer"
                )

    @staticmethod
    def _add_officer_to_supervision_periods(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """When present, adds supervising officer to the extracted SupervisionPeriods."""
        terminating_officer_id = row.get("terminating_officer_id", None)
        recent_officer_id = row.get("recent_officer_id", None)
        if not terminating_officer_id and not recent_officer_id:
            return

        termination_date = row.get("TERM_DATE", None)

        if terminating_officer_id:
            officer_id: Optional[str] = terminating_officer_id
            officer_fname = row.get("terminating_officer_fname", None)
            officer_lname = row.get("terminating_officer_lname", None)
            officer_siteid = row.get("terminating_officer_siteid", None)
        elif not termination_date:
            officer_id = recent_officer_id
            officer_fname = row.get("recent_officer_fname", None)
            officer_lname = row.get("recent_officer_lname", None)
            officer_siteid = row.get("recent_officer_siteid", None)
        else:
            return

        agent_to_create = StateAgent(
            state_agent_id=officer_id,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
            given_names=officer_fname,
            surname=officer_lname,
        )

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionPeriod):
                create_if_not_exists(
                    agent_to_create, extracted_object, "supervising_officer"
                )
                extracted_object.supervision_site = officer_siteid

    # TODO(#1882): Specify this mapping in the YAML once a single csv column can
    # can be mapped to multiple fields.
    @staticmethod
    def _hydrate_supervision_period_sentence_shared_date_fields(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets SupervisionPeriod.termination_date/supervision_type from the parent
        SupervisionSentence.completion_date/supervision_type. This is possible because in ND, we always have a
        1:1 mapping of SupervisionSentence:SupervisionPeriod.
        """
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionSentence):
                completion_date = extracted_object.completion_date
                supervision_type = extracted_object.supervision_type
                start_date = extracted_object.start_date

                supervision_periods = extracted_object.state_supervision_periods
                if len(supervision_periods) > 1:
                    raise ValueError(
                        f"SupervisionSentence "
                        f"{extracted_object.state_supervision_sentence_id} has "
                        f"{str(len(supervision_periods))} associated "
                        f"supervision periods, expected a maximum of 1."
                    )
                if supervision_periods:
                    supervision_periods[0].start_date = start_date
                    supervision_periods[0].termination_date = completion_date
                    supervision_periods[0].supervision_type = supervision_type

    @staticmethod
    def _concatenate_docstars_length_periods(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """For a sentence row in Docstars that contains distinct SENT_YY and SENT_MM fields, compose these into a
        single string of days and set it on the appropriate length field."""
        years = row.get("SENT_YY", None)
        months = row.get("SENT_MM", None)

        # It appears a recent change to Docstars files started passing negative values inside of parentheses instead
        # of after a '-' sign
        match = re.match(_DOCSTARS_NEGATIVE_PATTERN, months)
        if match is not None:
            value = match.group("value")
            months = "-" + value

        if not years and not months:
            return

        effective_date = row["PAROLE_FR"]

        total_days = parse_days_from_duration_pieces(
            years_str=years,
            months_str=months,
            days_str=None,
            start_dt_str=effective_date,
        )

        for extracted_object in extracted_objects:
            if total_days and isinstance(extracted_object, StateSupervisionSentence):
                day_string = "{}d".format(total_days)
                extracted_object.__setattr__("max_length", day_string)

    @staticmethod
    def _record_revocation(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Captures information in a Docstars cases row that indicates a revocation, and fills out the surrounding data
        model accordingly."""
        revocation_occurred = bool(
            row.get("REV_DATE", None) or row.get("RevoDispo", None)
        )

        if not revocation_occurred:
            return

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionPeriod):
                terminating_officer = extracted_object.supervising_officer
                for violation in extracted_object.state_supervision_violation_entries:
                    _record_revocation_on_violation(violation, row)
                    for (
                        violation_response
                    ) in violation.state_supervision_violation_responses:
                        _record_revocation_on_violation_response(
                            violation_response, terminating_officer
                        )

    @staticmethod
    def _rationalize_incident_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncident):
                if extracted_object.incident_type == "MISC":
                    # TODO(#1948): Infer incident type from offense code cols when marked as MISC
                    extracted_object.incident_type = None

    @staticmethod
    def _rationalize_outcome_type(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        finding = row["FINDING_DESCRIPTION"]

        if finding == "NOT GUILTY":
            for extracted_object in extracted_objects:
                if isinstance(extracted_object, StateIncarcerationIncidentOutcome):
                    extracted_object.outcome_type = (
                        StateIncarcerationIncidentOutcomeType.NOT_GUILTY.value
                    )

    @staticmethod
    def _set_punishment_length_days(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        months = row["SANCTION_MONTHS"]
        days = row["SANCTION_DAYS"]

        if not months and not days:
            # No punishment length info
            return

        effective_date = row["EFFECTIVE_DATE"]

        total_days = parse_days_from_duration_pieces(
            years_str=None,
            months_str=months,
            days_str=days,
            start_dt_str=effective_date,
        )

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncidentOutcome):
                extracted_object.punishment_length_days = str(total_days)

    @staticmethod
    def _set_location_within_facility(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        facility = row["AGY_LOC_ID"]
        facility_with_loc = row["OMS_OWNER_V_OIC_INCIDENTS_INT_LOC_DESCRIPTION"]

        if not facility or not facility_with_loc:
            return

        facility_prefix = f"{facility}-"
        if not facility_with_loc.startswith(facility_prefix):
            return

        # Strip facility prefix
        location = facility_with_loc[len(facility_prefix) :]
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncident):
                extracted_object.location_within_facility = location

    @staticmethod
    def _set_incident_outcome_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        incident_id = _generate_incident_id(row)
        sanction_seq = row["SANCTION_SEQ"]

        if not incident_id or not sanction_seq:
            return

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateIncarcerationIncidentOutcome):
                extracted_object.state_incarceration_incident_outcome_id = "-".join(
                    [incident_id, sanction_seq]
                )

    @staticmethod
    def _add_sentence_children(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        term_code = row.get("SENTENCE_TERM_CODE", None)
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSentenceGroup):
                sentence_id = _generate_sentence_id(row)
                max_length = get_normalized_ymd_str("YEARS", "MONTHS", "DAYS", row)
                if term_code == "SUSP":
                    supervision_sentence = StateSupervisionSentence(
                        state_supervision_sentence_id=sentence_id,
                        supervision_type=StateSupervisionType.PROBATION.value,
                        max_length=max_length,
                    )
                    create_if_not_exists(
                        supervision_sentence,
                        extracted_object,
                        "state_supervision_sentences",
                    )
                else:
                    incarceration_sentence = StateIncarcerationSentence(
                        state_incarceration_sentence_id=sentence_id,
                        max_length=max_length,
                    )
                    create_if_not_exists(
                        incarceration_sentence,
                        extracted_object,
                        "state_incarceration_sentences",
                    )

    @staticmethod
    def _set_elite_charge_status(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                # Note: If we hear about a charge in Elite, the person has already been sentenced.
                extracted_object.status = ChargeStatus.SENTENCED.value

    @staticmethod
    def _rationalize_controlling_charge(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        status = row.get("CHARGE_STATUS", None)
        is_controlling = status and status.upper() in ["C", "CT"]

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                extracted_object.is_controlling = str(is_controlling)

    @staticmethod
    def _rationalize_violent_charge(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        offense_type = row.get("SEVERITY_RANKING", None)
        is_violent = offense_type and offense_type.upper() == "VIOLENT"

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                extracted_object.is_violent = str(is_violent)

    @staticmethod
    def _parse_elite_charge_classification(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        classification_str = row["OFFENCE_TYPE"]
        _parse_charge_classification(classification_str, extracted_objects)

    @staticmethod
    def _parse_docstars_charge_classification(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        classification_str = row["LEVEL"]
        _parse_charge_classification(classification_str, extracted_objects)

    @staticmethod
    def _parse_docstars_charge_offense(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        ncic_code = row.get("CODE", None)
        if not ncic_code:
            return

        description = ncic.get_description(ncic_code)
        is_violent = ncic.get_is_violent(ncic_code)

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                extracted_object.description = description
                if is_violent is not None:
                    extracted_object.is_violent = str(is_violent)

    @staticmethod
    def _normalize_judicial_district_code(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCourtCase):
                normalized_code = normalized_judicial_district_code(
                    extracted_object.judicial_district_code
                )
                extracted_object.__setattr__("judicial_district_code", normalized_code)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides


def _generate_sentence_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    return IngestFieldCoordinates(
        "state_incarceration_sentence",
        "state_incarceration_sentence_id",
        _generate_sentence_id(row),
    )


def _generate_sentence_id(row: Dict[str, str]) -> str:
    sentence_group_id = row["OFFENDER_BOOK_ID"]
    sentence_seq = row["SENTENCE_SEQ"]
    sentence_id = "-".join([sentence_group_id, sentence_seq])
    return sentence_id


def _generate_incident_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    return IngestFieldCoordinates(
        "state_incarceration_incident",
        "state_incarceration_incident_id",
        _generate_incident_id(row),
    )


def _generate_incident_id(row: Dict[str, str]) -> str:
    overall_incident_id = row["AGENCY_INCIDENT_ID"]
    person_incident_id = row["OIC_INCIDENT_ID"]

    return "-".join([overall_incident_id, person_incident_id])


def _generate_period_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    return IngestFieldCoordinates(
        "state_incarceration_period",
        "state_incarceration_period_id",
        _generate_period_id(row),
    )


def _generate_period_id(row: Dict[str, str]) -> str:
    sentence_group_id = row["OFFENDER_BOOK_ID"]
    movement_seq = row["MOVEMENT_SEQ"]

    return "-".join([sentence_group_id, movement_seq])


def _recover_movement_sequence(period_id: str) -> str:
    period_id_components = period_id.split(".")
    if len(period_id_components) < 2:
        raise ValueError(
            f"Expected period id [{period_id}] to have multiple components separated by a period"
        )
    return period_id_components[1]


def _generate_charge_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    return IngestFieldCoordinates(
        "state_charge", "state_charge_id", _generate_charge_id(row)
    )


def _generate_charge_id(row: Dict[str, str]) -> str:
    sentence_group_id = row["OFFENDER_BOOK_ID"]
    charge_seq = row["CHARGE_SEQ"]

    return "-".join([sentence_group_id, charge_seq])


def _state_charge_ancestor_chain_overrides(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> Dict[str, str]:
    # The charge id can be used interchangeably in ND with the sentence id because there is a 1:1 mapping between
    # charges and sentences, so the CHARGE_SEQ and SENTENCE_SEQ numbers can be used interchangeably.
    return {"state_incarceration_sentence": _generate_charge_id(row)}


def _parse_charge_classification(
    classification_str: Optional[str], extracted_objects: List[IngestObject]
) -> None:
    if not classification_str:
        return

    upper_classification_str = classification_str.upper()

    classification_subtype = None
    if upper_classification_str[0] in {"F", "M"}:
        classification_type = upper_classification_str[0]

        if upper_classification_str[1:]:
            classification_subtype = upper_classification_str[1:]
    elif upper_classification_str in {"IF", "IM"}:
        classification_type = upper_classification_str[1]
    else:
        raise ValueError(f"Cannot parse classification string: [{classification_str}]")

    for extracted_object in extracted_objects:
        if isinstance(extracted_object, StateCharge):
            extracted_object.classification_type = classification_type
            extracted_object.classification_subtype = classification_subtype


_LSIR_DOMAINS: Dict[str, str] = {
    "chtotal": "domain_criminal_history",
    "eetotal": "domain_education_employment",
    "fncltotal": "domain_financial",
    "fmtotal": "domain_family_marital",
    "accomtotal": "domain_accommodation",
    "lrtotal": "domain_leisure_recreation",
    "cptotal": "domain_companions",
    "adtotal": "domain_alcohol_drug_problems",
    "eptotal": "domain_emotional_personal",
    "aototal": "domain_attitudes_orientation",
}


def _get_lsir_domain_score(row: Dict[str, str], domain: str) -> int:
    domain_score = row.get(domain.lower(), "0")
    if domain_score.strip():
        return int(domain_score)
    return 0


def _get_lsir_domain_scores_and_sum(
    row: Dict[str, str], domains: List[str]
) -> Tuple[int, Dict[str, int]]:
    total_score = 0
    domain_scores = {}

    for domain in domains:
        domain_score = _get_lsir_domain_score(row, domain)
        total_score += domain_score

        domain_label = _LSIR_DOMAINS[domain.lower()]
        domain_scores[domain_label] = domain_score

    return total_score, domain_scores


_LSIR_QUESTIONS: Dict[str, str] = {
    "q18value": "question_18",
    "q19value": "question_19",
    "q20value": "question_20",
    "q21value": "question_21",
    "q23value": "question_23",
    "q24value": "question_24",
    "q25value": "question_25",
    "q27value": "question_27",
    "q31value": "question_31",
    "q39value": "question_39",
    "q40value": "question_40",
    "q51value": "question_51",
    "q52value": "question_52",
}


def _get_lsir_question_score(row: Dict[str, str], question: str) -> int:
    question_score = row.get(question.lower(), "0")
    if question_score.strip():
        return int(question_score)
    return 0


def _get_lsir_question_scores(
    row: Dict[str, str], questions: List[str]
) -> Dict[str, int]:
    question_scores = {}

    for question in questions:
        question_score = _get_lsir_question_score(row, question)
        question_label = _LSIR_QUESTIONS[question.lower()]
        question_scores[question_label] = question_score

    return question_scores


def get_program_referral_fields(
    row: Dict[str, str], referral_field_labels: List[str]
) -> Dict[str, Optional[str]]:
    referral_fields = {}
    for str_field in referral_field_labels:
        label = str_field.lower()
        value = row.get(str_field, None)
        referral_fields[label] = value
    return referral_fields


def _record_revocation_on_violation(
    violation: StateSupervisionViolation, row: Dict[str, str]
) -> None:
    """Adds revocation information onto the provided |violation| as necessary."""
    # These three flags are either '0' (False) or '-1' (True). That -1 may now be (1) after a recent Docstars
    # change.
    revocation_for_new_offense = row.get("REV_NOFF_YN", None) in ["-1", "(1)"]
    revocation_for_absconsion = row.get("REV_ABSC_YN", None) in ["-1", "(1)"]
    revocation_for_technical = row.get("REV_TECH_YN", None) in ["-1", "(1)"]

    def _get_ncic_codes() -> List[str]:
        first = row.get("NEW_OFF", None)
        second = row.get("NEW_OFF2", None)
        third = row.get("NEW_OFF3", None)

        return [code for code in [first, second, third] if code]

    violation_types = []
    violation_type = None
    if revocation_for_new_offense:
        violation_type = StateSupervisionViolationType.LAW.value
        violation_types.append(violation_type)

        ncic_codes = _get_ncic_codes()
        violent_flags = [ncic.get_is_violent(code) for code in ncic_codes]
        violation.is_violent = str(any(violent_flags))
    elif revocation_for_absconsion:
        violation_type = StateSupervisionViolationType.ABSCONDED.value
        violation_types.append(violation_type)
    elif revocation_for_technical:
        violation_type = StateSupervisionViolationType.TECHNICAL.value
        violation_types.append(violation_type)
    # TODO(#2668): Once BQ dashboard for ND is using new pipeline calcs that reference
    #  state_supervision_violation_types (2750), delete the flat violation_type field on
    #  StateSupervisionViolation entirely.
    violation.violation_type = violation_type

    for violation_type in violation_types:
        vt = StateSupervisionViolationTypeEntry(violation_type=violation_type)
        create_if_not_exists(vt, violation, "state_supervision_violation_types")


def _record_revocation_on_violation_response(
    violation_response: StateSupervisionViolationResponse,
    terminating_officer: Optional[StateAgent],
) -> None:
    """Adds revocation information onto the provided |violation_response| as necessary."""
    violation_response.response_type = (
        StateSupervisionViolationResponseType.PERMANENT_DECISION.value
    )
    violation_response.decision = (
        StateSupervisionViolationResponseDecision.REVOCATION.value
    )
    if terminating_officer:
        create_if_not_exists(terminating_officer, violation_response, "decision_agents")
