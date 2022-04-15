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
from typing import Dict, List, Optional, Tuple, cast

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common import ncic
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.external_id_types import (
    US_ND_ELITE,
    US_ND_ELITE_BOOKING,
    US_ND_SID,
)
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    create_if_not_exists,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    IngestRowPrehookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.state_shared_row_posthooks import (
    IngestGatingContext,
    copy_name_to_alias,
    gen_convert_person_ids_to_external_id_objects,
    gen_label_single_external_id_hook,
    gen_normalize_county_codes_posthook,
    gen_set_agent_type,
    gen_set_is_life_sentence_hook,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    normalized_county_code,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_custom_parsers import (
    decimal_str_as_int_str,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_judicial_district_code_reference import (
    normalized_judicial_district_code,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_legacy_enum_helpers import (
    generate_enum_overrides,
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
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StateProgramAssignment,
    StateSupervisionCaseTypeEntry,
    StateSupervisionContact,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment


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
            "elite_offenderchargestable": [self._normalize_id_fields],
            "elite_orderstable": [self._normalize_id_fields],
        }
        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            "elite_offenders": [copy_name_to_alias],
            "elite_offenderidentifier": [self._normalize_external_id_type],
            "elite_offenderbookingstable": [self._add_person_external_id],
            "elite_offendersentences": [
                gen_set_is_life_sentence_hook("SENTENCE_CALC_TYPE", "LIFE")
            ],
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
            "docstars_contacts_v2": [
                self._add_supervision_officer_to_contact,
                gen_set_agent_type(StateAgentType.SUPERVISION_OFFICER),
                self._add_contact_fields,
            ],
        }

        self.primary_key_override_hook_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            "elite_offendersentences": _generate_sentence_primary_key,
            "elite_offenderchargestable": _generate_charge_primary_key,
            "elite_offense_in_custody_and_pos_report_data": _generate_incident_primary_key,
        }

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, AncestorChainOverridesCallable
        ] = {
            "elite_offenderchargestable": _state_charge_ancestor_chain_overrides,
        }

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """

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
            "elite_externalmovements_incarceration_periods",
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
            "docstars_contacts_v2",
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
                row[field_name] = decimal_str_as_int_str(row[field_name])

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
        return []

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag.startswith("elite"):
            if file_tag in (
                "elite_alias",
                "elite_offenderbookingstable",
                "elite_offenderidentifier",
                "elite_offenders",
                "elite_offense_in_custody_and_pos_report_data",
            ):
                return US_ND_ELITE
            return US_ND_ELITE_BOOKING
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

    def _add_contact_fields(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds a code string to various contact fields to the extracted supervision contact."""
        codes: List[str] = list(
            filter(len, [row.get(f"CONTACT_CODE_{i}", "") for i in range(1, 7)])
        )
        code_str = "-".join(codes)
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateSupervisionContact):
                extracted_object.contact_type = code_str
                extracted_object.contact_method = code_str
                extracted_object.status = code_str
                extracted_object.location = code_str

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
                f"File [{_gating_context.ingest_view_name}] is missing an SID external id"
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
                        ethnicity=StateEthnicity.HISPANIC.value
                    )
                    create_if_not_exists(
                        ethnicity_to_create, person, "state_person_ethnicities"
                    )
                else:
                    updated_person_races.append(person_race)
            person.state_person_races = updated_person_races

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
    def _add_person_external_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds a StatePersonExternalId with a US_ND_ELITE_BOOKING id_type."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                person_external_id = StatePersonExternalId(
                    state_person_external_id_id=_generate_person_book_id(row),
                    id_type=US_ND_ELITE_BOOKING,
                )
                create_if_not_exists(
                    person_external_id,
                    extracted_object,
                    "state_person_external_ids",
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
    def _set_elite_charge_status(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                # Note: If we hear about a charge in Elite, the person has already been sentenced.
                extracted_object.status = StateChargeStatus.SENTENCED.value

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
    booking_id = row["OFFENDER_BOOK_ID"]
    sentence_seq = row["SENTENCE_SEQ"]
    sentence_id = "-".join([booking_id, sentence_seq])
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


def _generate_period_id(row: Dict[str, str]) -> str:
    booking_id = row["OFFENDER_BOOK_ID"]
    movement_seq = row["MOVEMENT_SEQ"]

    return "-".join([booking_id, movement_seq])


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
    booking_id = row["OFFENDER_BOOK_ID"]
    charge_seq = row["CHARGE_SEQ"]

    return "-".join([booking_id, charge_seq])


def _generate_person_book_id(row: Dict[str, str]) -> str:
    return row["OFFENDER_BOOK_ID"]


def _state_charge_ancestor_chain_overrides(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> Dict[str, str]:
    # The charge id can be used interchangeably in ND with the sentence id because there is a 1:1 mapping between
    # charges and sentences, so the CHARGE_SEQ and SENTENCE_SEQ numbers can be used interchangeably.
    return {
        "state_incarceration_sentence": _generate_charge_id(row),
    }


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
