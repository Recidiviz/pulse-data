# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Direct ingest controller implementation for US_PA."""

import json
import re
from typing import List, Dict, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumOverrides,
    EnumMapper,
    EnumIgnorePredicate,
)
from recidiviz.common.constants.person_characteristics import Race, Gender, Ethnicity
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)
from recidiviz.common.constants.state.external_id_types import (
    US_PA_CONTROL,
    US_PA_PBPP,
    US_PA_SID,
    US_PA_INMATE,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentClass,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionLevel,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseType,
    StateSupervisionViolationResponseDecidingBodyType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import (
    CsvGcsfsDirectIngestController,
    IngestRowPosthookCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestAncestorChainOverridesCallable,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
    create_if_not_exists,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_assessment_level_reference import (
    set_date_specific_lsir_fields,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_enum_helpers import (
    incarceration_period_release_reason_mapper,
    concatenate_sci_incarceration_period_end_codes,
    incarceration_period_purpose_mapper,
    concatenate_sci_incarceration_period_purpose_codes,
    incarceration_period_admission_reason_mapper,
    concatenate_sci_incarceration_period_start_codes,
    revocation_type_mapper,
    assessment_level_mapper,
    concatenate_ccis_incarceration_period_start_codes,
    concatenate_ccis_incarceration_period_purpose_codes,
    concatenate_ccis_incarceration_period_end_codes,
    supervision_contact_location_mapper,
    supervision_contact_type_mapper,
    supervision_period_supervision_type_mapper,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_violation_type_reference import (
    violated_condition,
)
from recidiviz.ingest.direct.state_shared_row_posthooks import (
    copy_name_to_alias,
    gen_label_single_external_id_hook,
    gen_rationalize_race_and_ethnicity,
    gen_convert_person_ids_to_external_id_objects,
    IngestGatingContext,
)
from recidiviz.ingest.extractor.csv_data_extractor import (
    IngestFieldCoordinates,
)
from recidiviz.ingest.models.ingest_info import (
    IngestObject,
    StatePerson,
    StatePersonExternalId,
    StateAssessment,
    StateIncarcerationSentence,
    StateCharge,
    StateSentenceGroup,
    StateIncarcerationPeriod,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateSupervisionPeriod,
    StatePersonRace,
    StateSupervisionContact,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment

MAGICAL_DATES = ["20000000"]

AGENT_NAME_AND_ID_REGEX = re.compile(r"(.*?)( (\d+))$")


class UsPaController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_PA."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_PA.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)
        self.enum_overrides = self.generate_enum_overrides()

        sci_incarceration_period_row_postprocessors: List[IngestRowPosthookCallable] = [
            gen_label_single_external_id_hook(US_PA_CONTROL),
            self._concatenate_admission_reason_codes,
            self._concatenate_release_reason_codes,
            self._concatenate_incarceration_purpose_codes,
            self._add_incarceration_type,
            self._set_sci_incarceration_period_custodial_authority,
        ]

        doc_person_info_postprocessors: List[IngestRowPosthookCallable] = [
            gen_label_single_external_id_hook(US_PA_CONTROL),
            self.gen_hydrate_alternate_external_ids(
                {
                    "PBPP_Num": US_PA_PBPP,
                }
            ),
            copy_name_to_alias,
            gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            self._compose_current_address,
            self._hydrate_sentence_group_ids,
        ]
        dbo_Miscon_postprocessors: List[IngestRowPosthookCallable] = [
            gen_label_single_external_id_hook(US_PA_CONTROL),
            self._specify_incident_location,
            self._specify_incident_type,
            self._specify_incident_details,
            self._specify_incident_outcome,
        ]
        supervision_period_postprocessors: List[IngestRowPosthookCallable] = [
            self._unpack_supervision_period_conditions,
            self._set_supervising_officer,
            self._set_supervision_site,
            self._set_supervision_period_custodial_authority,
        ]

        dbo_offender_postprocessors: List[IngestRowPosthookCallable] = [
            gen_label_single_external_id_hook(US_PA_PBPP),
            self._hydrate_races,
            gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
        ]

        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            # TODO(#7222): Delete this once person_external_ids_v2 has shipped to prod
            "person_external_ids": [self._hydrate_person_external_ids],
            "person_external_ids_v2": [self._hydrate_person_external_ids],
            # TODO(#7222): Delete this once v2 has shipped to prod
            "doc_person_info": doc_person_info_postprocessors,
            "doc_person_info_v2": doc_person_info_postprocessors,
            "dbo_tblInmTestScore": [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self._generate_doc_assessment_external_id,
                self._enrich_doc_assessments,
            ],
            "dbo_Senrec": [
                self._set_incarceration_sentence_id,
                self._enrich_incarceration_sentence,
                self._strip_id_whitespace,
                self._rationalize_offense_type,
                self._set_is_violent,
            ],
            # TODO(#7222): Delete this once v2 has shipped to prod
            "sci_incarceration_period": sci_incarceration_period_row_postprocessors,
            "sci_incarceration_period_v2": sci_incarceration_period_row_postprocessors,
            "ccis_incarceration_period": [
                self._concatenate_admission_reason_codes,
                self._concatenate_release_reason_codes,
                self._add_incarceration_type,
                self._concatenate_incarceration_purpose_codes,
            ],
            "dbo_Miscon": dbo_Miscon_postprocessors,
            # TODO(#7222): Delete this once v2 has shipped to prod
            "dbo_Offender": dbo_offender_postprocessors,
            "dbo_Offender_v2": dbo_offender_postprocessors,
            # TODO(#7222): Delete this when dbo_LSIHistory has shipped to prod
            "dbo_LSIR": [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self._generate_legacy_pbpp_assessment_external_id,
                self._enrich_pbpp_assessments,
            ],
            "dbo_LSIHistory": [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self._generate_pbpp_assessment_external_id,
                self._enrich_pbpp_assessments,
            ],
            "supervision_period": supervision_period_postprocessors,
            # TODO(#6251): Rename to supervision_period and delete previous
            #  supervision_period v1 view once this version has been rerun in prod.
            "supervision_period_v2": supervision_period_postprocessors,
            "supervision_violation": [
                self._append_supervision_violation_entries,
            ],
            "supervision_violation_response": [
                self._append_supervision_violation_response_entries,
                self._set_violation_response_type,
            ],
            "board_action": [
                self._set_board_action_violation_response_fields,
                self._append_board_action_supervision_violation_response_entries,
            ],
            "supervision_contacts": [
                self._set_supervision_contact_agent,
                self._set_supervision_contact_fields,
            ],
        }

        self.file_post_processors_by_file: Dict[
            str, List[IngestFilePostprocessorCallable]
        ] = {
            # TODO(#7222): Delete this once person_external_ids_v2 has shipped to prod
            "person_external_ids": [],
            "person_external_ids_v2": [],
            # TODO(#7222): Delete this once v2 has shipped to prod
            "doc_person_info": [],
            "doc_person_info_v2": [],
            "dbo_tblInmTestScore": [],
            "dbo_Senrec": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            # TODO(#7222): Delete this once v2 has shipped to prod
            "sci_incarceration_period": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "sci_incarceration_period_v2": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "ccis_incarceration_period": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type)
            ],
            "dbo_Miscon": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            # TODO(#7222): Delete this once v2 has shipped to prod
            "dbo_Offender": [],
            "dbo_Offender_v2": [],
            "dbo_LSIR": [],
            "dbo_LSIHistory": [],
            "supervision_period": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            # TODO(#6251): Rename to supervision_period and delete previous
            #  supervision_period v1 view once this version has been rerun in prod.
            "supervision_period_v2": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "supervision_violation": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "supervision_violation_response": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "board_action": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type)
            ],
            "supervision_contacts": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type)
            ],
        }

        self.primary_key_override_hook_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            # TODO(#7222): Delete this once v2 has shipped to prod
            "sci_incarceration_period": _generate_sci_incarceration_period_primary_key,
            "sci_incarceration_period_v2": _generate_sci_incarceration_period_primary_key,
            "supervision_period": _generate_supervision_period_primary_key,
            # TODO(#6251): Rename to supervision_period and delete previous
            #  supervision_period v1 view once this version has been rerun in prod.
            "supervision_period_v2": _generate_supervision_period_primary_key,
            "supervision_violation": _generate_supervision_violation_primary_key,
            "supervision_violation_response": _generate_supervision_violation_response_primary_key,
            "board_action": _generate_board_action_supervision_violation_response_primary_key,
        }

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, IngestAncestorChainOverridesCallable
        ] = {
            # TODO(#7222): Delete this once v2 has shipped to prod
            "sci_incarceration_period": _state_incarceration_period_ancestor_chain_overrides,
            "sci_incarceration_period_v2": _state_incarceration_period_ancestor_chain_overrides,
            "ccis_incarceration_period": _state_incarceration_period_ancestor_chain_overrides,
            "supervision_violation_response": _state_supervision_violation_response_ancestor_chain_overrides,
        }

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: ["ASIAN", "A"],
        Race.BLACK: ["BLACK", "B"],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ["AMERICAN INDIAN", "I"],
        Race.OTHER: ["OTHER", "N"],
        Race.WHITE: ["WHITE", "W", "WW"],
        Ethnicity.HISPANIC: ["HISPANIC", "H"],
        Gender.FEMALE: ["FEMALE", "F"],
        Gender.MALE: ["MALE", "M", "MM"],
        # NOTE: We've only seen one instance of this as of 10/12/2020 - it could just be a typo.
        Gender.OTHER: ["OTHER", "N"],
        StateAssessmentType.CSSM: ["CSS-M"],
        StateAssessmentType.LSIR: ["LSI-R"],
        StateAssessmentType.PA_RST: ["RST"],
        StateAssessmentType.STATIC_99: ["ST99"],
        # TODO(#3020): Confirm the COMPLETED codes below. Some may be intermediate and not appropriately mapped as
        # final.
        StateSentenceStatus.COMPLETED: [
            "B",  # Bailed
            "CS",  # Change other Sentence
            "DA",  # Deceased - Assault
            "DN",  # Deceased - Natural
            "DS",  # Deceased - Suicide
            "DX",  # Deceased - Accident
            "DZ",  # Deceased - Non DOC Location
            "EX",  # Executed
            "FR",  # Federal Release
            "NC",  # Non-Return CSC
            "NF",  # Non-Return Furlough
            "NR",  # [Unlisted]
            "NW",  # Non-Return Work Release
            "P",  # Paroled
            "RP",  # Re-paroled (extremely rare)
            "SC",  # Sentence Complete
            "SP",  # Serve Previous
            "TC",  # Transfer to County
            "TS",  # Transfer to Other State
        ],
        StateSentenceStatus.COMMUTED: [
            "RD",  # Release Detentioner
            "RE",  # Received in Error
        ],
        StateSentenceStatus.PARDONED: [
            "PD",  # Pardoned
        ],
        StateSentenceStatus.SERVING: [
            "AS",  # Actively Serving
            "CT",  # In Court
            "DC",  # Diag/Class (Diagnostics / Classification)
            "EC",  # Escape CSC
            "EI",  # Escape Institution
            "F",  # Furloughed
            # TODO(#3312): What does it mean when someone else is in custody elsewhere? Does this mean they are no
            # longer the responsibility of the PA DOC? Should they also stop being counted towards population counts?
            # What does it mean when this code is used with a county code?
            "IC",  # In Custody Elsewhere
            "MH",  # Mental Health
            "SH",  # State Hospital
            "W",  # Waiting
            "WT",  # WRIT/ATA
        ],
        StateSentenceStatus.VACATED: [
            "VC",  # Vacated Conviction
            "VS",  # Vacated Sentence
        ],
        StateSentenceStatus.EXTERNAL_UNKNOWN: [
            "O",  # ??? (this is PA's own label; it means unknown within their own system)
        ],
        StateIncarcerationType.COUNTY_JAIL: [
            "C",  # County
            "CCIS",  # All CCIS periods are in contracted county facilities
        ],
        StateIncarcerationType.FEDERAL_PRISON: [
            "F",  # Federal
        ],
        StateIncarcerationType.OUT_OF_STATE: [
            "O",  # Transfer out of Pennsylvania
        ],
        StateIncarcerationType.STATE_PRISON: [
            "S",  # State
            "I",  # Transfer into Pennsylvania
            "T",  # County Transfer, i.e. transfer from county to state, usually for mental health services
            # ("5B Case")
            "P",  # SIP Program
            "E",  # SIP Evaluation
            "SCI",  # State Correctional Institution
        ],
        StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT: [
            "C",  # Cell Confinement
        ],
        StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT: [
            "Y",  # Restricted Confinement
        ],
        StateSupervisionType.PROBATION: [
            "Y",  # Yes means Probation; anything else means Parole
        ],
        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE: [
            "02",  # Paroled from SCI to PBPP Supervision
            "B2",  # Released according to Boot Camp Law
            "R2",  # RSAT Parole
            "C2",  # CCC Parole
            "03",  # Reparoled from SCI to PBPP Supervision
            "R3",  # RSAT Reparole
            "C3",  # CCC Reparole
        ],
        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE: [
            "04",  # Sentenced to Probation by County Judge and Supervised by PBPP
            "4A",  # ARD case - Sentenced by County Judge and Supervised by PBPP
            "4B",  # PWV case - Sentenced by County Judge and Supervised by PBPP
            "4C",  # COOP case - Offender on both PBPP and County Supervision
            "05",  # Special Parole sentenced by County and Supervised by PBPP
        ],
        StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN: [
            "08",  # Other States' Deferred Sentence
            "09",  # Emergency Release - used for COVID releases
        ],
        StateSupervisionPeriodAdmissionReason.TRANSFER_OUT_OF_STATE: [
            "06",  # Paroled/Reparoled by other state and transferred to PA
            "07",  # Sentenced to Probation by other state and transferred to PA
        ],
        StateSupervisionPeriodTerminationReason.ABSCONSION: [
            "45",  # Case closed for client with criminal charges pending that has reached maximum expiration
            # of sentence on paroled offense - usually applies to absconders or unconvicted violators
        ],
        StateSupervisionPeriodTerminationReason.DEATH: [
            "47",  # Death while under supervision of causes unrelated to crime
            "48",  # Death while under supervision caused by criminal activity
        ],
        StateSupervisionPeriodTerminationReason.DISCHARGE: [
            "46",  # The Board of Pardons grants a pardon or commutation which terminates supervision,
            # or early discharge is granted by a judge.
        ],
        StateSupervisionPeriodTerminationReason.EXPIRATION: [
            "43",  # Successful completion of sentence at maximum expiration date
            "49",  # Not an actually closed case - Case reached the Maximum Expiration Date for a State Sentence but
            # has a county sentence of probation to finish. Closes the case and reopens it as a county
            # probation case,
        ],
        StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN: [
            "50",  # Case Opened in Error
            "51",  # ?? Not in data dictionary
        ],
        StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION: [
            "44",  # Conviction and return to prison to serve detainer sentence
        ],
        StateSupervisionPeriodTerminationReason.REVOCATION: [
            "40",  # Recommitment to prison for new criminal convictions while under supervision
            "41",  # Recommitment to prison for adjudication of technical parole violations while under supervision
            "42",  # Recommitment to prison for convictions of new crimes and technical parole
            # violations while under supervision
        ],
        StateSupervisionLevel.MAXIMUM: [
            "MA",  # Maximum (shorter)
            "MAX",  # Maximum
        ],
        StateSupervisionLevel.MEDIUM: [
            "ME",  # Medium (shorter)
            "MED",  # Medium
        ],
        StateSupervisionLevel.MINIMUM: [
            "MI",  # Minimum (shorter)
            "MIN",  # Minimum
        ],
        StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY: [
            "MON",  # Monitoring
        ],
        StateSupervisionLevel.LIMITED: [
            "ADM",  # Administrative Parole
            "SPC",  # Special Circumstance
        ],
        StateSupervisionLevel.HIGH: [
            "ENH",  # Enhanced
        ],
        StateSupervisionLevel.INTERNAL_UNKNOWN: [
            "SPC",  # Special Circumstance
            "NOT",  # <Unclear what this is>
            # These are very old status codes that only show up in history table (dbo_Hist_Release), largely in records
            # from the 80s.
            "00",
            "50",
            "51",
            "52",
            "53",
            "54",
            "55",
            "56",
            "57",
            "58",
            "59",
            "5A",
            "5B",
            "5C",
            "5D",
            "5E",
            "5F",
            "5G",
            "5H",
            "5J",
            "5K",
        ],
        StateSupervisionViolationType.ABSCONDED: [
            "H06",  # Failure to report upon release
            "H09",  # Absconding
        ],
        StateSupervisionViolationType.LAW: [
            "H04",  # Pending criminal charges (UCV) Detained/Not detained
            "M20",  # Conviction of Misdemeanor Offense
            "M13",  # Conviction of a summary offense (a minor criminal, not civil offense)
        ],
        StateSupervisionViolationType.TECHNICAL: [
            "M04",  # Travel violations
            "H01",  # Changing residence without permission
            "M02",  # A - Failure to report as instructed
            "M19",  # B - Failure to notify agent of arrest or citation within 72 hrs
            "L07",  # C - Failure to notify agent of change in status/employment
            "M01",  # C - Failure to notify agent of change in status/employment
            "L08",  # A - Positive urine, drugs
            "M03",  # A - Positive urine, drugs
            "H12",  # A - Positive urine, drugs
            "H10",  # B - Possession of offense weapon
            "H11",  # B - Possession of firearm
            "H08",  # C - Assaultive behavior
            "L06",  # Failure to pay court ordered fees, restitution
            "L01",  # Failure to participate in community service
            "L03",  # Failure to pay supervision fees
            "L04",  # Failure to pay urinalysis fees
            "L05",  # Failure to support dependents
            "M05",  # Possession of contraband, cell phones, etc.
            "M06",  # Failure to take medications as prescribed
            "M07",  # Failure to maintain employment
            "M08",  # Failure to participate or maintain treatment
            "M09",  # Entering prohibited establishments
            "M10",  # Associating with gang members, co-defendants, etc
            "M11",  # Failure to abide by written instructions
            "M12",  # Failure to abide by field imposed special conditions
            "L02",  # Positive urine, alcohol (Previous History)
            "M14",  # Positive urine, alcohol (Previous History)
            "H03",  # Positive urine, alcohol (Previous History)
            "M15",  # Violating curfew
            "M16",  # Violating electronic monitoring
            "M17",  # Failure to provide urine
            "M18",  # Failure to complete treatment
            "H02",  # Associating with crime victims
            "H05",  # Failure to abide by Board Imposed Special Conditions
            "H07",  # Removal from Treatment/CCC Failure
        ],
        StateSupervisionViolationResponseDecision.COMMUNITY_SERVICE: [
            "COMS",  # Imposition of Community Service
        ],
        StateSupervisionViolationResponseDecision.DELAYED_ACTION: [
            "ACCG",  # Refer to ASCRA groups
        ],
        StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN: [
            "IAOD",
            "GCON",
            "GARR",
            "SAVE",  # Placement in SAVE
            "ARRT",  # Arrest
            "H03",
            "CON1",  # Administrative Conference 1
            "PV01",
            "PV02",
            "PV03",
            "PV04",
            "PV05",
            "PV06",
        ],
        StateSupervisionViolationResponseDecision.NEW_CONDITIONS: [
            "IRPT",  # Increased Reporting Requirements
            "CURF",  # Imposition of Curfew
            "ICRF",  # Imposition of Increased Curfew
            "URIN",  # Imposition of Increased Urinalysis Testing
            "EMOS",  # Imposition of Electronic Monitoring
            "AGPS",  # Imposition of Global Positioning
            "WTVR",  # Written Travel Restriction
            "DJBS",  # Documented Job Search
            "DRPT",  # Day Reporting Center
            "DFSE",  # Deadline for Securing Employment
            "RECT",  # Refer to Re-Entry Program
            "SCCC",  # Secure CCC
            "IMAT",  # Imposition of Mandatory Antabuse Use
            "PGPS",  # Imposition of Passive Global Positioning
        ],
        StateSupervisionViolationResponseDecision.OTHER: [
            "LOTR",  # Low Sanction Range - Other
            "MOTR",  # Medium Sanction Range - Other
            "HOTR",  # High Sanction Range - Other
        ],
        StateSupervisionViolationResponseDecision.REVOCATION: [
            "ARR2",  # Incarceration
            "CPCB",  # Placement in CCC Half Way Back
            "VCCF",  # Placement in PV Center
            "IDOX",  # Placement in D&A Detox Facility
            "IPMH",  # Placement in Mental Health Facility
            "VCCP",  # Placement in Violation Center County Prison
            "CPCO",  # Community Parole Corrections Half Way Out
        ],
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: [
            "RESCR",
            "RESCR6",
            "RESCR9",
            "RESCR12",
        ],
        StateSupervisionViolationResponseDecision.TREATMENT_IN_FIELD: [
            "OPAT",  # Placement in Out-Patient D&A Treatment
            "TXEV",  # Obtain treatment evaluation
            "GVPB",  # Refer to Violence Prevention Booster
        ],
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON: [
            "IPAT",  # Placement in In-Patient D&A Treatment
        ],
        StateSupervisionViolationResponseDecision.WARNING: [
            "WTWR",  # Written Warning
        ],
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION: [
            "RESCR",
            "RESCR6",
            "RESCR9",
            "RESCR12",
        ],
        StateCustodialAuthority.STATE_PRISON: [
            # SUPERVISION CUSTODIAL AUTHORITY CODES
            "09",  # Emergency Release - used for COVID releases
            # INCARCERATION CUSTODIAL AUTHORITY CODES
            "46",  # Technical parole violator being held in a contracted county facility
            "51",  # Receiving treatment in a contracted county facility
        ],
        StateCustodialAuthority.SUPERVISION_AUTHORITY: [
            # SUPERVISION CUSTODIAL AUTHORITY CODES
            # These periods are in-state probation cases supervised by PBPP. If we implement decision_making_authority,
            # these would have a type of COURT
            "4A",  # ARD case - Sentenced by County Judge and Supervised by PBPP
            "4B",  # PWV case - Sentenced by County Judge and Supervised by PBPP
            "4C",  # COOP case - Offender on both PBPP and County Supervision (deprecated)
            "04",  # Special Probation - Sentenced to Probation by County Judge and Supervised by PBPP
            "05",  # Special Parole - Sentenced by County and Supervised by PBPP
            # These periods are in-state parole cases supervised by PBPP. If we implement decision_making_authority,
            # these would have a type of PAROLE_BOARD
            "R2",  # RSAT Parole (deprecated)
            "C2",  # CCC Parole
            "C3",  # CCC Reparole
            "02",  # State Parole - Paroled from SCI to PBPP Supervision
            "03",  # State Rearole - Reparoled from SCI to PBPP Supervision
            "B2",  # Boot Camp - Released according to Boot Camp Law
            "R3",  # RSAT Reparole (deprecated)
            # These periods are supervised in-state for sentences from out-of-state. If we implement
            # decision_making_authority, these would have a type of OTHER_STATE
            "06",  # Other States' Parole/Reparole - Paroled/Reparoled by other state and transferred to PA
            "07",  # Other States' Probation - Sentenced to Probation by other state and transferred to PA
            "08",  # Other States' Deferred Sentence (deprecated)
            # INCARCERATION CUSTODIAL AUTHORITY CODES
            "26",  # Parolee in a Parole Violator Center
        ],
        StateSupervisionContactStatus.ATTEMPTED: ["Yes"],
        StateSupervisionContactStatus.COMPLETED: ["No"],
    }

    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {
        StateAssessmentLevel: assessment_level_mapper,
        StateIncarcerationPeriodAdmissionReason: incarceration_period_admission_reason_mapper,
        StateIncarcerationPeriodReleaseReason: incarceration_period_release_reason_mapper,
        StateSpecializedPurposeForIncarceration: incarceration_period_purpose_mapper,
        StateSupervisionViolationResponseRevocationType: revocation_type_mapper,
        StateSupervisionPeriodSupervisionType: supervision_period_supervision_type_mapper,
        StateSupervisionContactLocation: supervision_contact_location_mapper,
        StateSupervisionContactType: supervision_contact_type_mapper,
    }

    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {
        Gender: ["W", "B", "U", "A", "1"],  # Unexplained rare values
        Race: [
            "M",
            "U",
            "F",
            "QW",
            "WSW",
            "QB",
            "EW",
            "Q",
            "S",
        ],  # Unexplained rare values
        StateIncarcerationType: [
            "'"  # The dbo_Senrec table has several rows where the value type_of_sent is a single quotation mark
        ],
    }
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def get_file_tag_rank_list(self) -> List[str]:
        launched_file_tags = []
        if environment.in_gcp_production():
            # Data source: Mixed
            launched_file_tags.append("person_external_ids")
            # Data source: DOC
            launched_file_tags.append("doc_person_info")
        else:
            # TODO(#6251): Ungate this for next PA rerun.
            launched_file_tags.append("person_external_ids_v2")
            launched_file_tags.append("doc_person_info_v2")

        launched_file_tags += [
            "dbo_tblInmTestScore",
            "dbo_Senrec",
        ]

        if environment.in_gcp_production():
            launched_file_tags.append("sci_incarceration_period")
        else:
            launched_file_tags.append("sci_incarceration_period_v2")

        launched_file_tags += [
            "dbo_Miscon",
            # Data source: CCIS
            "ccis_incarceration_period",
        ]

        if environment.in_gcp_production():
            # Data source: PBPP
            launched_file_tags.append("dbo_Offender")
            launched_file_tags.append("dbo_LSIR")
            launched_file_tags.append("supervision_period")
        else:
            launched_file_tags.append("dbo_Offender_v2")
            launched_file_tags.append("dbo_LSIHistory")
            launched_file_tags.append("supervision_period_v2")

        launched_file_tags += [
            "supervision_violation",
            "supervision_violation_response",
            "board_action",
            "supervision_contacts",
        ]

        unlaunched_file_tags: List[str] = [
            # Empty for now
        ]

        file_tags = launched_file_tags
        if not environment.in_gcp():
            file_tags += unlaunched_file_tags

        return file_tags

    @classmethod
    def generate_enum_overrides(cls) -> EnumOverrides:
        """Provides Pennsylvania-specific overrides for enum mappings."""
        base_overrides = get_standard_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            cls.ENUM_OVERRIDES,
            cls.ENUM_IGNORES,
            cls.ENUM_MAPPERS,
            cls.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def _get_primary_key_override_for_file(
        self, file_tag: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file_tag, None)

    def _get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    @staticmethod
    def gen_hydrate_alternate_external_ids(
        columns_to_id_types: Dict[str, str]
    ) -> IngestRowPosthookCallable:
        """Generates a row post-hook that will hydrate alternate external ids than the "main" external id in a row, for
        rows which have multiple external ids to be hydrated.

        TODO(#1882): If yaml format supported raw values and multiple children of the same type,
        then this would be no-longer necessary.
        """

        def _hydrate_external_id(
            _gating_context: IngestGatingContext,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache,
        ) -> None:
            for obj in extracted_objects:
                if isinstance(obj, StatePerson):
                    external_ids_to_create = []
                    for column, id_type in columns_to_id_types.items():
                        value = row.get(column, "").strip()

                        if value:
                            external_ids_to_create.append(
                                StatePersonExternalId(
                                    state_person_external_id_id=value, id_type=id_type
                                )
                            )

                    for id_to_create in external_ids_to_create:
                        create_if_not_exists(
                            id_to_create, obj, "state_person_external_ids"
                        )

        return _hydrate_external_id

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
            "dbo_Senrec",
            # TODO(#7222): Delete this once v2 has shipped to prod
            "sci_incarceration_period",
            "sci_incarceration_period_v2",
            "ccis_incarceration_period",
            "dbo_Miscon",
        ]:
            return US_PA_CONTROL

        if file_tag in [
            "supervision_period",
            "supervision_period_v2",
            "supervision_violation",
            "supervision_violation_response",
            "board_action",
            "supervision_contacts",
        ]:
            return US_PA_PBPP
        return None

    @staticmethod
    def _hydrate_person_external_ids(
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Creates StatePersonExternalId entities from all of the identifiers in the given row."""
        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                control_numbers = (
                    row["control_numbers"].split(",") if row["control_numbers"] else []
                )
                parole_numbers = (
                    row["parole_numbers"].split(",") if row["parole_numbers"] else []
                )

                external_ids_to_create = []
                # TODO(#7222): Delete this once v2 has shipped to prod
                if gating_context.file_tag == "person_external_ids":
                    state_ids = row["state_ids"].split(",") if row["state_ids"] else []
                    for state_id in state_ids:
                        external_ids_to_create.append(
                            StatePersonExternalId(
                                state_person_external_id_id=state_id, id_type=US_PA_SID
                            )
                        )
                elif gating_context.file_tag == "person_external_ids_v2":
                    inmate_numbers = (
                        row["inmate_numbers"].split(",")
                        if row["inmate_numbers"]
                        else []
                    )
                    for inmate_number in inmate_numbers:
                        external_ids_to_create.append(
                            StatePersonExternalId(
                                state_person_external_id_id=inmate_number,
                                id_type=US_PA_INMATE,
                            )
                        )
                else:
                    raise ValueError(
                        f"Unexpected file_tag: [{gating_context.file_tag}]"
                    )

                for control_number in control_numbers:
                    external_ids_to_create.append(
                        StatePersonExternalId(
                            state_person_external_id_id=control_number,
                            id_type=US_PA_CONTROL,
                        )
                    )
                for parole_number in parole_numbers:
                    external_ids_to_create.append(
                        StatePersonExternalId(
                            state_person_external_id_id=parole_number,
                            id_type=US_PA_PBPP,
                        )
                    )
                for id_to_create in external_ids_to_create:
                    create_if_not_exists(id_to_create, obj, "state_person_external_ids")

    @staticmethod
    def _hydrate_sentence_group_ids(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                inmate_numbers = (
                    row["inmate_numbers"].split(",") if row["inmate_numbers"] else []
                )

                sentence_groups_to_create = []
                for inmate_number in inmate_numbers:
                    sentence_groups_to_create.append(
                        StateSentenceGroup(state_sentence_group_id=inmate_number)
                    )

                for sg_to_create in sentence_groups_to_create:
                    create_if_not_exists(sg_to_create, obj, "state_sentence_groups")

    @staticmethod
    def _hydrate_races(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                races = (
                    row["races_ethnicities_list"].split(",")
                    if row["races_ethnicities_list"]
                    else []
                )
                for race in races:
                    race_obj = StatePersonRace(race=race)
                    create_if_not_exists(race_obj, obj, "state_person_external_ids")

    @staticmethod
    def _compose_current_address(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Composes all of the address-related fields into a single address."""
        line_1 = row["legal_address_1"]
        line_2 = row["legal_address_2"]
        city = row["legal_city"]
        state = row["legal_state"]
        zip_code = row["legal_zip_code"]
        state_and_zip = f"{state} {zip_code}" if zip_code else state

        address = ", ".join(filter(None, (line_1, line_2, city, state_and_zip)))

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                obj.current_address = address

    ASSESSMENT_CLASSES: Dict[str, StateAssessmentClass] = {
        "CSS-M": StateAssessmentClass.SOCIAL,
        "HIQ": StateAssessmentClass.SOCIAL,
        "LSI-R": StateAssessmentClass.RISK,
        "RST": StateAssessmentClass.RISK,
        "ST99": StateAssessmentClass.SEX_OFFENSE,
        "TCU": StateAssessmentClass.SUBSTANCE_ABUSE,
    }

    @staticmethod
    def _generate_doc_assessment_external_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the assessment external_id to the extracted state assessment."""
        control_number = row["Control_Number"]
        inmate_number = row["Inmate_number"]
        test_id = row["Test_Id"]
        version_number = row["AsmtVer_Num"]
        external_id = "-".join([control_number, inmate_number, test_id, version_number])

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.state_assessment_id = external_id

    def _enrich_doc_assessments(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Enriches the assessment object with additional metadata."""

        def _rst_metadata() -> Optional[Dict]:
            version_flag = row.get("RSTRvsd_Flg", None)
            if version_flag:
                return {"latest_version": version_flag in ["1", "-1"]}
            return None

        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                assessment_type = (
                    obj.assessment_type.strip() if obj.assessment_type else ""
                )
                assessment_class = self.ASSESSMENT_CLASSES.get(assessment_type, None)
                if assessment_class:
                    obj.assessment_class = assessment_class.value

                if assessment_type == "RST":
                    rst_metadata = _rst_metadata()
                    if rst_metadata:
                        obj.assessment_metadata = json.dumps(rst_metadata)

                if assessment_type == "LSI-R":
                    set_date_specific_lsir_fields(obj)

    # TODO(#7222): Delete this when dbo_LSIHistory has shipped to prod
    @staticmethod
    def _generate_legacy_pbpp_assessment_external_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the assessment external_id to the extracted state assessment."""
        parole_number = row["ParoleNumber"]
        parole_count_id = row["ParoleCountID"]
        lsir_instance = row["LsirID"]
        external_id = "-".join([parole_number, parole_count_id, lsir_instance])

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.state_assessment_id = external_id

    @staticmethod
    def _generate_pbpp_assessment_external_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the assessment external_id to the extracted state assessment."""
        parole_number = row["ParoleNumber"]
        parole_count_id = row["ParoleCountID"]
        lsir_instance = row["LsirID"]
        release_status = row["ReleaseStatus"]
        external_id = "-".join(
            [parole_number, parole_count_id, lsir_instance, release_status]
        )

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.state_assessment_id = external_id

    @staticmethod
    def _enrich_pbpp_assessments(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Enriches the assessment object with additional metadata."""
        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                obj.assessment_type = StateAssessmentType.LSIR.value
                obj.assessment_class = StateAssessmentClass.RISK.value
                set_date_specific_lsir_fields(obj)

    @staticmethod
    def _set_incarceration_sentence_id(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        sentence_group_id = row["curr_inmate_num"]
        sentence_number = row["type_number"]
        sentence_id = f"{sentence_group_id}-{sentence_number}"

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                obj.state_incarceration_sentence_id = sentence_id

    @staticmethod
    def _enrich_incarceration_sentence(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Enriches incarceration sentences by setting sentence length and boolean fields."""
        max_years = row.get("max_cort_sent_yrs", "0")
        max_months = row.get("max_cort_sent_mths", "0")
        max_days = row.get("max_cort_sent_days", "0")
        min_years = row.get("min_cort_sent_yrs", "0")
        min_months = row.get("min_cort_sent_mths", "0")
        min_days = row.get("min_cort_sent_days", "0")

        sentence_class = row.get("class_of_sent", "")
        is_life = sentence_class in ("CL", "LF")
        is_capital_punishment = sentence_class in ("EX", "EP")

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                start_date = obj.start_date
                max_time = parse_days_from_duration_pieces(
                    years_str=max_years,
                    months_str=max_months,
                    days_str=max_days,
                    start_dt_str=start_date,
                )
                min_time = parse_days_from_duration_pieces(
                    years_str=min_years,
                    months_str=min_months,
                    days_str=min_days,
                    start_dt_str=start_date,
                )

                if max_time:
                    obj.max_length = str(max_time)
                if min_time:
                    obj.min_length = str(min_time)

                obj.is_life = str(is_life)
                obj.is_capital_punishment = str(is_capital_punishment)

    # TODO(#3020): When PA is switched to use SQL pre-processing, this will no longer be necessary.
    @staticmethod
    def _strip_id_whitespace(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Strips id fields provided as strings with inconsistent whitespace padding to avoid id matching issues."""
        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                if obj.state_charge_id:
                    obj.state_charge_id = obj.state_charge_id.strip()

    @staticmethod
    def _set_is_violent(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        asca_category = row.get("ASCA_Category___Ranked", None)
        is_violent = asca_category == "1-Violent"

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.is_violent = str(is_violent)

    @staticmethod
    def _rationalize_offense_type(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        category = row.get("Category", "")
        sub_category = row.get("SubCategory", None)
        offense_type = "-".join(filter(None, [category, sub_category]))

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.offense_type = offense_type

    @staticmethod
    def _concatenate_admission_reason_codes(
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Concatenates the incarceration period admission reason-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if gating_context.file_tag in (
                    # TODO(#7222): Delete this once v2 has shipped to prod
                    "sci_incarceration_period",
                    "sci_incarceration_period_v2",
                ):
                    obj.admission_reason = (
                        concatenate_sci_incarceration_period_start_codes(row)
                    )
                elif gating_context.file_tag == "ccis_incarceration_period":
                    obj.admission_reason = (
                        concatenate_ccis_incarceration_period_start_codes(row)
                    )

    @staticmethod
    def _concatenate_release_reason_codes(
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Concatenates the incarceration period release reason-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date:
                    if gating_context.file_tag in (
                        # TODO(#7222): Delete this once v2 has shipped to prod
                        "sci_incarceration_period",
                        "sci_incarceration_period_v2",
                    ):
                        obj.release_reason = (
                            concatenate_sci_incarceration_period_end_codes(row)
                        )
                    elif gating_context.file_tag == "ccis_incarceration_period":
                        obj.release_reason = (
                            concatenate_ccis_incarceration_period_end_codes(row)
                        )

    @staticmethod
    def _concatenate_incarceration_purpose_codes(
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Concatenates the incarceration period specialized purpose-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if gating_context.file_tag in (
                    # TODO(#7222): Delete this once v2 has shipped to prod
                    "sci_incarceration_period",
                    "sci_incarceration_period_v2",
                ):
                    obj.specialized_purpose_for_incarceration = (
                        concatenate_sci_incarceration_period_purpose_codes(row)
                    )
                elif gating_context.file_tag == "ccis_incarceration_period":
                    obj.specialized_purpose_for_incarceration = (
                        concatenate_ccis_incarceration_period_purpose_codes(row)
                    )

    @staticmethod
    def _add_incarceration_type(
        gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets incarceration type on incarceration periods based on facility."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if gating_context.file_tag in (
                    # TODO(#7222): Delete this once v2 has shipped to prod
                    "sci_incarceration_period",
                    "sci_incarceration_period_v2",
                ):
                    # TODO(#3312): Figure out how to fill out the incarceration_type COUNTY_JAIL/STATE/FEDERAL based on
                    #  IC sentence status + location codes? Ask PA about this!
                    obj.incarceration_type = "SCI"
                elif gating_context.file_tag == "ccis_incarceration_period":
                    obj.incarceration_type = "CCIS"

    @staticmethod
    def _specify_incident_location(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Specifies the exact location where the incarceration incident took place."""
        place_code = row.get("place_hvl_code", None)
        place_extended = row.get("place_extended", None)
        location = "-".join(filter(None, [place_code, place_extended]))

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                if location:
                    obj.location_within_facility = location

    @staticmethod
    def _specify_incident_type(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Specifies the type of incarceration incident."""
        drug_related = row.get("drug_related", None)
        is_contraband = drug_related == "Y"

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                if is_contraband:
                    obj.incident_type = StateIncarcerationIncidentType.CONTRABAND.value
                else:
                    obj.incident_type = StateIncarcerationIncidentType.REPORT.value

    @staticmethod
    def _specify_incident_details(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Specifies the incarceration incident details. This is a grouping of flags indicating whether certain
        "classes" of charges are involved in the incident."""
        category_1 = row.get("ctgory_of_chrgs_1", None)
        category_2 = row.get("ctgory_of_chrgs_2", None)
        category_3 = row.get("ctgory_of_chrgs_3", None)
        category_4 = row.get("ctgory_of_chrgs_4", None)
        category_5 = row.get("ctgory_of_chrgs_5", None)

        details_mapping = {
            "category_1": category_1,
            "category_2": category_2,
            "category_3": category_3,
            "category_4": category_4,
            "category_5": category_5,
        }

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                obj.incident_details = json.dumps(details_mapping)

    @staticmethod
    def _specify_incident_outcome(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Specifies the type of outcome of the incarceration incident."""
        misconduct_number = row.get("misconduct_number", None)
        confinement_code = row.get("confinement", None)
        confinement_date = row.get("confinement_date", None)
        is_restricted = confinement_code == "Y"
        is_cell = confinement_code == "C"

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncidentOutcome):
                if misconduct_number:
                    obj.state_incarceration_incident_outcome_id = misconduct_number

                if is_restricted or is_cell:
                    obj.outcome_type = confinement_code
                    obj.date_effective = confinement_date

    @staticmethod
    def _unpack_supervision_period_conditions(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Unpacks the comma-separated string of condition codes into an array of strings."""
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                conditions = (
                    row["condition_codes"].split(",") if row["condition_codes"] else []
                )
                if conditions:
                    obj.conditions = conditions

    @staticmethod
    def _set_supervising_officer(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the supervision officer (as an Agent entity) on the supervision period."""
        officer_full_name_and_id = row.get("supervising_officer_name", None)

        if officer_full_name_and_id and (
            "Vacant, Position" in officer_full_name_and_id
            or "Position, Vacant" in officer_full_name_and_id
        ):
            # This is a placeholder name for when a person does not actually have a PO
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if officer_full_name_and_id:
                    # TODO(#4159): Update this regex to extract and set the given_names and surname from the full_name
                    match = re.match(AGENT_NAME_AND_ID_REGEX, officer_full_name_and_id)
                    if match:
                        full_name = match.group(1)
                        external_id: Optional[str] = match.group(3)
                    else:
                        full_name = officer_full_name_and_id
                        external_id = None
                    obj.create_state_agent(
                        state_agent_id=external_id,
                        full_name=full_name,
                        agent_type=StateAgentType.SUPERVISION_OFFICER.value,
                    )

    @staticmethod
    def _set_supervision_site(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the supervision_site on the supervision period."""
        district_office = row["district_office"]
        district_sub_office_id = row["district_sub_office_id"]
        supervision_location_org_code = row["supervision_location_org_code"]
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if district_office:
                    obj.supervision_site = f"{district_office}|{district_sub_office_id}|{supervision_location_org_code}"

    @staticmethod
    def _set_supervision_period_custodial_authority(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the custodial_authority on the supervision period."""
        # TODO(#1882): This row post hook should not be necessary once you can map a column value to multiple fields on
        #  the ingested object.
        supervision_types = row["supervision_types"]

        # For dual supervision types, the custodial authority will always be the supervision authority, so we
        # just arbitrarily pick raw text to map to an enum.
        custodial_authority = (
            supervision_types.split(",")[0] if supervision_types else supervision_types
        )

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                obj.custodial_authority = custodial_authority

    @staticmethod
    def _set_sci_incarceration_period_custodial_authority(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the custodial_authority on the incarceration period."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.custodial_authority = StateCustodialAuthority.STATE_PRISON.value

    @staticmethod
    def _append_supervision_violation_entries(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Appends violation type and violated condition entries to the parent supervision violation."""
        parole_number = row["parole_number"]
        parole_count_id = row["parole_count_id"]
        set_id = row["set_id"]

        raw_violation_types = row.get("violation_types", "[]")
        violation_types = json.loads(raw_violation_types)

        conditions_violated = []
        for violation_type in violation_types:
            sequence_id = violation_type["sequence_id"]
            violation_code = violation_type["violation_code"]
            condition_violated = violated_condition(violation_code)
            if condition_violated not in conditions_violated:
                conditions_violated.append(condition_violated)

            violation_type_entry_id = (
                f"{parole_number}-{parole_count_id}-{set_id}-{sequence_id}"
            )

            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionViolation):
                    obj.create_state_supervision_violation_type_entry(
                        state_supervision_violation_type_entry_id=violation_type_entry_id,
                        violation_type=violation_code,
                    )

        for condition_violated in conditions_violated:
            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionViolation):
                    obj.create_state_supervision_violated_condition_entry(
                        condition=condition_violated,
                    )

    @staticmethod
    def _append_supervision_violation_response_entries(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Appends violation response decision entries to the parent supervision violation response."""
        parole_number = row["parole_number"]
        parole_count_id = row["parole_count_id"]
        set_id = row["set_id"]

        raw_sanction_types = row.get("sanction_types", "[]")
        sanction_types = json.loads(raw_sanction_types)

        for sanction_type in sanction_types:
            sequence_id = sanction_type["sequence_id"]
            sanction_code = sanction_type["sanction_code"]

            entry_id = f"{parole_number}-{parole_count_id}-{set_id}-{sequence_id}"

            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionViolationResponse):
                    obj.create_state_supervision_violation_response_decision_entry(
                        state_supervision_violation_response_decision_entry_id=entry_id,
                        decision=sanction_code,
                        revocation_type=sanction_code,
                    )

    @staticmethod
    def _set_violation_response_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets relevant fields on the parent supervision violation response.

        We set all violation response types as VIOLATION REPORT because we have no additional metadata to make
        a meaningful distinction, and this ensures that they will all be included in relevant analysis
        """
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.response_type = (
                    StateSupervisionViolationResponseType.VIOLATION_REPORT.value
                )

    @staticmethod
    def _set_board_action_violation_response_fields(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets relevant fields specific to a board action supervision violation response."""
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.response_type = (
                    StateSupervisionViolationResponseType.PERMANENT_DECISION.value
                )
                obj.deciding_body_type = (
                    StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD.value
                )

    @staticmethod
    def _append_board_action_supervision_violation_response_entries(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Appends board action violation response decision entries to the parent supervision violation response."""
        parole_number = row["ParoleNumber"]
        parole_count_id = row["ParoleCountID"]
        board_action_id = row["BdActionID"]
        entry_id = f"{parole_number}-{parole_count_id}-{board_action_id}"
        condition_code = row["CndConditionCode"]
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.create_state_supervision_violation_response_decision_entry(
                    state_supervision_violation_response_decision_entry_id=entry_id,
                    decision=condition_code,
                    revocation_type=condition_code,
                )

    @staticmethod
    def _set_supervision_contact_agent(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the supervision officer (as a contacted Agent entity) on the supervision contact."""
        officer_id = row.get("agent_number", None)
        officer_first_name = row.get("agent_first_name", None)
        officer_last_name = row.get("agent_last_name", None)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionContact):
                if officer_id and officer_first_name and officer_last_name:
                    full_name = (
                        f"{officer_last_name.upper()}, {officer_first_name.upper()}"
                    )
                    obj.create_state_agent(
                        state_agent_id=officer_id,
                        full_name=full_name,
                        agent_type=StateAgentType.SUPERVISION_OFFICER.value,
                    )

    @staticmethod
    def _set_supervision_contact_fields(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the supervision contact fields accordingly so that they can be parsed by enum mappers."""
        contact_type = row["contact_type"]
        method = row["method"].replace("-", "")
        collateral_type = row["collateral_type"].replace(" ", "").replace("/", "")

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionContact):
                if not collateral_type:
                    obj.location = f"None-{method}"
                else:
                    obj.location = f"{collateral_type}-{method}"
                obj.contact_type = f"{contact_type}-{method}"


def _generate_sci_incarceration_period_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    sentence_group_id = row["inmate_number"]
    sequence_number = row["sequence_number"]
    incarceration_period_id = f"{sentence_group_id}-{sequence_number}"

    return IngestFieldCoordinates(
        "state_incarceration_period",
        "state_incarceration_period_id",
        incarceration_period_id,
    )


def _state_incarceration_period_ancestor_chain_overrides(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> Dict[str, str]:
    """This creates an incarceration sentence id for specifying the ancestor of an incarceration period.

    Incarceration periods only have explicit links to sentence groups. However, we know that the vast majority of
    sentence groups in PA have a single sentence with a type number of 01, and the rest have 2 sentences with type
    numbers of 01 and 02. The fields for sentences 01 and 02 are highly similar and usually differ only as it
    relates to charge information. Thus, tying each incarceration period to sentence 01 in a given group appears
    to be safe.
    """
    sentence_group_id = row["inmate_number"]
    assumed_type_number = "01"
    incarceration_sentence_id = f"{sentence_group_id}-{assumed_type_number}"

    return {"state_incarceration_sentence": incarceration_sentence_id}


def _generate_supervision_period_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    person_id = row["parole_number"]
    period_sequence_number = row["period_sequence_number"]
    supervision_period_id = f"{person_id}-{period_sequence_number}"

    return IngestFieldCoordinates(
        "state_supervision_period", "state_supervision_period_id", supervision_period_id
    )


def _generate_supervision_violation_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    person_id = row["parole_number"]
    parole_count = row["parole_count_id"]
    set_id = row["set_id"]
    violation_id = f"{person_id}-{parole_count}-{set_id}"

    return IngestFieldCoordinates(
        "state_supervision_violation", "state_supervision_violation_id", violation_id
    )


def _state_supervision_violation_ancestor_chain_overrides(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> Dict[str, str]:
    person_id = row["parole_number"]
    parole_count = row["parole_count_id"]
    period_id = f"{person_id}-{parole_count}"

    return {"state_supervision_period": period_id}


def _generate_supervision_violation_response_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    person_id = row["parole_number"]
    parole_count = row["parole_count_id"]
    set_id = row["set_id"]
    response_id = f"{person_id}-{parole_count}-{set_id}"

    return IngestFieldCoordinates(
        "state_supervision_violation_response",
        "state_supervision_violation_response_id",
        response_id,
    )


def _generate_board_action_supervision_violation_response_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    parole_id = row["ParoleNumber"]
    parole_count_id = row["ParoleCountID"]
    board_action_id = row["BdActionID"]
    response_id = f"{parole_id}-{parole_count_id}-{board_action_id}"

    return IngestFieldCoordinates(
        "state_supervision_violation_response",
        "state_supervision_violation_response_id",
        response_id,
    )


def _state_supervision_violation_response_ancestor_chain_overrides(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> Dict[str, str]:
    person_id = row["parole_number"]
    parole_count = row["parole_count_id"]
    set_id = row["set_id"]

    violation_id = f"{person_id}-{parole_count}-{set_id}"

    return {"state_supervision_violation": violation_id}
