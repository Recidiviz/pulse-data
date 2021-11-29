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
from enum import Enum
from typing import Dict, List, Optional, Type

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.person_characteristics import Gender, Race
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)
from recidiviz.common.constants.state.external_id_types import US_PA_CONTROL, US_PA_PBPP
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.states import StateCode
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
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    create_if_not_exists,
    update_overrides_from_maps,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_assessment_level_reference import (
    set_date_specific_lsir_fields,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_legacy_enum_helpers import (
    assessment_level_mapper,
    concatenate_ccis_incarceration_period_end_codes,
    concatenate_ccis_incarceration_period_purpose_codes,
    concatenate_ccis_incarceration_period_start_codes,
    concatenate_sci_incarceration_period_end_codes,
    concatenate_sci_incarceration_period_purpose_codes,
    concatenate_sci_incarceration_period_start_codes,
    incarceration_period_admission_reason_mapper,
    incarceration_period_purpose_mapper,
    incarceration_period_release_reason_mapper,
    supervision_contact_location_mapper,
    supervision_period_supervision_type_mapper,
)
from recidiviz.ingest.direct.state_shared_row_posthooks import (
    IngestGatingContext,
    gen_convert_person_ids_to_external_id_objects,
    gen_label_single_external_id_hook,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import (
    IngestObject,
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StateSupervisionContact,
    StateSupervisionPeriod,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment

MAGICAL_DATES = ["20000000"]

AGENT_NAME_AND_ID_REGEX = re.compile(r"([^,]*), (.*?) (\d+)$")
FALLBACK_AGENT_NAME_AND_ID_REGEX = re.compile(r"(.*?) (\d+.*)$")


# TODO(#8902): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsPaController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
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

        supervision_period_postprocessors: List[IngestRowPosthookCallable] = [
            self._unpack_supervision_period_conditions,
            self._set_supervising_officer,
            self._set_supervision_site,
            self._set_supervision_period_custodial_authority,
        ]

        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            "dbo_tblInmTestScore": [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self._generate_doc_assessment_external_id,
                self._enrich_doc_assessments,
            ],
            "sci_incarceration_period": sci_incarceration_period_row_postprocessors,
            "ccis_incarceration_period": [
                self._concatenate_admission_reason_codes,
                self._concatenate_release_reason_codes,
                self._add_incarceration_type,
                self._concatenate_incarceration_purpose_codes,
            ],
            "dbo_LSIHistory": [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self._generate_pbpp_assessment_external_id,
                self._enrich_pbpp_assessments,
            ],
            # TODO(#10138): [US_PA] Transition supervision_periods to v4
            "supervision_period_v3": supervision_period_postprocessors,
            "supervision_period_v4": supervision_period_postprocessors,
            "supervision_contacts": [
                self._set_supervision_contact_agent,
                self._set_supervision_contact_fields,
            ],
        }

        self.file_post_processors_by_file: Dict[
            str, List[IngestFilePostprocessorCallable]
        ] = {
            "dbo_tblInmTestScore": [],
            "sci_incarceration_period": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "ccis_incarceration_period": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type)
            ],
            "dbo_LSIR": [],
            "dbo_LSIHistory": [],
            # TODO(#10138): [US_PA] Transition supervision_periods to v4
            "supervision_period_v3": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "supervision_period_v4": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            "supervision_contacts": [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type)
            ],
        }

        self.primary_key_override_hook_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            "sci_incarceration_period": _generate_sci_incarceration_period_primary_key,
            # TODO(#10138): [US_PA] Transition supervision_periods to v4
            "supervision_period_v3": _generate_supervision_period_primary_key,
            "supervision_period_v4": _generate_supervision_period_primary_key,
            "supervision_contacts": _generate_supervision_contact_primary_key,
        }

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, IngestAncestorChainOverridesCallable
        ] = {
            "sci_incarceration_period": _state_incarceration_period_ancestor_chain_overrides,
            "ccis_incarceration_period": _state_incarceration_period_ancestor_chain_overrides,
        }

    ENUM_OVERRIDES: Dict[Enum, List[str]] = {
        StateAssessmentType.CSSM: ["CSS-M"],
        StateAssessmentType.LSIR: ["LSI-R"],
        StateAssessmentType.PA_RST: ["RST"],
        StateAssessmentType.STATIC_99: ["ST99"],
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
        StateSupervisionSentenceSupervisionType.PROBATION: [
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
        StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION: [
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
        StateCustodialAuthority.STATE_PRISON: [
            # SUPERVISION CUSTODIAL AUTHORITY CODES
            "09",  # Emergency Release - used for COVID releases
            # INCARCERATION CUSTODIAL AUTHORITY CODES
            "46",  # Technical Parole Violator: 6-9-12 Month Revocation in a contracted county facility
            "51",  # Detox: Treatment Revocation in a contracted county facility
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
            "1",  # AOD  -  Alcohol or Other Drugs
            "2",  # BCA  -  Boot Camp Aftercare
            "3",  # CPC  -  Halfway Back Parolee
            "4",  # CTEP  -  Comprehensive Transitional & Education Program
            "5",  # DRP  -  Day Reporting Program
            "6",  # EAL  -  Elderly Assisted Living
            "7",  # EMI  -  Extreme Mental Illness
            "8",  # GH  -  Group Home
            "9",  # HTP  -  Hard To Place
            "10",  # MH  -  Mental Health
            "11",  # PEN  -  PENNCAP  Pre - rel Secure Facility
            "12",  # RSAT  -  Residential Substance Abuse Treatment
            "13",  # SAVE  -  SAVE
            "14",  # SIP2  -  State Intermediate Punishment - 2
            "15",  # SO  -  Sex Offender
            "16",  # YAO  -  Young Adult Offenders
            "17",  # BT  -  Back On Track
            "18",  # DD  -  Dual - diagnosis
            "19",  # SIP3  -  State Intermediate Punishment - 3
            "20",  # SIP4  -  State Intermediate Punishment - 4
            "21",  # SIP3-NR - State Intermediate Punishment 3 Non - Resident
            "22",  # SIP4-NR - State Intermediate Punishment 4 Non - Resident
            "23",  # VPB  -  Violence Prevention Booster
            "24",  # CA  -  County Assessment
            "25",  # WR  -  Work Release
            "26",  # PV  -  Parole Violator / Parolee in a Parole Violator Center
            "27",  # PVC  -  Parole Violator Completion
            "28",  # RPV  -  Return Parole Violator
            "29",  # VOC  -  Violent Offender Completion
            "30",  # RVO  -  Return Violent Offender
            "31",  # CA / G  -  County Assessment / Grant
            "32",  # WR / G  -  Work Release / Grant
            "33",  # TFC  -  Thinking for a Change
            "34",  # TFC-W  -  Thinking for a Change - Waiting
            "35",  # TFC-C  -  Thinking for a Change - Completed
            "36",  # BAT  -  Batterers Intervention
            "37",  # BAT-W  -  Batterers Intervention - Waiting
            "38",  # BAT-C  -  Batterers Intervention - Completed
            "39",  # VPM  -  Violence Prevention Moderate
            "40",  # VPM-W  -  Violence Prevention Moderate - Waiting
            "41",  # VPM-C  -  Violence Prevention Moderate - Completed
            "42",  # VPB-W  -  Violence Prevention Booster - Waiting
            "43",  # VPB-C  -  Violence Prevention Booster - Completed
            "44",  # DPW-T  -  TRN to DPW
            "45",  # DPW-R  -  RTN from DPW
            "47",  # TPV-C  -  Technical Parole Violator - Completed
            "48",  # VET  -  Veteran
            "52",  # DET-C  -  Detox Completion
            "53",  # SEP-P  -  PREA
            "54",  # SEP-T  -  Threat
            "55",  # SEP-E  -  Employee
            "56",  # JL  -  Juvenile Lifer
            "57",  # SIP-MA  -  SIP - Medical Assistance
            "58",  # SS  -  Social Security
            "59",  # MM  -  Medical / Mental
            "60",  # CL  -  Commuted Lifers
            "61",  # ACT  -  ACT 122
            "62",  # SDTP2  -  State Drug Treatment Program  2
            "63",  # SDTP3  -  State Drug Treatment Program  3
            "64",  # SDTP3-NR  -  State Drug Treatment Program  3 Non-Resident
            "65",  # SDTP4  -  State Drug Treatment Program  4
            "66",  # SDTP4-NR  -  State Drug Treatment Program  4 Non-Resident
            "67",  # SDTP-EX  -  State Drug Treatment Program  Extension
            "68",  # QD  -  Quick Dips
            "69",  # SDTP-QD  -  State Drug Treatment Program Quick Dips
            "70",  # COV  -  COVID 19 Furlough
            "71",  # MA  -  Medical Assistance
            "72",  # NR  -  Non Resident
            "73",  # EX  -  Extended
        ],
        StateSupervisionContactStatus.ATTEMPTED: ["Yes"],
        StateSupervisionContactStatus.COMPLETED: ["No"],
        StateSupervisionContactType.DIRECT: ["Offender"],
        StateSupervisionContactType.COLLATERAL: ["Collateral"],
        StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT: ["Both"],
        StateSupervisionContactMethod.TELEPHONE: ["Phone-Voicemail", "Phone-Voice"],
        StateSupervisionContactMethod.WRITTEN_MESSAGE: [
            "Email",
            "Phone-Text",
            "Mail",
            "Facsimile",
        ],
        StateSupervisionContactMethod.IN_PERSON: [
            "Home",
            "Office",
            "Field",
            "Work",
        ],
    }

    ENUM_MAPPER_FUNCTIONS: Dict[Type[Enum], EnumMapperFn] = {
        StateAssessmentLevel: assessment_level_mapper,
        StateIncarcerationPeriodAdmissionReason: incarceration_period_admission_reason_mapper,
        StateIncarcerationPeriodReleaseReason: incarceration_period_release_reason_mapper,
        StateSpecializedPurposeForIncarceration: incarceration_period_purpose_mapper,
        StateSupervisionPeriodSupervisionType: supervision_period_supervision_type_mapper,
        StateSupervisionContactLocation: supervision_contact_location_mapper,
    }

    ENUM_IGNORES: Dict[Type[Enum], List[str]] = {
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
        ],
    }
    ENUM_IGNORE_PREDICATES: Dict[Type[Enum], EnumIgnorePredicate] = {}

    def get_file_tag_rank_list(self) -> List[str]:
        launched_file_tags = [
            # Data source: Mixed
            "person_external_ids",
            # Data source: DOC
            "doc_person_info",
            "dbo_tblInmTestScore",
            "dbo_Senrec",
            "sci_incarceration_period",
            "dbo_Miscon",
            # Data source: CCIS
            "ccis_incarceration_period",
            # Data source: PBPP
            "dbo_Offender",
            "dbo_LSIHistory",
            "supervision_violation",
            "supervision_violation_response",
            "board_action",
            "supervision_contacts",
        ]

        # TODO(#10138): [US_PA] Transition supervision_periods to v4
        if (
            not environment.in_gcp_production()
            or self.ingest_instance is DirectIngestInstance.SECONDARY
        ):
            launched_file_tags.append("supervision_period_v4")
        else:
            launched_file_tags.append("supervision_period_v3")

        unlaunched_file_tags: List[str] = [
            # Empty for now
        ]

        file_tags = launched_file_tags
        if not environment.in_gcp():
            file_tags += unlaunched_file_tags

        # TODO(#9882): Update to `dbo_Senrec_v2` everywhere once rerun completes.
        if (
            not environment.in_gcp_production()
            or self.ingest_instance == DirectIngestInstance.SECONDARY
        ):
            idx_of_senrec = file_tags.index("dbo_Senrec")
            file_tags[idx_of_senrec] = "dbo_Senrec_v2"

        return file_tags

    @classmethod
    def generate_enum_overrides(cls) -> EnumOverrides:
        """Provides Pennsylvania-specific overrides for enum mappings."""
        base_overrides = get_standard_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            cls.ENUM_OVERRIDES,
            cls.ENUM_IGNORES,
            cls.ENUM_MAPPER_FUNCTIONS,
            cls.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    # TODO(#8902): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def get_primary_key_override_for_file(
        self, file_tag: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file_tag, None)

    def get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    def get_row_pre_processors_for_file(
        self, _file_tag: str
    ) -> List[IngestRowPrehookCallable]:
        return []

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []

    @staticmethod
    def gen_hydrate_alternate_external_ids(
        columns_to_id_types: Dict[str, str]
    ) -> IngestRowPosthookCallable:
        """Generates a row post-hook that will hydrate alternate external ids than the "main" external id in a row, for
        rows which have multiple external ids to be hydrated.

        TODO(#8902): Delete this entirely once all US_PA ingest views are migrated to
        ingest mappings v2.
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
                        value = row[column].strip()

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
            "sci_incarceration_period",
            "ccis_incarceration_period",
        ]:
            return US_PA_CONTROL

        # TODO(#10138): [US_PA] Transition supervision_periods to v4
        if file_tag in [
            "supervision_period_v3",
            "supervision_period_v4",
            "supervision_contacts",
        ]:
            return US_PA_PBPP
        return None

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
    def _concatenate_admission_reason_codes(
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Concatenates the incarceration period admission reason-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if gating_context.file_tag in ("sci_incarceration_period",):
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
                    if gating_context.file_tag in ("sci_incarceration_period",):
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
                if gating_context.file_tag in ("sci_incarceration_period",):
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
                if gating_context.file_tag in ("sci_incarceration_period",):
                    # TODO(#3312): Figure out how to fill out the incarceration_type COUNTY_JAIL/STATE/FEDERAL based on
                    #  IC sentence status + location codes? Ask PA about this!
                    obj.incarceration_type = "SCI"
                elif gating_context.file_tag == "ccis_incarceration_period":
                    obj.incarceration_type = "CCIS"

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
                    obj.conditions = ", ".join(conditions)

    @staticmethod
    def _set_supervising_officer(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the supervision officer (as an Agent entity) on the supervision period."""
        officer_id = row.get("supervising_officer_id")
        if not officer_id:
            # In a large percentage of cases when there is no id attached to an
            # officer, the PO is a placeholder.
            return

        officer_name = row.get("supervising_officer_name", None)

        given_names = None
        surname = None
        full_name = None
        if officer_name:
            match = re.match(AGENT_NAME_AND_ID_REGEX, officer_name)
            if match:
                surname = match.group(1)
                given_names = match.group(2)
            elif full_name_match := re.match(
                FALLBACK_AGENT_NAME_AND_ID_REGEX, officer_name
            ):
                full_name = full_name_match.group(1)
            else:
                full_name = officer_name

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                obj.create_state_agent(
                    state_agent_id=officer_id,
                    full_name=full_name,
                    given_names=given_names,
                    surname=surname,
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
        # TODO(#8972): This row post hook should not be necessary once the supervision
        #  periods view is migrated to v2 ingest mappings.
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
                    obj.create_state_agent(
                        state_agent_id=officer_id,
                        given_names=officer_first_name.upper(),
                        surname=officer_last_name.upper(),
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
        method = row["method"].replace("-", "")
        collateral_type = row["collateral_type"].replace(" ", "").replace("/", "")

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionContact):
                if not collateral_type:
                    obj.location = f"None-{method}"
                else:
                    obj.location = f"{collateral_type}-{method}"


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


def _generate_supervision_contact_primary_key(
    _gating_context: IngestGatingContext, row: Dict[str, str]
) -> IngestFieldCoordinates:
    parole_number = row["parole_number"]
    created_date = row["created_date"]
    duration = row["duration"]
    contact_type = row["contact_type"]

    contact_id = f"{parole_number}-{created_date}-{contact_type}-{duration}"
    return IngestFieldCoordinates(
        "state_supervision_contact", "state_supervision_contact_id", contact_id
    )
