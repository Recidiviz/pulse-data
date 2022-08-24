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

from enum import Enum
from typing import Callable, Dict, List, Optional, Type

from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.state.external_id_types import US_MO_DOC
from recidiviz.common.constants.state.standard_enum_overrides import (
    legacy_mappings_standard_enum_overrides,
)
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_days
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.state_shared_row_posthooks import (
    IngestGatingContext,
    gen_convert_person_ids_to_external_id_objects,
    gen_set_field_as_concatenated_values_hook,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_constants import (
    CYCLE_ID,
    DOC_ID,
    FIELD_KEY_SEQ,
    MOST_RECENT_SENTENCE_STATUS_DATE,
    PERIOD_CLOSE_CODE,
    PERIOD_CLOSE_CODE_SUBTYPE,
    SENTENCE_KEY_SEQ,
    TAK076_PREFIX,
    TAK291_PREFIX,
    VIOLATION_KEY_SEQ,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_custom_enum_parsers import (
    MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES,
    MID_INCARCERATION_TREATMENT_FAILURE_STATUSES,
    PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_legacy_enum_helpers import (
    incarceration_period_admission_reason_mapper,
    supervising_officer_mapper,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestObject, StateIncarcerationPeriod
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment


# TODO(#8899): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsMoController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for US_MO."""

    PERIOD_SEQUENCE_PRIMARY_COL_PREFIX = "F1"

    PRIMARY_COL_PREFIXES_BY_FILE_TAG = {
        "tak022_tak023_tak025_tak026_offender_sentence_institution": "BS",
        "tak022_tak024_tak025_tak026_offender_sentence_supervision": "BS",
        "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": "BT",
        "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": "BU",
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": "",
        "tak028_tak042_tak076_tak024_violation_reports": "BY",
        "tak291_tak292_tak024_citations": "JT",
    }

    PERIOD_MAGICAL_DATES = ["0", "99999999"]

    # TODO(#2898): Complete transition to TAK026 for IncarcerationPeriod statuses
    ENUM_MAPPER_FUNCTIONS: Dict[Type[Enum], EnumMapperFn] = {
        StateAgentType: supervising_officer_mapper,
        StateIncarcerationPeriodAdmissionReason: incarceration_period_admission_reason_mapper,
    }

    ENUM_IGNORE_PREDICATES: Dict[Type[Enum], EnumIgnorePredicate] = {}

    ENUM_OVERRIDES: Dict[Enum, List[str]] = {
        StateChargeClassificationType.CIVIL: ["L"],  # Local/ordinance
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "BP-FF",  # Board Parole
            "BP-FM",
            "BP-FT",
            "FB-TR",  # Field Administrative
            "CR-FF",  # Conditional Release
            "CR-FT",
            "IT-BD",  # Institutional Release to Supervision
            "IT-BH",
            "IT-BP",
            "IT-CD",
            "IT-CR",
            "IT-EM",
            "IT-IC",
            "IT-IN",
            "IT-RF",
            "IC-IC",  # Institutional Release to Probation
            "RT-BH",  # Board Return
            # Inmate Release to EMP (electronic monitoring program). Only 1
            # occurrence of any of these in the last 10 years.
            "EM-FB",
            "EM-FF",
            "EM-FM",
            "EM-FT",
        ],
        StateIncarcerationPeriodReleaseReason.COURT_ORDER: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "IB-EI",  # Institutional Administrative
            "IB-ER",
            "IB-IB",
            "IB-RB",
            "IB-RR",
            "IB-TR",
            "IB-XX",
            "OR-OR",  # Off Records; Suspension
        ],
        StateIncarcerationPeriodReleaseReason.DEATH: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DE-DE",  # Death
            "DE-XX",
        ],
        StateIncarcerationPeriodReleaseReason.ESCAPE: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "IE-IE",  # Institutional Escape
            "IE-XX",
            "IW-IW",  # Institutional Walkaway
            "IW-XX",
        ],
        StateIncarcerationPeriodReleaseReason.EXECUTION: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DE-EX",  # Execution
        ],
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "XX-XX",  # Unknown (Not Associated)
            "??-??",  # Code Unknown
            "??-XX",
        ],
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN: [
            "CN-FB",  # Committed New Charge- No Vio: seems erroneous
            "CN-NV",
            "RV-FF",  # Revoked: seems erroneous
            "RV-FM",
            "RV-FT",
        ],
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: [
            # These statuses indicate an end to a period of temporary hold since
            # it has now been determined that the person has had their parole
            # revoked or has been given some other sanction. With the exception of a few
            # rare cases, these statuses are always closing a board hold period when
            # seen as an end code.
            *PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES,
            *MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES,
        ],
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE: [
            # These statuses indicate a failure of treatment causing mandate to serve
            # rest of sentence
            *MID_INCARCERATION_TREATMENT_FAILURE_STATUSES,
        ],
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DC-DC",  # Discharge
            "DC-DO",  # Inst. Converted-Inactive
            "DC-XX",  # Discharge - Unknown
            "ID-DC",  # Institutional Discharge - Discharge
            "ID-DO",  # Institutional Discharge - Other
            "ID-DR",  # Institutional Discharge - Director's Release
            "ID-ID",  # Institutional Discharge - Institutional Discharge
            "ID-PD",  # Institutional Discharge - Pardoned
            "ID-RR",  # Institutional Discharge - Reversed and Remanded
            "ID-XX",  # Institutional Discharge - Unknown
        ],
        StateSpecializedPurposeForIncarceration.GENERAL: [
            "S",  # Serving Sentence
        ],
        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION: [
            "O",  # 120-Day Shock
            "R",  # Regimented Disc Program
        ],
        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON: [
            "A",  # Assessment
            "I",  # Inst Treatment Center
            "L",  # Long Term Drug Treatment
        ],
    }

    ENUM_IGNORES: Dict[Type[Enum], List[str]] = {
        StateSpecializedPurposeForIncarceration: [
            "X",  # Unknown
        ],
    }

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MO.value.lower()

    def __init__(self, ingest_instance: DirectIngestInstance):
        super().__init__(ingest_instance)

        self.enum_overrides = self.generate_enum_overrides()
        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {}

        incarceration_period_row_posthooks: List[IngestRowPosthookCallable] = [
            self._replace_invalid_release_date,
            self._gen_clear_magical_date_value(
                "release_date", self.PERIOD_MAGICAL_DATES, StateIncarcerationPeriod
            ),
            gen_set_field_as_concatenated_values_hook(
                StateIncarcerationPeriod,
                "release_reason",
                [PERIOD_CLOSE_CODE, PERIOD_CLOSE_CODE_SUBTYPE],
            ),
        ]

        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            # SQL Preprocessing View
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": incarceration_period_row_posthooks,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": incarceration_period_row_posthooks,
        }

        self.primary_key_override_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            # SQL Preprocessing View
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": self._generate_incarceration_period_id_coords,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": self._generate_incarceration_period_id_coords,
        }

        self.ancestor_chain_override_by_file: Dict[
            str, IngestAncestorChainOverridesCallable
        ] = {
            # SQL Preprocessing View
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": self._incarceration_sentence_ancestor_chain_override,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": self._supervision_sentence_ancestor_chain_override,
        }

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """
        return (
            [
                # SQL Preprocessing View
                "tak001_offender_identification",
                "oras_assessments_weekly_v2",
                "tak022_tak023_tak025_tak026_offender_sentence_institution",
                "tak022_tak024_tak025_tak026_offender_sentence_supervision",
            ]
            + (
                ["tak158_tak026_incarceration_periods"]
                if not environment.in_gcp_production()
                and self.ingest_instance == DirectIngestInstance.SECONDARY
                else [
                    "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
                    "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
                ]
            )
            + [
                "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
                "tak028_tak042_tak076_tak024_violation_reports",
                "tak291_tak292_tak024_citations",
            ]
        )

    # TODO(#8899): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_row_pre_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_pre_processors_by_file.get(file_tag, [])

    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(self, _file_tag: str) -> List[Callable]:
        post_processors: List[Callable] = [
            gen_convert_person_ids_to_external_id_objects(self._get_id_type),
        ]
        return post_processors

    def get_primary_key_override_for_file(
        self, file_tag: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_by_file.get(file_tag, None)

    def get_ancestor_chain_overrides_callback_for_file(
        self, file_tag: str
    ) -> Optional[Callable]:
        return self.ancestor_chain_override_by_file.get(file_tag, None)

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []

    @classmethod
    def generate_enum_overrides(cls) -> EnumOverrides:
        """Provides Missouri-specific overrides for enum mappings."""
        base_overrides = legacy_mappings_standard_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            cls.ENUM_OVERRIDES,
            cls.ENUM_IGNORES,
            cls.ENUM_MAPPER_FUNCTIONS,
            cls.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
            # SQL Preprocessing View
            "oras_assessments_weekly_v2",
            "tak022_tak023_tak025_tak026_offender_sentence_institution",
            "tak022_tak024_tak025_tak026_offender_sentence_supervision",
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
            "tak028_tak042_tak076_tak024_violation_reports",
            "tak291_tak292_tak024_citations",
        ]:
            return US_MO_DOC

        return None

    # TODO(#2701): Remove posthook in place of general child-id setting solution.

    @classmethod
    def _replace_invalid_release_date(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Replaces a 99999999 release date with the most recent status update date."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date == "99999999":
                    obj.release_date = row.get(MOST_RECENT_SENTENCE_STATUS_DATE, None)

    @classmethod
    def _gen_clear_magical_date_value(
        cls,
        field_name: str,
        magical_dates: List[str],
        sentence_type: Type[IngestObject],
    ) -> IngestRowPosthookCallable:
        def _clear_magical_date_values(
            _gating_context: IngestGatingContext,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache,
        ) -> None:
            for obj in extracted_objects:
                if isinstance(obj, sentence_type):
                    # pylint: disable=unnecessary-dunder-call
                    if obj.__getattribute__(field_name) in magical_dates:
                        obj.__setattr__(field_name, None)

        return _clear_magical_date_values

    def _revocation_admission_reason(
        self, ip_admission_reason: Optional[str]
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if not ip_admission_reason:
            return None

        ip_admission_reason_enum = self.get_enum_overrides().parse(
            ip_admission_reason, StateIncarcerationPeriodAdmissionReason
        )

        if ip_admission_reason_enum is None:
            return None

        if not isinstance(
            ip_admission_reason_enum, StateIncarcerationPeriodAdmissionReason
        ):
            raise ValueError(
                f"Unexpected enum type returned: [{ip_admission_reason_enum}]"
            )

        if (
            ip_admission_reason_enum
            == StateIncarcerationPeriodAdmissionReason.REVOCATION
        ):
            return ip_admission_reason_enum
        return None

    @classmethod
    def _incarceration_sentence_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        sentence_coords = cls._generate_incarceration_sentence_id_coords(
            gating_context, row
        )

        return {
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_sentence_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        sentence_coords = cls._generate_supervision_sentence_id_coords(
            gating_context, row
        )

        return {
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_violation_report_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        if row.get(f"{TAK076_PREFIX}_{FIELD_KEY_SEQ}", "0") == "0":
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                gating_context, row, TAK076_PREFIX
            )
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                gating_context, row, TAK076_PREFIX
            )
        return {
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_violation_citation_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        if row.get(f"{TAK291_PREFIX}_{FIELD_KEY_SEQ}", "0") == "0":
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                gating_context, row, TAK291_PREFIX
            )
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                gating_context, row, TAK291_PREFIX
            )
        return {
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _generate_supervision_sentence_id_coords(
        cls,
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        col_prefix: str = None,
    ) -> IngestFieldCoordinates:
        if not col_prefix:
            col_prefix = cls.primary_col_prefix_for_file_tag(
                gating_context.ingest_view_name
            )
        return IngestFieldCoordinates(
            "state_supervision_sentence",
            "state_supervision_sentence_id",
            cls._generate_sentence_id(col_prefix, row),
        )

    @classmethod
    def _generate_incarceration_sentence_id_coords(
        cls,
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        col_prefix: str = None,
    ) -> IngestFieldCoordinates:
        if not col_prefix:
            col_prefix = cls.primary_col_prefix_for_file_tag(
                gating_context.ingest_view_name
            )
        return IngestFieldCoordinates(
            "state_incarceration_sentence",
            "state_incarceration_sentence_id",
            cls._generate_sentence_id(col_prefix, row),
        )

    @classmethod
    def _generate_incarceration_period_id_coords(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(
            gating_context.ingest_view_name
        )

        doc_cycle_id = cls._generate_doc_cycle_id(col_prefix, row)

        # TODO(#2728): The SQN is potentially not a stable ID if status
        #  information gets backdated and the SQN numbers generated in the
        #  roll-up shift.
        subcycle_seq_num = row[f"{cls.PERIOD_SEQUENCE_PRIMARY_COL_PREFIX}_SQN"]
        start_status_seq_num = row["START_STATUS_SEQ_NUM"]
        incarceration_period_id = (
            f"{doc_cycle_id}-{subcycle_seq_num}-{start_status_seq_num}"
        )

        return IngestFieldCoordinates(
            "state_incarceration_period",
            "state_incarceration_period_id",
            incarceration_period_id,
        )

    @classmethod
    def _generate_sentence_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        doc_cycle_id = cls._generate_doc_cycle_id(col_prefix, row)
        sen_seq_num = row[f"{col_prefix}_{SENTENCE_KEY_SEQ}"]
        return f"{doc_cycle_id}-{sen_seq_num}"

    @classmethod
    def _generate_doc_cycle_id(cls, col_prefix: str, row: Dict[str, str]) -> str:

        if col_prefix:
            doc_id = row[f"{col_prefix}_{DOC_ID}"]
            cyc_id = row[f"{col_prefix}_{CYCLE_ID}"]
        else:
            doc_id = row[DOC_ID]
            cyc_id = row[CYCLE_ID]
        return f"{doc_id}-{cyc_id}"

    @classmethod
    def _generate_supervision_violation_id(
        cls, col_prefix: str, row: Dict[str, str], violation_id_prefix: str
    ) -> str:
        group_id = cls._generate_doc_cycle_id(TAK076_PREFIX, row)
        sentence_seq_id = row[f"{TAK076_PREFIX}_{SENTENCE_KEY_SEQ}"]
        violation_seq_num = row[f"{col_prefix}_{VIOLATION_KEY_SEQ}"]
        return f"{group_id}-{violation_id_prefix}{violation_seq_num}-{sentence_seq_id}"

    @classmethod
    def primary_col_prefix_for_file_tag(cls, file_tag: str) -> str:
        return cls.PRIMARY_COL_PREFIXES_BY_FILE_TAG[file_tag]

    @classmethod
    def _test_length_string(cls, time_string: str) -> bool:
        """Tests the length string to see if it will cause an overflow beyond the Python MAXYEAR."""
        try:
            parse_days(time_string)
            return True
        except ValueError:
            return False
