# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Constants related to a StateDrugScreen."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateDrugScreenResult(StateEntityEnum):
    """Enum indicating whether the test result was positive, negative or other."""

    POSITIVE = state_enum_strings.state_drug_screen_result_positive
    NEGATIVE = state_enum_strings.state_drug_screen_result_negative
    ADMITTED_POSITIVE = state_enum_strings.state_drug_screen_result_admitted_positive
    MEDICAL_EXEMPTION = state_enum_strings.state_drug_screen_result_medical_exemption
    NO_RESULT = state_enum_strings.state_drug_screen_result_no_result
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateDrugScreenResult"]:
        return _STATE_DRUG_SCREEN_RESULT_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "Describes a person's drug screen result for a given date."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_DRUG_SCREEN_RESULTS_VALUE_DESCRIPTIONS


_STATE_DRUG_SCREEN_RESULTS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateDrugScreenResult.POSITIVE: (
        "Drug screen returned a positive result based on collected sample."
    ),
    StateDrugScreenResult.NEGATIVE: (
        "Drug screen returned a negative result based on collected sample."
    ),
    StateDrugScreenResult.ADMITTED_POSITIVE: (
        "Drug screen was recorded as positive based on admission of substance use, "
        "but not based on the collected sample."
    ),
    StateDrugScreenResult.MEDICAL_EXEMPTION: (
        "Drug screen was recorded as negative due to a medical condition or "
        "valid prescription, regardless of result from the collected sample."
    ),
    StateDrugScreenResult.NO_RESULT: ("No result was recorded for a drug screen."),
}


_STATE_DRUG_SCREEN_RESULT_MAP = {
    "POSITIVE": StateDrugScreenResult.POSITIVE,
    "NEGATIVE": StateDrugScreenResult.NEGATIVE,
    "ADMITTED POSITIVE": StateDrugScreenResult.ADMITTED_POSITIVE,
    "MEDICAL EXEMPTION": StateDrugScreenResult.MEDICAL_EXEMPTION,
    "NO RESULT": StateDrugScreenResult.NO_RESULT,
    "EXTERNAL UNKNOWN": StateDrugScreenResult.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateDrugScreenResult.INTERNAL_UNKNOWN,
}

# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateDrugScreenSampleType(StateEntityEnum):
    """Type of sample collected for drug screen."""

    URINE = state_enum_strings.state_drug_screen_sample_type_urine
    SWEAT = state_enum_strings.state_drug_screen_sample_type_sweat
    SALIVA = state_enum_strings.state_drug_screen_sample_type_saliva
    BLOOD = state_enum_strings.state_drug_screen_sample_type_blood
    HAIR = state_enum_strings.state_drug_screen_sample_type_hair
    BREATH = state_enum_strings.state_drug_screen_sample_type_breath
    NO_SAMPLE = state_enum_strings.state_drug_screen_sample_type_no_sample
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateDrugScreenSampleType"]:
        return _STATE_DRUG_SCREEN_SAMPLE_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "Describes a person's drug screen sample type for a given drug screen."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_DRUG_SCREEN_SAMPLE_TYPE_VALUE_DESCRIPTIONS


_STATE_DRUG_SCREEN_SAMPLE_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateDrugScreenSampleType.URINE: ("Urine."),
    StateDrugScreenSampleType.SWEAT: ("Sweat."),
    StateDrugScreenSampleType.SALIVA: ("Saliva."),
    StateDrugScreenSampleType.BLOOD: ("Blood."),
    StateDrugScreenSampleType.HAIR: ("Hair."),
    StateDrugScreenSampleType.BREATH: ("Breath."),
    StateDrugScreenSampleType.NO_SAMPLE: ("No sample was collected for a drug screen."),
}


_STATE_DRUG_SCREEN_SAMPLE_TYPE_MAP = {
    "URINE": StateDrugScreenSampleType.URINE,
    "SWEAT": StateDrugScreenSampleType.SWEAT,
    "SALIVA": StateDrugScreenSampleType.SALIVA,
    "BLOOD": StateDrugScreenSampleType.BLOOD,
    "HAIR": StateDrugScreenSampleType.HAIR,
    "BREATH": StateDrugScreenSampleType.BREATH,
    "NO SAMPLE": StateDrugScreenSampleType.NO_SAMPLE,
    "EXTERNAL UNKNOWN": StateDrugScreenSampleType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateDrugScreenSampleType.INTERNAL_UNKNOWN,
}
