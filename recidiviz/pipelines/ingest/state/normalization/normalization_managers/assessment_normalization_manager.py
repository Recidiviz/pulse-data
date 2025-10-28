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
"""Contains the logic for a AssessmentNormalizationManager that manages the normalization
of StateAssessment entities in the calculation pipelines."""
import datetime
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)

DEFAULT_ASSESSMENT_SCORE_BUCKET = "NOT_ASSESSED"


class StateSpecificAssessmentNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalization assessments for
    calculations."""

    def set_lsir_assessment_score_bucket(
        self,
        assessment: StateAssessment,
    ) -> Optional[str]:
        """This determines the logic for defining LSIR score buckets, for states
        that use LSIR as the assessment of choice. States may override this logic
        based on interpretations of LSIR scores that are different from the default
        score ranges."""
        assessment_score = assessment.assessment_score
        if assessment_score:
            if assessment_score < 24:
                return "0-23"
            if assessment_score <= 29:
                return "24-29"
            if assessment_score <= 38:
                return "30-38"
            return "39+"
        return None


class AssessmentNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of StateAssessments
    for use in calculations."""

    def __init__(
        self,
        assessments: List[StateAssessment],
        delegate: StateSpecificAssessmentNormalizationDelegate,
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    ) -> None:
        self._assessments = deepcopy(assessments)
        self.delegate = delegate
        self._normalized_assessments_and_additional_attributes: Optional[
            Tuple[List[StateAssessment], AdditionalAttributesMap]
        ] = None
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateAssessment]

    def get_normalized_assessments(self) -> list[NormalizedStateAssessment]:
        (
            processed_assessments,
            additional_attributes,
        ) = self.normalized_assessments_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_assessments, NormalizedStateAssessment, additional_attributes
        )

    def normalized_assessments_and_additional_attributes(
        self,
    ) -> Tuple[List[StateAssessment], AdditionalAttributesMap]:
        """Performs normalization on assessments."""

        if not self._normalized_assessments_and_additional_attributes:
            # Make a deep copy of the original assessments to preprocess
            assessments_for_normalization = deepcopy(self._assessments)
            sorted_assessments = self._sort_assessments(assessments_for_normalization)
            assessment_id_to_score_bucket = self._get_assessment_score_buckets(
                sorted_assessments
            )

            self._normalized_assessments_and_additional_attributes = (
                sorted_assessments,
                self.additional_attributes_map_for_normalized_assessments(
                    assessments_for_normalization, assessment_id_to_score_bucket
                ),
            )

        return self._normalized_assessments_and_additional_attributes

    def _sort_assessments(
        self, assessments: List[StateAssessment]
    ) -> List[StateAssessment]:
        """Sorts assessments by assessment date.

        This key sorts by date and then by  external id. In order to get something that
        behaves like integer sorting and to have consistent sorting between dataflow and
        sessions, we assume longer ids always come after shorter ones."""
        assessments.sort(
            key=lambda a: (
                a.assessment_date or datetime.date.min,
                len(a.external_id) if a and a.external_id else 0,
                a.external_id if a else None,
            )
        )

        return assessments

    def _get_assessment_score_buckets(
        self, assessments: List[StateAssessment]
    ) -> Dict[int, Optional[str]]:
        """Obtains a map of assessment id mapped to assessment score bucket."""
        assessment_id_to_score_bucket: Dict[int, Optional[str]] = {}
        for assessment in assessments:
            if not assessment.assessment_id:
                raise ValueError(
                    "Expected non-null assessment_id values"
                    f"at this point. Found {assessment}."
                )

            assessment_id_to_score_bucket[
                assessment.assessment_id
            ] = self._assessment_score_bucket_for_assessment(assessment)

        return assessment_id_to_score_bucket

    def _assessment_score_bucket_for_assessment(
        self, assessment: StateAssessment
    ) -> str:
        """Calculates the assessment score bucket that applies to measurement based on
        the assessment type. If various fields are not provided, then it falls to the
        default state of not being assessed.

        NOTE: Only LSIR and ORAS buckets are currently supported
        TODO(#2742): Add calculation support for all supported StateAssessmentTypes

        Returns:
            A string representation of the assessment score for the person.
            DEFAULT_ASSESSMENT_SCORE_BUCKET if the assessment type is not supported or
            if the object is missing assessment information."""
        assessment_type = assessment.assessment_type
        assessment_level = assessment.assessment_level

        if assessment_type:
            if assessment_type == StateAssessmentType.LSIR:
                lsir_bucket = self.delegate.set_lsir_assessment_score_bucket(assessment)
                if lsir_bucket:
                    return lsir_bucket
            elif assessment_type in [
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
                StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
                StateAssessmentType.ORAS_PRE_TRIAL,
                StateAssessmentType.ORAS_PRISON_SCREENING,
                StateAssessmentType.ORAS_PRISON_INTAKE,
                StateAssessmentType.ORAS_REENTRY,
                StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY,
                StateAssessmentType.STRONG_R,
                StateAssessmentType.STRONG_R2,
            ]:
                if assessment_level:
                    return assessment_level.value
            elif assessment_type in [
                StateAssessmentType.EXTERNAL_UNKNOWN,
                StateAssessmentType.INTERNAL_UNKNOWN,
                StateAssessmentType.CAF,
                StateAssessmentType.CSRA,
                StateAssessmentType.CSSM,
                StateAssessmentType.CMHS,
                StateAssessmentType.HIQ,
                StateAssessmentType.ICASA,
                StateAssessmentType.J_SOAP,
                StateAssessmentType.LS_RNR,
                StateAssessmentType.ODARA,
                StateAssessmentType.OYAS,
                StateAssessmentType.PA_RST,
                StateAssessmentType.PSA,
                StateAssessmentType.SACA,
                StateAssessmentType.SORAC,
                StateAssessmentType.SOTIPS,
                StateAssessmentType.SPIN_W,
                StateAssessmentType.STABLE,
                StateAssessmentType.STATIC_99,
                StateAssessmentType.TABE,
                StateAssessmentType.TCU_DRUG_SCREEN,
                StateAssessmentType.COMPAS,
                StateAssessmentType.TX_CSST,
                StateAssessmentType.TX_CST,
                StateAssessmentType.TX_RT,
                StateAssessmentType.TX_SRT,
                StateAssessmentType.ACCAT,
                StateAssessmentType.RSLS,
                StateAssessmentType.CCRRA,
                StateAssessmentType.MI_CSJ_353,
                StateAssessmentType.AZ_GEN_RISK_LVL,
                StateAssessmentType.AZ_VLNC_RISK_LVL,
                StateAssessmentType.MO_CLASSIFICATION_E,
                StateAssessmentType.MO_CLASSIFICATION_MH,
                StateAssessmentType.MO_CLASSIFICATION_M,
                StateAssessmentType.MO_CLASSIFICATION_W,
                StateAssessmentType.MO_CLASSIFICATION_V,
                StateAssessmentType.MO_CLASSIFICATION_I,
                StateAssessmentType.MO_CLASSIFICATION_P,
                StateAssessmentType.MO_1270,
                StateAssessmentType.ACUTE,
                StateAssessmentType.MI_SECURITY_CLASS,
                StateAssessmentType.UT_SECURITY_ASSESS,
                StateAssessmentType.IA_CUSTODY_CLASS,
            ]:
                pass
            else:
                raise ValueError(
                    f"Unexpected unsupported StateAssessmentType: {assessment_type}"
                )

        return DEFAULT_ASSESSMENT_SCORE_BUCKET

    def additional_attributes_map_for_normalized_assessments(
        self,
        assessments: List[StateAssessment],
        assessment_id_to_score_bucket: Dict[int, Optional[str]],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of each of
        the StateAssessments for each of the attributes that are unique to the
        NormalizedStateAssessment."""

        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(entities=assessments)
        )

        assessment_additional_attributes_map: Dict[str, Dict[int, Dict[str, Any]]] = {
            StateAssessment.__name__: {}
        }

        for assessment in assessments:
            if not assessment.assessment_id:
                raise ValueError(
                    "Expected non-null assessment_id values"
                    f"at this point. Found {assessment}."
                )

            conducting_staff_id = None
            if assessment.conducting_staff_external_id:
                if not assessment.conducting_staff_external_id_type:
                    # if conducting_staff_external_id is set, a conducting_staff_external_id_type must be set
                    raise ValueError(
                        f"Found no conducting_staff_external_id_type for conducting_staff_external_id "
                        f"{assessment.conducting_staff_external_id} on person {assessment.person}"
                    )
                conducting_staff_id = self.staff_external_id_to_staff_id[
                    (
                        assessment.conducting_staff_external_id,
                        assessment.conducting_staff_external_id_type,
                    )
                ]

            assessment_additional_attributes_map[StateAssessment.__name__][
                assessment.assessment_id
            ] = {
                "assessment_score_bucket": assessment_id_to_score_bucket[
                    assessment.assessment_id
                ],
                "conducting_staff_id": conducting_staff_id,
            }

        return merge_additional_attributes_maps(
            [shared_additional_attributes_map, assessment_additional_attributes_map]
        )
