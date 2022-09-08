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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
import os
from datetime import date
from typing import Dict, List, Optional, Sequence, Set, Type, Union

from recidiviz.calculator.pipeline.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.execution_utils import TableRow
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_recidivism_metrics_producer_delegate import (
    StateSpecificRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_commitment_from_supervision_utils import (
    UsCaCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_incarceration_delegate import (
    UsCaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_incarceration_metrics_producer_delegate import (
    UsCaIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_incarceration_period_normalization_delegate import (
    UsCaIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_program_assignment_normalization_delegate import (
    UsCaProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_recidivism_metrics_producer_delegate import (
    UsCaRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_supervision_delegate import (
    UsCaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_supervision_metrics_producer_delegate import (
    UsCaSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_supervision_period_normalization_delegate import (
    UsCaSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_violation_response_normalization_delegate import (
    UsCaViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ca.us_ca_violations_delegate import (
    UsCaViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_commitment_from_supervision_utils import (
    UsCoCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_incarceration_delegate import (
    UsCoIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_incarceration_metrics_producer_delegate import (
    UsCoIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_incarceration_period_normalization_delegate import (
    UsCoIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_program_assignment_normalization_delegate import (
    UsCoProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_recidivism_metrics_producer_delegate import (
    UsCoRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_supervision_delegate import (
    UsCoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_supervision_metrics_producer_delegate import (
    UsCoSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_supervision_period_normalization_delegate import (
    UsCoSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_violation_response_normalization_delegate import (
    UsCoViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_violations_delegate import (
    UsCoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_commitment_from_supervision_delegate import (
    UsIdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_metrics_producer_delegate import (
    UsIdIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_period_normalization_delegate import (
    UsIdIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_program_assignment_normalization_delegate import (
    UsIdProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_recidivism_metrics_producer_delegate import (
    UsIdRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    UsIdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_metrics_producer_delegate import (
    UsIdSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_period_normalization_delegate import (
    UsIdSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violation_response_normalization_delegate import (
    UsIdViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violations_delegate import (
    UsIdViolationDelegate,
)

# TODO(#10703): Remove this state_code after merging US_IX into US_ID
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_commitment_from_supervision_utils import (
    UsIxCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_incarceration_delegate import (
    UsIxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_incarceration_metrics_producer_delegate import (
    UsIxIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_incarceration_period_normalization_delegate import (
    UsIxIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_program_assignment_normalization_delegate import (
    UsIxProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_recidivism_metrics_producer_delegate import (
    UsIxRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_supervision_metrics_producer_delegate import (
    UsIxSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_supervision_period_normalization_delegate import (
    UsIxSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_violation_response_normalization_delegate import (
    UsIxViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_ix.us_ix_violations_delegate import (
    UsIxViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_commitment_from_supervision_delegate import (
    UsMeCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_incarceration_delegate import (
    UsMeIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_incarceration_metrics_producer_delegate import (
    UsMeIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_incarceration_period_normalization_delegate import (
    UsMeIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_program_assignment_normalization_delegate import (
    UsMeProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_recidivism_metrics_producer_delegate import (
    UsMeRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_delegate import (
    UsMeSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_metrics_producer_delegate import (
    UsMeSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_period_normalization_delegate import (
    UsMeSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_violation_response_normalization_delegate import (
    UsMeViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_violations_delegate import (
    UsMeViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_commitment_from_supervision_delegate import (
    UsMiCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_incarceration_delegate import (
    UsMiIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_incarceration_metrics_producer_delegate import (
    UsMiIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_incarceration_period_normalization_delegate import (
    UsMiIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_program_assignment_normalization_delegate import (
    UsMiProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_recidivism_metrics_producer_delegate import (
    UsMiRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_supervision_delegate import (
    UsMiSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_supervision_metrics_producer_delegate import (
    UsMiSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_supervision_period_normalization_delegate import (
    UsMiSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_violation_response_normalization_delegate import (
    UsMiViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_violations_delegate import (
    UsMiViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_commitment_from_supervision_delegate import (
    UsMoCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_delegate import (
    UsMoIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_metrics_producer_delegate import (
    UsMoIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_period_normalization_delegate import (
    UsMoIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_program_assignment_normalization_delegate import (
    UsMoProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_recidivism_metrics_producer_delegate import (
    UsMoRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_metrics_producer_delegate import (
    UsMoSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_period_normalization_delegate import (
    UsMoSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_response_normalization_delegate import (
    UsMoViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_metrics_producer_delegate import (
    UsNdIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_normalization_delegate import (
    UsNdIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_program_assignment_normalization_delegate import (
    UsNdProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_recidivism_metrics_producer_delegate import (
    UsNdRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_metrics_producer_delegate import (
    UsNdSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_period_normalization_delegate import (
    UsNdSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violation_response_normalization_delegate import (
    UsNdViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_commitment_from_supervision_utils import (
    UsOrCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_incarceration_delegate import (
    UsOrIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_incarceration_metrics_producer_delegate import (
    UsOrIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_incarceration_period_normalization_delegate import (
    UsOrIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_program_assignment_normalization_delegate import (
    UsOrProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_recidivism_metrics_producer_delegate import (
    UsOrRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_supervision_delegate import (
    UsOrSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_supervision_metrics_producer_delegate import (
    UsOrSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_supervision_period_normalization_delegate import (
    UsOrSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_violation_response_normalization_delegate import (
    UsOrViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_or.us_or_violations_delegate import (
    UsOrViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_commitment_from_supervision_utils import (
    UsOzCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_incarceration_delegate import (
    UsOzIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_incarceration_metrics_producer_delegate import (
    UsOzIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_incarceration_period_normalization_delegate import (
    UsOzIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_program_assignment_normalization_delegate import (
    UsOzProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_recidivism_metrics_producer_delegate import (
    UsOzRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_supervision_delegate import (
    UsOzSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_supervision_metrics_producer_delegate import (
    UsOzSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_supervision_period_normalization_delegate import (
    UsOzSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_violation_response_normalization_delegate import (
    UsOzViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_oz.us_oz_violations_delegate import (
    UsOzViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_metrics_producer_delegate import (
    UsPaIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    UsPaIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_program_assignment_normalization_delegate import (
    UsPaProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_recidivism_metrics_producer_delegate import (
    UsPaRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    UsPaSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_metrics_producer_delegate import (
    UsPaSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_period_normalization_delegate import (
    UsPaSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violation_response_normalization_delegate import (
    UsPaViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_commitment_from_supervision_delegate import (
    UsTnCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_delegate import (
    UsTnIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_metrics_producer_delegate import (
    UsTnIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_period_normalization_delegate import (
    UsTnIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_program_assignment_normalization_delegate import (
    UsTnProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_recidivism_metrics_producer_delegate import (
    UsTnRecidivismMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_delegate import (
    UsTnSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_metrics_producer_delegate import (
    UsTnSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_period_normalization_delegate import (
    UsTnSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_violation_response_normalization_delegate import (
    UsTnViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_violations_delegate import (
    UsTnViolationDelegate,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.states import StateCode
from recidiviz.common.file_system import is_non_empty_code_directory
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
)
from recidiviz.utils.types import assert_type


def get_required_state_specific_delegates(
    state_code: str,
    required_delegates: List[Type[StateSpecificDelegate]],
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> Dict[str, StateSpecificDelegate]:
    """Returns a dictionary where the keys are the names of the required delegates
    listed in |required_delegates|, and the values are the state-specific
    implementation of that delegate."""
    required_state_specific_delegates: Dict[str, StateSpecificDelegate] = {}
    for required_delegate in required_delegates:
        if required_delegate is StateSpecificIncarcerationNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_incarceration_period_normalization_delegate(
                state_code
            )
        elif required_delegate is StateSpecificSupervisionNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_supervision_period_normalization_delegate(
                state_code, entity_kwargs
            )
        elif required_delegate is StateSpecificProgramAssignmentNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_program_assignment_normalization_delegate(
                state_code
            )
        elif required_delegate is StateSpecificViolationResponseNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_violation_response_normalization_delegate(
                state_code
            )
        elif required_delegate is StateSpecificCommitmentFromSupervisionDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_commitment_from_supervision_delegate(state_code)
        elif required_delegate is StateSpecificViolationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_violation_delegate(state_code)
        elif required_delegate is StateSpecificIncarcerationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_incarceration_delegate(state_code)
        elif required_delegate is StateSpecificSupervisionDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = get_state_specific_supervision_delegate(state_code, entity_kwargs)
        else:
            raise ValueError(
                f"Unexpected required delegate {required_delegate} for pipeline."
            )
    return required_state_specific_delegates


def get_required_state_specific_metrics_producer_delegates(
    state_code: str,
    required_delegates: Set[Type[StateSpecificMetricsProducerDelegate]],
) -> Dict[str, StateSpecificMetricsProducerDelegate]:
    """Returns the state-specific metrics delegate given the type requested for a given state."""
    required_metric_delegates: Dict[str, StateSpecificMetricsProducerDelegate] = {}
    for required_delegate in required_delegates:
        if required_delegate is StateSpecificIncarcerationMetricsProducerDelegate:
            required_metric_delegates[
                required_delegate.__name__
            ] = _get_state_specific_incarceration_metrics_producer_delegate(state_code)
        if required_delegate is StateSpecificRecidivismMetricsProducerDelegate:
            required_metric_delegates[
                required_delegate.__name__
            ] = _get_state_specific_recidivism_metrics_producer_delegate(state_code)
        if required_delegate is StateSpecificSupervisionMetricsProducerDelegate:
            required_metric_delegates[
                required_delegate.__name__
            ] = _get_state_specific_supervision_metrics_producer_delegate(state_code)

    return required_metric_delegates


def get_state_specific_case_compliance_manager(
    person: StatePerson,
    supervision_period: NormalizedStateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
    incarceration_sentences: List[StateIncarcerationSentence],
    incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionCaseComplianceManager]:
    """Returns a state-specific SupervisionCaseComplianceManager object, containing information about whether the
    given supervision case is in compliance with state-specific standards. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None."""
    state_code = supervision_period.state_code.upper()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
            incarceration_sentences,
            incarceration_period_index,
            supervision_delegate,
        )

    return None


def _get_state_specific_incarceration_period_normalization_delegate(
    state_code: str,
) -> StateSpecificIncarcerationNormalizationDelegate:
    """Returns the type of IncarcerationNormalizationDelegate that should be used for
    normalizing StateIncarcerationPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzIncarcerationNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxIncarcerationNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_supervision_period_normalization_delegate(
    state_code: str,
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> StateSpecificSupervisionNormalizationDelegate:
    """Returns the type of SupervisionNormalizationDelegate that should be used for
    normalizing StateSupervisionPeriod entities from a given |state_code|."""
    assessments = (
        [
            assert_type(a, StateAssessment)
            for a in entity_kwargs[StateAssessment.__name__]
        ]
        if entity_kwargs and entity_kwargs.get(StateAssessment.__name__) is not None
        else None
    )
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        if assessments is None:
            raise ValueError(
                "Missing StateAssessment entity for UsMeSupervisionNormalizationDelegate"
            )
        return UsMeSupervisionNormalizationDelegate(assessments=assessments)
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrSupervisionNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzSupervisionNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_program_assignment_normalization_delegate(
    state_code: str,
) -> StateSpecificProgramAssignmentNormalizationDelegate:
    """Returns the type of ProgramAssignmentNormalizationDelegate that should be used for
    normalizing StateProgramAssignment entities from a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnProgramAssignmentNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzProgramAssignmentNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxProgramAssignmentNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    """Returns the type of StateSpecificCommitmentFromSupervisionDelegate that should be used for
    commitment from supervision admission calculations in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzCommitmentFromSupervisionDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxCommitmentFromSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    """Returns the type of StateSpecificViolationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaViolationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoViolationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdViolationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiViolationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrViolationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnViolationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzViolationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxViolationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_response_normalization_delegate(
    state_code: str,
) -> StateSpecificViolationResponseNormalizationDelegate:
    """Returns the type of StateSpecificViolationResponseNormalizationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzViolationResponseNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxViolationResponseNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    """Returns the type of StateSpecificIncarcerationDelegate that should be used for
    incarceration calculations in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrIncarcerationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnIncarcerationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzIncarcerationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxIncarcerationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


# TODO(#10891): Make this a private method once it's no longer being called from
#  outside of this file
def get_state_specific_supervision_delegate(
    state_code: str,
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> StateSpecificSupervisionDelegate:
    """Returns the type of StateSpecificSupervisionDelegate that should be used for
    supervision calculations in a given |state_code|."""
    # Type is needed to set mypy to respect list of table rows instead of expecting
    # a union of entities or table rows.
    supervision_location_to_names: List[TableRow] = [
        a
        for a in entity_kwargs[SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME]
        if isinstance(a, dict)
    ]
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_OR.value:
        return UsOrSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionDelegate(supervision_location_to_names)
    if state_code == StateCode.US_OZ.value:
        return UsOzSupervisionDelegate(supervision_location_to_names)
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionDelegate(supervision_location_to_names)

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificIncarcerationMetricsProducerDelegate:
    """Returns the type of StateSpecificIncarcerationMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzIncarcerationMetricsProducerDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxIncarcerationMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_supervision_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificSupervisionMetricsProducerDelegate:
    """Returns the type of StateSpecificSupervisionMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzSupervisionMetricsProducerDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_recidivism_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificRecidivismMetricsProducerDelegate:
    """Returns the type of StateSpecificRecidivismMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_CA.value:
        return UsCaRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzRecidivismMetricsProducerDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxRecidivismMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_supported_states() -> Set[StateCode]:
    """Determines which states have directories containing state-specific delegates in
    the state_utils directory."""
    state_utils_path = os.path.dirname(__file__)
    directories = [
        dir_item
        for dir_item in os.listdir(state_utils_path)
        if is_non_empty_code_directory(os.path.join(state_utils_path, dir_item))
    ]
    supported_states: Set[StateCode] = set()

    for directory in directories:
        try:
            state_code = StateCode(directory.upper())
            supported_states.add(state_code)
        except ValueError:
            continue

    if not supported_states:
        raise ValueError(
            "Found zero supported states, which should never happen. If "
            "the location of the state-specific state utils directories "
            "have moved to a new location please update the "
            "state_utils_path."
        )

    return supported_states
