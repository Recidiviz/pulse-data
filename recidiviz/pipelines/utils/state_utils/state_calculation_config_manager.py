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
from datetime import date
from typing import Dict, List, Optional, Sequence, Set, Type, Union

from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StatePerson,
    StateStaffSupervisorPeriod,
    StateSupervisionContact,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    StateSpecificAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StateSpecificStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_recidivism_metrics_producer_delegate import (
    StateSpecificRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_assessment_normalization_delegate import (
    UsArAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_commitment_from_supervision_utils import (
    UsArCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_incarceration_delegate import (
    UsArIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_incarceration_metrics_producer_delegate import (
    UsArIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_incarceration_period_normalization_delegate import (
    UsArIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_recidivism_metrics_producer_delegate import (
    UsArRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_sentence_normalization_delegate import (
    UsArSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_staff_role_period_normalization_delegate import (
    UsArStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_supervision_delegate import (
    UsArSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_supervision_metrics_producer_delegate import (
    UsArSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_supervision_period_normalization_delegate import (
    UsArSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_violation_response_normalization_delegate import (
    UsArViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ar.us_ar_violations_delegate import (
    UsArViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_assessment_normalization_delegate import (
    UsAzAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_commitment_from_supervision_utils import (
    UsAzCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_incarceration_delegate import (
    UsAzIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_incarceration_metrics_producer_delegate import (
    UsAzIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_incarceration_period_normalization_delegate import (
    UsAzIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_recidivism_metrics_producer_delegate import (
    UsAzRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_sentence_normalization_delegate import (
    UsAzSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_staff_role_period_normalization_delegate import (
    UsAzStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_supervision_delegate import (
    UsAzSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_supervision_metrics_producer_delegate import (
    UsAzSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_supervision_period_normalization_delegate import (
    UsAzSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_violation_response_normalization_delegate import (
    UsAzViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_violations_delegate import (
    UsAzViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_assessment_normalization_delegate import (
    UsCaAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_commitment_from_supervision_utils import (
    UsCaCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_incarceration_delegate import (
    UsCaIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_incarceration_metrics_producer_delegate import (
    UsCaIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_incarceration_period_normalization_delegate import (
    UsCaIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_recidivism_metrics_producer_delegate import (
    UsCaRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_sentence_normalization_delegate import (
    UsCaSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_staff_role_period_normalization_delegate import (
    UsCaStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_supervision_delegate import (
    UsCaSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_supervision_metrics_producer_delegate import (
    UsCaSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_supervision_period_normalization_delegate import (
    UsCaSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_violation_response_normalization_delegate import (
    UsCaViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ca.us_ca_violations_delegate import (
    UsCaViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_assessment_normalization_delegate import (
    UsCoAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_commitment_from_supervision_utils import (
    UsCoCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_incarceration_delegate import (
    UsCoIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_incarceration_metrics_producer_delegate import (
    UsCoIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_incarceration_period_normalization_delegate import (
    UsCoIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_recidivism_metrics_producer_delegate import (
    UsCoRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_sentence_normalization_delegate import (
    UsCoSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_staff_role_period_normalization_delegate import (
    UsCoStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_supervision_delegate import (
    UsCoSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_supervision_metrics_producer_delegate import (
    UsCoSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_supervision_period_normalization_delegate import (
    UsCoSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_violation_response_normalization_delegate import (
    UsCoViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_violations_delegate import (
    UsCoViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_assessment_normalization_delegate import (
    UsIaAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_commitment_from_supervision_utils import (
    UsIaCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_incarceration_delegate import (
    UsIaIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_incarceration_metrics_producer_delegate import (
    UsIaIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_incarceration_period_normalization_delegate import (
    UsIaIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_recidivism_metrics_producer_delegate import (
    UsIaRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_sentence_normalization_delegate import (
    UsIaSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_staff_role_period_normalization_delegate import (
    UsIaStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_supervision_delegate import (
    UsIaSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_supervision_metrics_producer_delegate import (
    UsIaSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_supervision_period_normalization_delegate import (
    UsIaSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_violation_response_normalization_delegate import (
    UsIaViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ia.us_ia_violations_delegate import (
    UsIaViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_assessment_normalization_delegate import (
    UsIdAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_commitment_from_supervision_utils import (
    UsIdCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_incarceration_metrics_producer_delegate import (
    UsIdIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_incarceration_period_normalization_delegate import (
    UsIdIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_recidivism_metrics_producer_delegate import (
    UsIdRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_sentence_normalization_delegate import (
    UsIdSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_staff_role_period_normalization_delegate import (
    UsIdStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_supervision_metrics_producer_delegate import (
    UsIdSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_supervision_period_normalization_delegate import (
    UsIdSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_violation_response_normalization_delegate import (
    UsIdViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_id.us_id_violations_delegate import (
    UsIdViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_assessment_normalization_delegate import (
    UsIxAssessmentNormalizationDelegate,
)

# TODO(#10703): Remove this state_code after merging US_IX into US_ID
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_commitment_from_supervision_delegate import (
    UsIxCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_incarceration_delegate import (
    UsIxIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_incarceration_metrics_producer_delegate import (
    UsIxIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_incarceration_period_normalization_delegate import (
    UsIxIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_recidivism_metrics_producer_delegate import (
    UsIxRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_sentence_normalization_delegate import (
    UsIxSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_staff_role_period_normalization_delegate import (
    UsIxStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_compliance import (
    UsIxSupervisionCaseCompliance,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_metrics_producer_delegate import (
    UsIxSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_period_normalization_delegate import (
    UsIxSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_violation_response_normalization_delegate import (
    UsIxViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_violations_delegate import (
    UsIxViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_assessment_normalization_delegate import (
    UsMeAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_commitment_from_supervision_delegate import (
    UsMeCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_incarceration_delegate import (
    UsMeIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_incarceration_metrics_producer_delegate import (
    UsMeIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_incarceration_period_normalization_delegate import (
    UsMeIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_recidivism_metrics_producer_delegate import (
    UsMeRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_sentence_normalization_delegate import (
    UsMeSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_staff_role_period_normalization_delegate import (
    UsMeStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_supervision_delegate import (
    UsMeSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_supervision_metrics_producer_delegate import (
    UsMeSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_supervision_period_normalization_delegate import (
    UsMeSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_violation_response_normalization_delegate import (
    UsMeViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_violations_delegate import (
    UsMeViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_assessment_normalization_delegate import (
    UsMiAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_commitment_from_supervision_delegate import (
    UsMiCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_incarceration_delegate import (
    UsMiIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_incarceration_metrics_producer_delegate import (
    UsMiIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_incarceration_period_normalization_delegate import (
    UsMiIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_recidivism_metrics_producer_delegate import (
    UsMiRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_sentence_normalization_delegate import (
    UsMiSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_staff_role_period_normalization_delegate import (
    UsMiStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_delegate import (
    UsMiSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_metrics_producer_delegate import (
    UsMiSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_period_normalization_delegate import (
    UsMiSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_violation_response_normalization_delegate import (
    UsMiViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_violations_delegate import (
    UsMiViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_assessment_normalization_delegate import (
    UsMoAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_commitment_from_supervision_delegate import (
    UsMoCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_incarceration_delegate import (
    UsMoIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_incarceration_metrics_producer_delegate import (
    UsMoIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_incarceration_period_normalization_delegate import (
    UsMoIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_recidivism_metrics_producer_delegate import (
    UsMoRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_normalization_delegate import (
    UsMoSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_staff_role_period_normalization_delegate import (
    UsMoStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_metrics_producer_delegate import (
    UsMoSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_period_normalization_delegate import (
    UsMoSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_violation_response_normalization_delegate import (
    UsMoViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_assessment_normalization_delegate import (
    UsNcAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_commitment_from_supervision_utils import (
    UsNcCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_incarceration_delegate import (
    UsNcIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_incarceration_metrics_producer_delegate import (
    UsNcIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_incarceration_period_normalization_delegate import (
    UsNcIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_recidivism_metrics_producer_delegate import (
    UsNcRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_sentence_normalization_delegate import (
    UsNcSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_staff_role_period_normalization_delegate import (
    UsNcStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_supervision_delegate import (
    UsNcSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_supervision_metrics_producer_delegate import (
    UsNcSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_supervision_period_normalization_delegate import (
    UsNcSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_violation_response_normalization_delegate import (
    UsNcViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_violations_delegate import (
    UsNcViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_assessment_normalization_delegate import (
    UsNdAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_incarceration_metrics_producer_delegate import (
    UsNdIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_incarceration_period_normalization_delegate import (
    UsNdIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_recidivism_metrics_producer_delegate import (
    UsNdRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_sentence_normalization_delegate import (
    UsNdSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_staff_role_period_normalization_delegate import (
    UsNdStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_supervision_metrics_producer_delegate import (
    UsNdSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_supervision_period_normalization_delegate import (
    UsNdSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_violation_response_normalization_delegate import (
    UsNdViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_assessment_normalization_delegate import (
    UsOrAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_commitment_from_supervision_utils import (
    UsOrCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_incarceration_delegate import (
    UsOrIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_incarceration_metrics_producer_delegate import (
    UsOrIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_incarceration_period_normalization_delegate import (
    UsOrIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_recidivism_metrics_producer_delegate import (
    UsOrRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_sentence_normalization_delegate import (
    UsOrSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_staff_role_period_normalization_delegate import (
    UsOrStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_supervision_delegate import (
    UsOrSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_supervision_metrics_producer_delegate import (
    UsOrSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_supervision_period_normalization_delegate import (
    UsOrSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_violation_response_normalization_delegate import (
    UsOrViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_or.us_or_violations_delegate import (
    UsOrViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_assessment_normalization_delegate import (
    UsOzAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_commitment_from_supervision_utils import (
    UsOzCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_incarceration_delegate import (
    UsOzIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_incarceration_metrics_producer_delegate import (
    UsOzIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_incarceration_period_normalization_delegate import (
    UsOzIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_recidivism_metrics_producer_delegate import (
    UsOzRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_sentence_normalization_delegate import (
    UsOzSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_staff_role_period_normalization_delegate import (
    UsOzStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_supervision_delegate import (
    UsOzSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_supervision_metrics_producer_delegate import (
    UsOzSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_supervision_period_normalization_delegate import (
    UsOzSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_violation_response_normalization_delegate import (
    UsOzViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_violations_delegate import (
    UsOzViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_assessment_normalization_delegate import (
    UsPaAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_incarceration_metrics_producer_delegate import (
    UsPaIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_incarceration_period_normalization_delegate import (
    UsPaIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_recidivism_metrics_producer_delegate import (
    UsPaRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_sentence_normalization_delegate import (
    UsPaSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_staff_role_period_normalization_delegate import (
    UsPaStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_metrics_producer_delegate import (
    UsPaSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_period_normalization_delegate import (
    UsPaSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_violation_response_normalization_delegate import (
    UsPaViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_assessment_normalization_delegate import (
    UsTnAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_commitment_from_supervision_delegate import (
    UsTnCommitmentFromSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_incarceration_delegate import (
    UsTnIncarcerationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_incarceration_metrics_producer_delegate import (
    UsTnIncarcerationMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_incarceration_period_normalization_delegate import (
    UsTnIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_recidivism_metrics_producer_delegate import (
    UsTnRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_sentence_normalization_delegate import (
    UsTnSentenceNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_staff_role_period_normalization_delegate import (
    UsTnStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_supervision_delegate import (
    UsTnSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_supervision_metrics_producer_delegate import (
    UsTnSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_supervision_period_normalization_delegate import (
    UsTnSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_violation_response_normalization_delegate import (
    UsTnViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_violations_delegate import (
    UsTnViolationDelegate,
)
from recidiviz.utils.range_querier import RangeQuerier
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
        if required_delegate is StateSpecificAssessmentNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_assessment_normalization_delegate(
                state_code, entity_kwargs
            )
        elif required_delegate is StateSpecificIncarcerationNormalizationDelegate:
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
        elif required_delegate is StateSpecificSentenceNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_sentence_normalization_delegate(state_code)
        elif required_delegate is StateSpecificStaffRolePeriodNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_staff_role_period_normalization_delegate(
                state_code, entity_kwargs
            )
        elif required_delegate is StateSpecificViolationResponseNormalizationDelegate:
            required_state_specific_delegates[
                required_delegate.__name__
            ] = _get_state_specific_violation_response_normalization_delegate(
                state_code, entity_kwargs
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
            ] = _get_state_specific_supervision_delegate(state_code)
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
    assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
    supervision_contacts_by_date: RangeQuerier[date, StateSupervisionContact],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
    incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionCaseComplianceManager]:
    """Returns a state-specific SupervisionCaseComplianceManager object, containing information about whether the
    given supervision case is in compliance with state-specific standards. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None.
    """
    state_code = supervision_period.state_code.upper()
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments_by_date,
            supervision_contacts_by_date,
            violation_responses,
            incarceration_period_index,
            supervision_delegate,
        )
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments_by_date,
            supervision_contacts_by_date,
            violation_responses,
            incarceration_period_index,
            supervision_delegate,
        )

    return None


def _get_state_specific_assessment_normalization_delegate(
    state_code: str,
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> StateSpecificAssessmentNormalizationDelegate:
    """Returns the type of AssessmentNormalizationDelegate that should be used for
    normalizing StateAssessment entities from a given |state_code|."""
    persons = (
        [assert_type(a, StatePerson) for a in entity_kwargs[StatePerson.__name__]]
        if entity_kwargs and entity_kwargs.get(StatePerson.__name__) is not None
        else None
    )
    if state_code == StateCode.US_AR.value:
        return UsArAssessmentNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaAssessmentNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoAssessmentNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaAssessmentNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeAssessmentNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiAssessmentNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoAssessmentNormalizationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcAssessmentNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdAssessmentNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrAssessmentNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaAssessmentNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnAssessmentNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzAssessmentNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        if persons is None:
            raise ValueError(
                "Missing StatePerson entity for UsIxAssessmentNormalizationDelegate"
            )
        return UsIxAssessmentNormalizationDelegate(persons=persons)
    if state_code == StateCode.US_ID.value:
        return UsIdAssessmentNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzAssessmentNormalizationDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_period_normalization_delegate(
    state_code: str,
) -> StateSpecificIncarcerationNormalizationDelegate:
    """Returns the type of IncarcerationNormalizationDelegate that should be used for
    normalizing StateIncarcerationPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcIncarcerationNormalizationDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzIncarcerationNormalizationDelegate()
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
    incarceration_periods: Optional[List[StateIncarcerationPeriod]] = (
        [
            assert_type(a, StateIncarcerationPeriod)
            for a in entity_kwargs[StateIncarcerationPeriod.__name__]
        ]
        if entity_kwargs
        and entity_kwargs.get(StateIncarcerationPeriod.__name__) is not None
        else None
    )
    if state_code == StateCode.US_AR.value:
        return UsArSupervisionNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        if assessments is None:
            raise ValueError(
                "Missing StateAssessment entity for UsMeSupervisionNormalizationDelegate"
            )
        return UsMeSupervisionNormalizationDelegate(assessments=assessments)
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        if not entity_kwargs or US_MO_SENTENCE_STATUSES_VIEW_NAME not in entity_kwargs:
            raise ValueError(
                "Missing US_MO sentence status view for UsMoSupervisionNormalizationDelegate"
            )
        us_mo_sentence_statuses = [
            a
            for a in entity_kwargs[US_MO_SENTENCE_STATUSES_VIEW_NAME]
            if isinstance(a, dict)
        ]
        return UsMoSupervisionNormalizationDelegate(
            sentence_statuses_list=us_mo_sentence_statuses
        )
    if state_code == StateCode.US_NC.value:
        return UsNcSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrSupervisionNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        if incarceration_periods is None:
            raise ValueError(
                "Missing StateIncarcerationPeriod entity for UsPaSupervisionNormalizationDelegate"
            )
        return UsPaSupervisionNormalizationDelegate(
            incarceration_periods=incarceration_periods
        )
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzSupervisionNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzSupervisionNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_sentence_normalization_delegate(
    state_code: str,
) -> StateSpecificSentenceNormalizationDelegate:
    """Returns the type of SentenceNormalizationDelegate that should be used for normalizing
    StateIncarcerationSentence/StateSupervisionSentence entities from a given |state_code|.
    """
    if state_code == StateCode.US_AR.value:
        return UsArSentenceNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaSentenceNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSentenceNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaSentenceNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSentenceNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiSentenceNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSentenceNormalizationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcSentenceNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSentenceNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrSentenceNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSentenceNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSentenceNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzSentenceNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSentenceNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdSentenceNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzSentenceNormalizationDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_staff_role_period_normalization_delegate(
    state_code: str,
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> StateSpecificStaffRolePeriodNormalizationDelegate:
    """Returns the type of StaffRolePeriodNormalizationDelegate that should be used for normalizing
    StateStaff entities from a given |state_code|.
    """
    staff_supervisor_periods = (
        [
            assert_type(a, StateStaffSupervisorPeriod)
            for a in entity_kwargs[StateStaffSupervisorPeriod.__name__]
        ]
        if entity_kwargs
        and entity_kwargs.get(StateStaffSupervisorPeriod.__name__) is not None
        else None
    )
    if state_code == StateCode.US_AR.value:
        return UsArStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_IX.value:
        return UsIxStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        if staff_supervisor_periods is None:
            raise ValueError(
                "Missing StateStaffSupervisorPeriods for UsNdStaffRolePeriodNormalizationDelegate"
            )
        return UsNdStaffRolePeriodNormalizationDelegate(staff_supervisor_periods)
    if state_code == StateCode.US_OR.value:
        return UsOrStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnStaffRolePeriodNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzStaffRolePeriodNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    """Returns the type of StateSpecificCommitmentFromSupervisionDelegate that should be used for
    commitment from supervision admission calculations in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcCommitmentFromSupervisionDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzCommitmentFromSupervisionDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    """Returns the type of StateSpecificViolationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArViolationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaViolationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoViolationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaViolationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiViolationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcViolationDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdViolationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzViolationDelegate()
    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_violation_response_normalization_delegate(
    state_code: str,
    entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]],
) -> StateSpecificViolationResponseNormalizationDelegate:
    """Returns the type of StateSpecificViolationResponseNormalizationDelegate that should be used for
    violation calculations in a given |state_code|."""
    incarceration_periods = (
        [
            assert_type(a, StateIncarcerationPeriod)
            for a in entity_kwargs[StateIncarcerationPeriod.__name__]
        ]
        if entity_kwargs
        and entity_kwargs.get(StateIncarcerationPeriod.__name__) is not None
        else None
    )
    if state_code == StateCode.US_AR.value:
        return UsArViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_MI.value:
        if incarceration_periods is None:
            raise ValueError(
                "Missing StateIncarcerationPeriod entity for UsMiViolationResponseNormalizationDelegate"
            )
        return UsMiViolationResponseNormalizationDelegate(
            incarceration_periods=incarceration_periods
        )
    if state_code == StateCode.US_MO.value:
        return UsMoViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_TN.value:
        if incarceration_periods is None:
            raise ValueError(
                "Missing StateIncarcerationPeriod entity for UsTnViolationResponseNormalizationDelegate"
            )
        return UsTnViolationResponseNormalizationDelegate(
            incarceration_periods=incarceration_periods
        )
    if state_code == StateCode.US_OZ.value:
        return UsOzViolationResponseNormalizationDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdViolationResponseNormalizationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzViolationResponseNormalizationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    """Returns the type of StateSpecificIncarcerationDelegate that should be used for
    incarceration calculations in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArIncarcerationDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaIncarcerationDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcIncarcerationDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzIncarcerationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_supervision_delegate(
    state_code: str,
) -> StateSpecificSupervisionDelegate:
    """Returns the type of StateSpecificSupervisionDelegate that should be used for
    supervision calculations in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArSupervisionDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaSupervisionDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionDelegate()
    if state_code == StateCode.US_OR.value:
        return UsOrSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionDelegate()
    if state_code == StateCode.US_TN.value:
        return UsTnSupervisionDelegate()
    if state_code == StateCode.US_OZ.value:
        return UsOzSupervisionDelegate()
    # TODO(#10703): Remove this state_code after merging US_IX into US_ID
    if state_code == StateCode.US_IX.value:
        return UsIxSupervisionDelegate()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_incarceration_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificIncarcerationMetricsProducerDelegate:
    """Returns the type of StateSpecificIncarcerationMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcIncarcerationMetricsProducerDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationMetricsProducerDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzIncarcerationMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_supervision_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificSupervisionMetricsProducerDelegate:
    """Returns the type of StateSpecificSupervisionMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcSupervisionMetricsProducerDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionMetricsProducerDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzSupervisionMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def _get_state_specific_recidivism_metrics_producer_delegate(
    state_code: str,
) -> StateSpecificRecidivismMetricsProducerDelegate:
    """Returns the type of StateSpecificRecidivismMetricsProducerDelegate that should be used
    for incarceration metrics in a given |state_code|."""
    if state_code == StateCode.US_AR.value:
        return UsArRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_CA.value:
        return UsCaRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_CO.value:
        return UsCoRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_IA.value:
        return UsIaRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_ME.value:
        return UsMeRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_MI.value:
        return UsMiRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_NC.value:
        return UsNcRecidivismMetricsProducerDelegate()
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
    if state_code == StateCode.US_ID.value:
        return UsIdRecidivismMetricsProducerDelegate()
    if state_code == StateCode.US_AZ.value:
        return UsAzRecidivismMetricsProducerDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")
