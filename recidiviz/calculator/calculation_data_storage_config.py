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
"""Calculation data storage configuration."""

# The maximum number days of output that should be stored in a dataflow metrics table before being moved to cold storage
from typing import Dict, Type

from recidiviz.calculator.pipeline.incarceration.metrics import IncarcerationAdmissionMetric, \
    IncarcerationPopulationMetric, IncarcerationReleaseMetric
from recidiviz.calculator.pipeline.program.metrics import ProgramReferralMetric, ProgramParticipationMetric
from recidiviz.calculator.pipeline.recidivism.metrics import ReincarcerationRecidivismCountMetric, \
    ReincarcerationRecidivismRateMetric
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionPopulationMetric, SupervisionRevocationMetric, SupervisionRevocationAnalysisMetric, \
    SupervisionRevocationViolationTypeAnalysisMetric, SupervisionSuccessMetric, \
    SuccessfulSupervisionSentenceDaysServedMetric, SupervisionCaseComplianceMetric, SupervisionTerminationMetric
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric

MAX_DAYS_IN_DATAFLOW_METRICS_TABLE: int = 7

# Where the metrics from outdated Dataflow jobs are stored
DATAFLOW_METRICS_COLD_STORAGE_DATASET: str = 'dataflow_metrics_cold_storage'


# A map from the metric class to the name of the table where the output is stored
DATAFLOW_METRICS_TO_TABLES: Dict[Type[RecidivizMetric], str] = {
    # IncarcerationMetrics
    IncarcerationAdmissionMetric: 'incarceration_admission_metrics',
    IncarcerationPopulationMetric: 'incarceration_population_metrics',
    IncarcerationReleaseMetric: 'incarceration_release_metrics',
    # ProgramMetrics
    ProgramReferralMetric: 'program_referral_metrics',
    ProgramParticipationMetric: 'program_participation_metrics',
    # ReincarcerationRecidivismMetrics
    ReincarcerationRecidivismCountMetric: 'recidivism_count_metrics',
    ReincarcerationRecidivismRateMetric: 'recidivism_rate_metrics',
    # SupervisionMetrics
    SupervisionCaseComplianceMetric: 'supervision_case_compliance_metrics',
    SupervisionPopulationMetric: 'supervision_population_metrics',
    SupervisionRevocationMetric: 'supervision_revocation_metrics',
    SupervisionRevocationAnalysisMetric: 'supervision_revocation_analysis_metrics',
    SupervisionRevocationViolationTypeAnalysisMetric: 'supervision_revocation_violation_type_analysis_metrics',
    SupervisionSuccessMetric: 'supervision_success_metrics',
    SuccessfulSupervisionSentenceDaysServedMetric: 'successful_supervision_sentence_days_served_metrics',
    SupervisionTerminationMetric: 'supervision_termination_metrics'
}
