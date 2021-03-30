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
"""Config for Dataflow pipelines and BigQuery storage of metric output."""
import os
from typing import Dict, Type

from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
    IncarcerationMetricType,
)
from recidiviz.calculator.pipeline.program.metrics import (
    ProgramReferralMetric,
    ProgramParticipationMetric,
    ProgramMetricType,
)
from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismCountMetric,
    ReincarcerationRecidivismRateMetric,
    ReincarcerationRecidivismMetricType,
)
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionPopulationMetric,
    SupervisionRevocationMetric,
    SupervisionSuccessMetric,
    SuccessfulSupervisionSentenceDaysServedMetric,
    SupervisionCaseComplianceMetric,
    SupervisionTerminationMetric,
    SupervisionStartMetric,
    SupervisionOutOfStatePopulationMetric,
    SupervisionDowngradeMetric,
    SupervisionMetricType,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)

STAGING_ONLY_TEMPLATES_PATH = os.path.join(
    os.path.dirname(__file__),
    "pipeline",
    "staging_only_calculation_pipeline_templates.yaml",
)

PRODUCTION_TEMPLATES_PATH = os.path.join(
    os.path.dirname(__file__),
    "pipeline",
    "production_calculation_pipeline_templates.yaml",
)

# Pipelines that are always run for all dates.
ALWAYS_UNBOUNDED_DATE_PIPELINES = ["recidivism"]

# The maximum number days of output that should be stored in a dataflow metrics table before being moved to cold storage
MAX_DAYS_IN_DATAFLOW_METRICS_TABLE: int = 7

# Where the metrics from outdated Dataflow jobs are stored
DATAFLOW_METRICS_COLD_STORAGE_DATASET: str = "dataflow_metrics_cold_storage"


# A map from the metric class to the name of the table where the output is stored
DATAFLOW_METRICS_TO_TABLES: Dict[Type[RecidivizMetric], str] = {
    # IncarcerationMetrics
    IncarcerationAdmissionMetric: "incarceration_admission_metrics",
    IncarcerationPopulationMetric: "incarceration_population_metrics",
    IncarcerationReleaseMetric: "incarceration_release_metrics",
    # ProgramMetrics
    ProgramReferralMetric: "program_referral_metrics",
    ProgramParticipationMetric: "program_participation_metrics",
    # ReincarcerationRecidivismMetrics
    ReincarcerationRecidivismCountMetric: "recidivism_count_metrics",
    ReincarcerationRecidivismRateMetric: "recidivism_rate_metrics",
    # SupervisionMetrics
    SupervisionCaseComplianceMetric: "supervision_case_compliance_metrics",
    SupervisionDowngradeMetric: "supervision_downgrade_metrics",
    SupervisionOutOfStatePopulationMetric: "supervision_out_of_state_population_metrics",
    SupervisionPopulationMetric: "supervision_population_metrics",
    SupervisionRevocationMetric: "supervision_revocation_metrics",
    SupervisionStartMetric: "supervision_start_metrics",
    SupervisionSuccessMetric: "supervision_success_metrics",
    SuccessfulSupervisionSentenceDaysServedMetric: "successful_supervision_sentence_days_served_metrics",
    SupervisionTerminationMetric: "supervision_termination_metrics",
}

# A map from the BigQuery Dataflow metric table to the RecidivizMetricType stored in the table
DATAFLOW_TABLES_TO_METRIC_TYPES: Dict[str, RecidivizMetricType] = {
    # IncarcerationMetrics
    "incarceration_admission_metrics": IncarcerationMetricType.INCARCERATION_ADMISSION,
    "incarceration_population_metrics": IncarcerationMetricType.INCARCERATION_POPULATION,
    "incarceration_release_metrics": IncarcerationMetricType.INCARCERATION_RELEASE,
    # ProgramMetrics
    "program_referral_metrics": ProgramMetricType.PROGRAM_REFERRAL,
    "program_participation_metrics": ProgramMetricType.PROGRAM_PARTICIPATION,
    # ReincarcerationRecidivismMetrics
    "recidivism_count_metrics": ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT,
    "recidivism_rate_metrics": ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
    # SupervisionMetrics
    "supervision_case_compliance_metrics": SupervisionMetricType.SUPERVISION_COMPLIANCE,
    "supervision_downgrade_metrics": SupervisionMetricType.SUPERVISION_DOWNGRADE,
    "supervision_out_of_state_population_metrics": SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
    "supervision_population_metrics": SupervisionMetricType.SUPERVISION_POPULATION,
    "supervision_revocation_metrics": SupervisionMetricType.SUPERVISION_REVOCATION,
    "supervision_start_metrics": SupervisionMetricType.SUPERVISION_START,
    "supervision_success_metrics": SupervisionMetricType.SUPERVISION_SUCCESS,
    "successful_supervision_sentence_days_served_metrics": SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED,
    "supervision_termination_metrics": SupervisionMetricType.SUPERVISION_TERMINATION,
}
