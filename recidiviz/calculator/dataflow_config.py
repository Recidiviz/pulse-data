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
from typing import Dict, List, Type

from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetricType,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    PopulationSpanMetricType,
    SupervisionPopulationSpanMetric,
)
from recidiviz.calculator.pipeline.metrics.program.metrics import (
    ProgramMetricType,
    ProgramParticipationMetric,
    ProgramReferralMetric,
)
from recidiviz.calculator.pipeline.metrics.recidivism.metrics import (
    ReincarcerationRecidivismCountMetric,
    ReincarcerationRecidivismMetricType,
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.calculator.pipeline.metrics.supervision.metrics import (
    SupervisionCaseComplianceMetric,
    SupervisionDowngradeMetric,
    SupervisionMetricType,
    SupervisionOutOfStatePopulationMetric,
    SupervisionPopulationMetric,
    SupervisionStartMetric,
    SupervisionSuccessMetric,
    SupervisionTerminationMetric,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.metrics.violation.metrics import (
    ViolationMetricType,
    ViolationWithResponseMetric,
)

PIPELINE_CONFIG_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "pipeline",
    "calculation_pipeline_templates.yaml",
)

# Pipelines that are always run for all dates.
ALWAYS_UNBOUNDED_DATE_METRICS: List[RecidivizMetricType] = [
    ReincarcerationRecidivismMetricType.REINCARCERATION_COUNT,
    ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
    PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
    PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
]

# The maximum number days of output that should be stored in a dataflow metrics table
# before being moved to cold storage
MAX_DAYS_IN_DATAFLOW_METRICS_TABLE: int = 2

# Where the metrics from outdated Dataflow jobs are stored
DATAFLOW_METRICS_COLD_STORAGE_DATASET: str = "dataflow_metrics_cold_storage"


# A map from the metric class to the name of the table where the output is stored
DATAFLOW_METRICS_TO_TABLES: Dict[Type[RecidivizMetric], str] = {
    # IncarcerationMetrics
    IncarcerationAdmissionMetric: "incarceration_admission_metrics",
    IncarcerationCommitmentFromSupervisionMetric: "incarceration_commitment_from_supervision_metrics",
    IncarcerationPopulationMetric: "incarceration_population_metrics",
    IncarcerationReleaseMetric: "incarceration_release_metrics",
    # PopulationSpanMetrics
    IncarcerationPopulationSpanMetric: "incarceration_population_span_metrics",
    SupervisionPopulationSpanMetric: "supervision_population_span_metrics",
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
    SupervisionStartMetric: "supervision_start_metrics",
    SupervisionSuccessMetric: "supervision_success_metrics",
    SupervisionTerminationMetric: "supervision_termination_metrics",
    # ViolationMetrics
    ViolationWithResponseMetric: "violation_with_response_metrics",
}

# A map from the BigQuery Dataflow metric table to the RecidivizMetricType stored in the table
DATAFLOW_TABLES_TO_METRIC_TYPES: Dict[str, RecidivizMetricType] = {
    # IncarcerationMetrics
    "incarceration_admission_metrics": IncarcerationMetricType.INCARCERATION_ADMISSION,
    "incarceration_commitment_from_supervision_metrics": IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    "incarceration_population_metrics": IncarcerationMetricType.INCARCERATION_POPULATION,
    "incarceration_release_metrics": IncarcerationMetricType.INCARCERATION_RELEASE,
    # PopulationSpanMetrics
    "incarceration_population_span_metrics": PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
    "supervision_population_span_metrics": PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
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
    "supervision_start_metrics": SupervisionMetricType.SUPERVISION_START,
    "supervision_success_metrics": SupervisionMetricType.SUPERVISION_SUCCESS,
    "supervision_termination_metrics": SupervisionMetricType.SUPERVISION_TERMINATION,
    # ViolationMetrics
    "violation_with_response_metrics": ViolationMetricType.VIOLATION,
}

# A list of fields on which to cluster the Dataflow metrics tables
METRIC_CLUSTERING_FIELDS = ["state_code", "year"]
