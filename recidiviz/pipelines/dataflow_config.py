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
import datetime
from typing import Dict, List, Type

from recidiviz.pipelines.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetricType,
    IncarcerationReleaseMetric,
)
from recidiviz.pipelines.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    PopulationSpanMetricType,
    SupervisionPopulationSpanMetric,
)
from recidiviz.pipelines.metrics.program.metrics import (
    ProgramMetricType,
    ProgramParticipationMetric,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismMetricType,
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.pipelines.metrics.supervision.metrics import (
    SupervisionCaseComplianceMetric,
    SupervisionMetricType,
    SupervisionOutOfStatePopulationMetric,
    SupervisionPopulationMetric,
    SupervisionStartMetric,
    SupervisionTerminationMetric,
)
from recidiviz.pipelines.metrics.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.pipelines.metrics.violation.metrics import (
    ViolationMetricType,
    ViolationWithResponseMetric,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_content_text_analysis_configuration import (
    UsIxNoteContentTextEntity,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration import (
    UsIxNoteTitleTextEntity,
)

# Pipelines that are always run for all dates.
# TODO(#28513): This should be an abstract property on the metric objects, instead of separately maintained lists
ALWAYS_UNBOUNDED_DATE_METRICS: List[RecidivizMetricType] = [
    ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
    PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
    PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
    IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    IncarcerationMetricType.INCARCERATION_RELEASE,
    ProgramMetricType.PROGRAM_PARTICIPATION,
    SupervisionMetricType.SUPERVISION_COMPLIANCE,
    SupervisionMetricType.SUPERVISION_START,
    SupervisionMetricType.SUPERVISION_TERMINATION,
    ViolationMetricType.VIOLATION,
]

# The maximum number days of output that should be stored in a dataflow metrics table
# before being moved to cold storage
MAX_DAYS_IN_DATAFLOW_METRICS_TABLE: int = 2


# A map from the metric class to the name of the table where the output is stored
# TODO(#28513): This should be an abstract property on the metric objects, instead of separately maintained lists
DATAFLOW_METRICS_TO_TABLES: Dict[Type[RecidivizMetric], str] = {
    # IncarcerationMetrics
    IncarcerationAdmissionMetric: "incarceration_admission_metrics",
    IncarcerationCommitmentFromSupervisionMetric: "incarceration_commitment_from_supervision_metrics",
    IncarcerationReleaseMetric: "incarceration_release_metrics",
    # PopulationSpanMetrics
    IncarcerationPopulationSpanMetric: "incarceration_population_span_metrics",
    SupervisionPopulationSpanMetric: "supervision_population_span_metrics",
    # ProgramMetrics
    ProgramParticipationMetric: "program_participation_metrics",
    # ReincarcerationRecidivismMetrics
    ReincarcerationRecidivismRateMetric: "recidivism_rate_metrics",
    # SupervisionMetrics
    SupervisionCaseComplianceMetric: "supervision_case_compliance_metrics",
    SupervisionOutOfStatePopulationMetric: "supervision_out_of_state_population_metrics",
    SupervisionPopulationMetric: "supervision_population_metrics",
    SupervisionStartMetric: "supervision_start_metrics",
    SupervisionTerminationMetric: "supervision_termination_metrics",
    # ViolationMetrics
    ViolationWithResponseMetric: "violation_with_response_metrics",
}

DATAFLOW_TABLES_TO_METRICS = {
    table_name: metric_class
    for metric_class, table_name in DATAFLOW_METRICS_TO_TABLES.items()
}

# A map from the BigQuery Dataflow metric table to the RecidivizMetricType stored in the table
# TODO(#28513): This should be an abstract property on the metric objects, instead of separately maintained lists
DATAFLOW_TABLES_TO_METRIC_TYPES: Dict[str, RecidivizMetricType] = {
    # IncarcerationMetrics
    "incarceration_admission_metrics": IncarcerationMetricType.INCARCERATION_ADMISSION,
    "incarceration_commitment_from_supervision_metrics": IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    "incarceration_release_metrics": IncarcerationMetricType.INCARCERATION_RELEASE,
    # PopulationSpanMetrics
    "incarceration_population_span_metrics": PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
    "supervision_population_span_metrics": PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
    # ProgramMetrics
    "program_participation_metrics": ProgramMetricType.PROGRAM_PARTICIPATION,
    # ReincarcerationRecidivismMetrics
    "recidivism_rate_metrics": ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
    # SupervisionMetrics
    "supervision_case_compliance_metrics": SupervisionMetricType.SUPERVISION_COMPLIANCE,
    "supervision_out_of_state_population_metrics": SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION,
    "supervision_population_metrics": SupervisionMetricType.SUPERVISION_POPULATION,
    "supervision_start_metrics": SupervisionMetricType.SUPERVISION_START,
    "supervision_termination_metrics": SupervisionMetricType.SUPERVISION_TERMINATION,
    # ViolationMetrics
    "violation_with_response_metrics": ViolationMetricType.VIOLATION,
}

# A list of fields on which to cluster the Dataflow metrics tables
METRIC_CLUSTERING_FIELDS = ["state_code", "year"]


US_IX_CASE_NOTE_MATCHED_ENTITIES_TABLE_NAME = "us_ix_case_note_matched_entities"
US_IX_CASE_NOTE_DEFAULT_ENTITY_MAPPING = {
    entity.name.lower(): False
    for entity in UsIxNoteTitleTextEntity
    if entity != UsIxNoteTitleTextEntity.REVOCATION_INCLUDE
} | {
    entity.name.lower(): False
    for entity in UsIxNoteContentTextEntity
    if entity != UsIxNoteContentTextEntity.REVOCATION_INCLUDE
}

US_ME_SNOOZED_OPPORTUNITIES_TABLE_NAME = "us_me_snoozed_opportunities"

DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS: dict[str, dict[str, Type]] = {
    US_IX_CASE_NOTE_MATCHED_ENTITIES_TABLE_NAME: {
        "person_id": int,
        "person_external_id": str,
        "state_code": str,
        "OffenderNoteId": str,
        "NoteDate": datetime.date,
        "StaffId": str,
        "Details": str,
        **{entity: bool for entity in US_IX_CASE_NOTE_DEFAULT_ENTITY_MAPPING.keys()},
    },
    US_ME_SNOOZED_OPPORTUNITIES_TABLE_NAME: {
        "person_id": int,
        "person_external_id": str,
        "Note_Id": str,
        "Note_Date": datetime.datetime,
        "state_code": str,
        "is_valid_snooze_note": bool,
        "note": str,
        "error": str,
    },
}
