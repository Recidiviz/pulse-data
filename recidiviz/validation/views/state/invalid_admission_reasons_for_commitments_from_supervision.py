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
"""A view revealing when commitment from supervision admission metrics have an
invalid admission_reason.

Existence of any rows indicates a bug in the incarceration identifier logic that
classifies admissions as commitments from supervision.
"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationCommitmentFromSupervisionMetric,
)
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    is_commitment_from_supervision,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.dataflow_metric_validation_utils import (
    validation_query_for_metric,
)

INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_NAME = (
    "invalid_admission_reasons_for_commitments_from_supervision"
)

INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_DESCRIPTION = """
Incarceration commitment from supervision admission metrics with invalid admission 
reasons."""


def _valid_admission_reasons_for_commitments_from_supervision() -> str:
    # All commitment from supervision admissions should have one of the following
    # admission reasons
    valid_admission_reason_values: List[str] = [
        f"'{admission_reason.value}'"
        for admission_reason in StateIncarcerationPeriodAdmissionReason
        if is_commitment_from_supervision(
            admission_reason, allow_ingest_only_enum_values=True
        )
    ]

    return ", ".join(valid_admission_reason_values)


INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_NAME,
    view_query_template=validation_query_for_metric(
        metric=IncarcerationCommitmentFromSupervisionMetric,
        additional_columns_to_select=[
            "person_id",
            "metric_type",
            "admission_reason",
            "admission_date",
            "job_id",
        ],
        invalid_rows_filter_clause=f"WHERE admission_reason not in ({_valid_admission_reasons_for_commitments_from_supervision()})",
        validation_description=INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_DESCRIPTION,
    ),
    description=INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER.build_and_print()
