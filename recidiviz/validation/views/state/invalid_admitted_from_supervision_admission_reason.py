# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view revealing when metrics have an admission_reason value of
ADMITTED_FROM_SUPERVISION. This is an ingest-only enum and should not be observed in
any metrics.

Existence of any rows indicates a bug in IP pre-processing logic.
"""
import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.common.attr_utils import get_enum_cls
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views import dataset_config

INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_NAME = (
    "invalid_admitted_from_supervision_admission_reason"
)

INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_DESCRIPTION = """Metrics with
invalid, ingest-only admission_reason values of ADMITTED_FROM_SUPERVISION."""


SELECT_FROM_METRICS_TEMPLATE = (
    "(SELECT state_code AS region_code, person_id, metric_type, job_id FROM "
    "`{{project_id}}.{{materialized_metrics_dataset}}.most_recent_{metric_view_name}_materialized` "
    "WHERE admission_reason = 'ADMITTED_FROM_SUPERVISION')"
)


def _invalid_admitted_from_supervision_admissions_view_builder() -> str:
    applicable_tables = [
        StrictStringFormatter().format(
            SELECT_FROM_METRICS_TEMPLATE, metric_view_name=metric_view_name
        )
        for metric, table in DATAFLOW_METRICS_TO_TABLES.items()
        if (field := attr.fields_dict(metric).get("admission_reason")) is not None
        and get_enum_cls(field) == StateIncarcerationPeriodAdmissionReason
        for metric_view_name in generate_metric_view_names(table)
    ]

    return "\n UNION ALL \n".join(applicable_tables)


INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_NAME,
    view_query_template=_invalid_admitted_from_supervision_admissions_view_builder(),
    description=INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER.build_and_print()
