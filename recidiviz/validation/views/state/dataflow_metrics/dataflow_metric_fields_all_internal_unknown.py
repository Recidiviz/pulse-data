# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A view revealing when metrics have an enum field set completely to INTERNAL_UNKNOWN
that is not to be expected."""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.dataflow_metric_validation_utils import (
    all_enum_fields_across_all_metrics_with_field_value,
    validation_query_for_metric_views_with_all_invalid_fields,
)

VALIDATION_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=f"dataflow_metrics_{field}_all_internal_unknown",
        description=f"Reveals if {field} = 'INTERNAL_UNKNOWN' for every single row in a given Dataflow metric",
        view_query_template=validation_query_for_metric_views_with_all_invalid_fields(
            field_name=field,
            invalid_clause="= 'INTERNAL_UNKNOWN'",
            validation_description=f"Reveals if {field} = 'INTERNAL_UNKNOWN' for every single row in a given Dataflow metric",
        ),
        materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
        should_materialize=True,
    )
    for field in all_enum_fields_across_all_metrics_with_field_value("INTERNAL_UNKNOWN")
]

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in VALIDATION_VIEW_BUILDERS:
            view_builder.build_and_print()
