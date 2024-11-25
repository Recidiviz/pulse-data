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
"""A view surfacing rows in experiment assignments table where the unit_type does not correspond
with a configured MetricUnitOfAnalysisType.

To build, run:
    python -m recidiviz.validation.views.state.experiment_assignemnts_unit_of_analysis_validation
"""

from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

_VIEW_NAME = "experiment_assignments_unit_of_analysis_validation"

_DESCRIPTION = """Rows in experiment assignments table where the unit_type does not correspond
with a configured MetricUnitOfAnalysisType."""

_QUERY_TEMPLATE = f"""
SELECT 
    *, state_code as region_code
FROM
    `{{project_id}}.experiments_metadata.experiment_assignments_materialized`
WHERE
    unit_type NOT IN ({list_to_query_string([e.value for e in MetricUnitOfAnalysisType], quoted=True)})
"""

EXPERIMENT_ASSIGNMENTS_UNIT_OF_ANALYSIS_VALIDATION_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EXPERIMENT_ASSIGNMENTS_UNIT_OF_ANALYSIS_VALIDATION_VIEW_BUILDER.build_and_print()
