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
"""Reincarcerations by month."""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.supervision_transition_template import (
    supervision_transition_template,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME = "supervision_to_prison_count_by_month"

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION = (
    """Admissions to prison from supervision by month."""
)

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE = supervision_transition_template(
    "prison",
)

SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "supervision_type", "gender"),
    description=SUPERVISION_TO_PRISON_COUNT_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER.build_and_print()
