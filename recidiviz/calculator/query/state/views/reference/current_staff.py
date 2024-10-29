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
"""View containing attributes of current staff members in a state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CURRENT_STAFF_VIEW_NAME = "current_staff"

CURRENT_STAFF_DESCRIPTION = (
    """View containing attributes of current staff members in a state."""
)

CURRENT_STAFF_QUERY_TEMPLATE = f"""
    SELECT *
    FROM `{{project_id}}.reference_views.product_staff_materialized` os
    WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date_exclusive")}
"""


CURRENT_STAFF_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=CURRENT_STAFF_VIEW_NAME,
    view_query_template=CURRENT_STAFF_QUERY_TEMPLATE,
    description=CURRENT_STAFF_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_STAFF_VIEW_BUILDER.build_and_print()
