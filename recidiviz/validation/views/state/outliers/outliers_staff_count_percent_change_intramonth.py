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
"""A view comparing the most recent archived officer status and supervisor views and the
current officer status and supervisor view by role to determine if major changes
in counts have occurred.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
from recidiviz.validation.views.state.outliers.outliers_staff_count_percent_change_helper import (
    staff_count_percent_change_helper,
)

_VIEW_NAME = "outliers_staff_count_percent_change_intramonth"

_VIEW_DESCRIPTION = """
A view comparing the most recent archived officer status and supervisor views and the
current officer status and supervisor view by role to determine if major changes
in counts have occurred. There should be near-zero % change, if not close to it, 
within a given month because we observe the same period of data for an entire month.

This validation includes special handling on the first of the month and should
pass since we always expect change then. This is because the export on Jan 31 would be 
for the period ending on Jan 1, 20XX, however the export on Feb 1 would be for a new 
period ending on Feb 1, 20XX. So on the first of the month, override the actual counts 
with a dummy value.
"""

_QUERY_TEMPLATE = f"""
{staff_count_percent_change_helper("intramonth")}
"""

OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_INTRAMONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_views_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_INTRAMONTH_VIEW_BUILDER.build_and_print()
