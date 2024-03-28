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

_VIEW_NAME = "outliers_staff_count_percent_change_intermonth"

_VIEW_DESCRIPTION = """
A view comparing the most recent archived officer status and supervisor views and the
current officer status and supervisor view by role to determine if major changes
in counts have occurred. 

This validation ensures that the change in staff count is within a given threshold
when the observed period in the Insights products changes at the turn of the month. 
Thus, this is only meaningful on the first of the month. For intramonth validations, see 
recidiviz/validation/views/state/outliers/outliers_staff_count_percent_change_intramonth.py
"""

_QUERY_TEMPLATE = f"""
{staff_count_percent_change_helper("intermonth")}
"""

OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_INTERMONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_views_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OUTLIERS_STAFF_COUNT_PERCENT_CHANGE_INTERMONTH_VIEW_BUILDER.build_and_print()
