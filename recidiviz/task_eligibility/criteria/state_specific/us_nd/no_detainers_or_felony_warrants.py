# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

"""
Shows the spans of time during which someone in ND has no detainers or felony warrants.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    get_warrants_and_detainers_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ND_NO_DETAINERS_OR_FELONY_WARRANTS"

ORDER_TYPES = [
    "WARF",  # Warrant - Felony
    "DET",  # Detainer
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    get_warrants_and_detainers_query(
        order_types=ORDER_TYPES, criteria_name=_CRITERIA_NAME, description=__doc__
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
