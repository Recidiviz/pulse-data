# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""Defines a criterion that shows spans of time when incarcerated clients
have been incarcerated in state prison for at least 1 year.
"""

from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_minimum_time_served_criteria_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDER = get_minimum_time_served_criteria_query(
    criteria_name="INCARCERATED_IN_STATE_PRISON_AT_LEAST_1_YEAR",
    description=__doc__,
    minimum_time_served=1,
    time_served_interval="YEAR",
    custodial_authority_types=["STATE_PRISON"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
