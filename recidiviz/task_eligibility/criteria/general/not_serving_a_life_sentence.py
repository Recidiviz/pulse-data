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
# ============================================================================
"""Describes the spans of time when someone is not serving a life sentence.
"""

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    get_ineligible_offense_type_criteria,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#37445) Rename to specify incarceration

_CRITERIA_NAME = "NOT_SERVING_A_LIFE_SENTENCE"

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    get_ineligible_offense_type_criteria(
        criteria_name=_CRITERIA_NAME,
        compartment_level_1="INCARCERATION",
        description=__doc__,
        where_clause="WHERE sent.is_life",
        additional_json_fields=["LOGICAL_OR(sent.is_life) AS life_sentence"],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
