# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Dataset configuration for task eligibility views"""

# Dataset with views that union together all task eligibility spans,
# component criteria, and populations.
from recidiviz.common.constants.states import StateCode

TASK_ELIGIBILITY_DATASET_ID = "task_eligibility"


def task_eligibility_spans_state_specific_dataset(state_code: StateCode) -> str:
    """Returns the dataset containing task eligibility spans for this region."""
    return f"task_eligibility_spans_{state_code.value.lower()}"
