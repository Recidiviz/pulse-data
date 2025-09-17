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

from recidiviz.common.constants.states import StateCode

# Dataset with views that union together all task eligibility spans,
# component criteria, and populations.
TASK_ELIGIBILITY_DATASET_ID = "task_eligibility"

TASK_COMPLETION_EVENTS_DATASET_ID = "task_eligibility_completion_events_general"

# Where general TES criteria live
TASK_ELIGIBILITY_CRITERIA_GENERAL = "task_eligibility_criteria_general"

# Dataset with views that union together all task eligibility spans,
# component criteria, and populations with information about compliance tasks
COMPLIANCE_TASK_ELIGIBILITY_SPANS_DATASET_ID = "compliance_task_eligibility_spans"


def task_eligibility_spans_state_specific_dataset(state_code: StateCode) -> str:
    """Returns the dataset containing task eligibility spans for this region."""
    return f"task_eligibility_spans_{state_code.value.lower()}"


def task_eligibility_criteria_state_specific_dataset(state_code: StateCode) -> str:
    """Returns the dataset containing task eligibility spans for this region."""
    return f"task_eligibility_criteria_{state_code.value.lower()}"


def completion_event_state_specific_dataset(state_code: StateCode) -> str:
    """Returns the dataset containing task eligibility spans for this region."""
    return f"task_eligibility_completion_events_{state_code.value.lower()}"


def compliance_task_eligibility_spans_state_specific_dataset(
    state_code: StateCode,
) -> str:
    """Returns the dataset containing compliance task eligibility spans for this region."""
    return f"compliance_task_eligibility_spans_{state_code.value.lower()}"
