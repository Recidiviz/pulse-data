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
"""Defines a BigQueryViewBuilder that can be used to define task completion events of a
given type. These views are used as inputs to a task eligibility spans view.
"""
from enum import Enum
from typing import Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_COMPLETION_EVENTS_DATASET_ID,
    completion_event_state_specific_dataset,
)


class TaskCompletionEventType(Enum):
    FULL_TERM_DISCHARGE = "FULL_TERM_DISCHARGE"
    EARLY_DISCHARGE = "EARLY_DISCHARGE"
    TRANSFER_TO_LIMITED_SUPERVISION = "TRANSFER_TO_LIMITED_SUPERVISION"
    SUPERVISION_LEVEL_DOWNGRADE = "SUPERVISION_LEVEL_DOWNGRADE"
    CUSTODY_LEVEL_DOWNGRADE = "CUSTODY_LEVEL_DOWNGRADE"
    SUPERVISION_CLASSIFICATION_REVIEW = "SUPERVISION_CLASSIFICATION_REVIEW"
    RELEASE_TO_FURLOUGH = "RELEASE_TO_FURLOUGH"
    RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION = (
        "RELEASE_TO_COMMUNITY_CONFINEMENT_SUPERVISION"
    )
    RELEASE_TO_PAROLE = "RELEASE_TO_PAROLE"
    TRANSFER_OUT_OF_SOLITARY_CONFINEMENT = "TRANSFER_OUT_OF_SOLITARY_CONFINEMENT"
    SCHEDULED_HEARING_OCCURRED = "SCHEDULED_HEARING_OCCURRED"
    GRANTED_WORK_RELEASE = "GRANTED_WORK_RELEASE"
    GRANTED_FURLOUGH = "GRANTED_FURLOUGH"
    TRANSFER_TO_REENTRY_CENTER = "TRANSFER_TO_REENTRY_CENTER"


class StateSpecificTaskCompletionEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """Defines a BigQueryViewBuilder that can be used to define task completion events
    of a given type. These views are used as inputs to a task eligibility spans view.
    """

    def __init__(
        self,
        state_code: StateCode,
        completion_event_type: TaskCompletionEventType,
        completion_event_query_template: str,
        description: str,
        **query_format_kwargs: str,
    ) -> None:
        view_id = completion_event_type.value.lower()
        super().__init__(
            dataset_id=completion_event_state_specific_dataset(state_code),
            view_id=view_id,
            description=description,
            view_query_template=completion_event_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.completion_event_type = completion_event_type
        self.state_code = state_code
        self.task_type_name = completion_event_type.name
        self.task_title = self.task_type_name.replace("_", " ").title()


class StateAgnosticTaskCompletionEventBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """Defines a BigQueryViewBuilder that can be used to define task completion events
    of a given type. These views are used as inputs to a task eligibility spans view.
    """

    def __init__(
        self,
        completion_event_type: TaskCompletionEventType,
        completion_event_query_template: str,
        description: str,
        **query_format_kwargs: str,
    ) -> None:
        view_id = completion_event_type.value.lower()
        super().__init__(
            dataset_id=TASK_COMPLETION_EVENTS_DATASET_ID,
            view_id=view_id,
            description=description,
            view_query_template=completion_event_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.completion_event_type = completion_event_type
        self.task_type_name = completion_event_type.name
        self.task_title = self.task_type_name.replace("_", " ").title()


TaskCompletionEventBigQueryViewBuilder = Union[
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
]
