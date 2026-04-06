# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Base abstractions for the writeback system."""
import abc
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus


class WritebackRequestData(BaseModel):
    """Base pydantic model for writeback request data.

    Accepts both camelCase (from frontend) and snake_case field names.
    """

    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, frozen=True
    )

    should_queue_task: bool = True


RequestDataT = TypeVar("RequestDataT", bound=WritebackRequestData)


class WritebackStatusTracker(abc.ABC):
    @abc.abstractmethod
    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        ...


class WritebackExecutorInterface(abc.ABC, Generic[RequestDataT]):
    @abc.abstractmethod
    def to_cloud_task_payload(self) -> dict[str, Any]:
        ...

    @abc.abstractmethod
    def execute(self) -> None:
        """Perform the external system request. Raises on failure."""

    @abc.abstractmethod
    def create_status_tracker(self) -> WritebackStatusTracker:
        ...

    @property
    @abc.abstractmethod
    def operation_action_description(self) -> str:
        ...
