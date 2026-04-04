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

import attr
from marshmallow import Schema

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.common.constants.states import StateCode

DataT = TypeVar("DataT")


@attr.s(frozen=True)
class WritebackConfig:
    state_code: StateCode = attr.ib()
    operation_action_description: str = attr.ib()
    api_schema_cls: type[Schema] = attr.ib()


class WritebackStatusTracker(abc.ABC):
    @abc.abstractmethod
    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        ...


class WritebackExecutorInterface(abc.ABC, Generic[DataT]):
    @abc.abstractmethod
    def execute(self, request_data: DataT) -> None:
        """Perform the external system request. Raises on failure."""

    # TODO(#68791): Consider switching to pydantic instead of marshmallow, so that when
    # we parse request bodies we get a class instance instead of a dict
    @classmethod
    @abc.abstractmethod
    def parse_request_data(cls, raw_request: dict[str, Any]) -> DataT:
        """Convert a raw dict (from marshmallow dump) into a typed data object."""

    @abc.abstractmethod
    def create_status_tracker(self) -> WritebackStatusTracker:
        ...

    @classmethod
    @abc.abstractmethod
    def config(cls) -> WritebackConfig:
        ...
