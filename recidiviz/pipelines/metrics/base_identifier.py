# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Base class for identifying various instances of events for a metric calculation pipeline."""
import abc
from typing import Any, Dict, Generic, Set, Type, TypeVar

import attr

from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson
from recidiviz.pipelines.utils.identifier_models import IdentifierResult

IdentifierResultT = TypeVar("IdentifierResultT")
IdentifierContext = Dict[str, Any]


@attr.s
class BaseIdentifier(abc.ABC, Generic[IdentifierResultT]):
    """A base class for identifying various events or spans for a metric calculation pipeline."""

    identifier_result_class: Type[IdentifierResult] = attr.ib()

    @abc.abstractmethod
    def identify(
        self,
        person: NormalizedStatePerson,
        identifier_context: IdentifierContext,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> IdentifierResultT:
        """Define the function to identify the events or spans needed by the pipeline."""
