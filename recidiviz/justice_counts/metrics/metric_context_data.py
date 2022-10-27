# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Base class for the reported value(s) for a Justice Counts context."""

from typing import Any, Dict, Type, TypeVar

import attr

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.metrics.metric_definition import Context

MetricContextDataT = TypeVar("MetricContextDataT", bound="MetricContextData")


@attr.define()
class MetricContextData:
    """An agency's response to a `Context` field. The `key` should be a unique identifier
    that matches the `Context` object, and `value` should be what the agency reported.
    """

    key: ContextKey
    value: Any

    def to_json(self, context_definition: Context) -> Dict[str, Any]:
        value = self.value
        multiple_choice_options = []
        if context_definition.value_type == ValueType.MULTIPLE_CHOICE:
            for elem in context_definition.multiple_choice_options or []:
                multiple_choice_options.append(elem.value)

        return {
            "key": self.key.value,
            "reporting_note": context_definition.reporting_note,
            "display_name": context_definition.label,
            "type": context_definition.value_type.value,
            "required": context_definition.required,
            "value": value,
            "multiple_choice_options": multiple_choice_options,
        }

    @classmethod
    def from_json(
        cls: Type[MetricContextDataT],
        json: Dict[str, Any],
    ) -> MetricContextDataT:
        key = ContextKey[json["key"]]
        value = json["value"]
        return cls(key=key, value=value)
