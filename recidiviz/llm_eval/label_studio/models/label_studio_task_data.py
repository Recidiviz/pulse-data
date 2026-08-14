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
"""Holds the typed task.data payload of one kind of Label Studio task, which is everything
an annotator is shown for one unit of work.

A task makes two trips. We upload one carrying this payload, a human labels it, and the
export that comes back carries the payload unchanged as task.data alongside their answers.
LabelStudioProjectConfig describes the return trip, naming which task.data fields to project
into columns of the parsed annotations view. This is the outbound half.

The two halves have to agree on field names, and nothing about them naturally does. The
parsed annotations view reads task.data by column name, so a name written on the way out that
the config does not expect comes back as an all-NULL column rather than an error. A subclass
here closes that gap by declaring one attrs field per key it writes, and construction checks
those fields against the project config, so a payload the export could not parse back cannot
be built at all.

Construction is the backstop rather than the primary guard, since it only fires when an
export actually runs. The test that keeps the two schemas honest compares the declarations
themselves and so catches drift in CI, with no data needed.

The two schemas are not identical and are not meant to be. A task may show an annotator
context that is not worth a column in the exported table, so the outbound payload is a
superset. Every field the config projects must be present, and extra keys are fine.
"""
import abc
import functools
import json
from typing import Any

import attr

from recidiviz.llm_eval.label_studio.models.label_studio_project_config import (
    LabelStudioProjectConfig,
    collect_label_studio_project_configs,
)

# The key Label Studio nests a task's annotator-facing payload under, and the one the parsed
# annotations view reads it back out of as task.data.
_TASK_DATA_KEY = "data"

LabelStudioTaskDataValue = str | int | float | bool | None | dict[str, Any] | list[Any]
"""The value types a task.data field may hold, which are the JSON types, since Label Studio
stores the payload as JSON.

A container is only valid for a field the project config marks extract_as_json, which tells
the parsed annotations view to preserve it whole rather than cast it to a scalar.
LabelStudioTaskDataField.validate_value enforces that pairing. This alias only says what JSON
can carry.
"""


@functools.cache
def _project_config_for_task_name(task_name: str) -> LabelStudioProjectConfig:
    """Returns the project config for the given task name, caching the result because
    building a batch validates every task against its config and the configs are parsed from
    YAML.
    """
    configs = collect_label_studio_project_configs()
    if task_name not in configs:
        raise ValueError(
            f"Found no Label Studio project config for task [{task_name}]. Known tasks: "
            f"{sorted(configs)}."
        )
    return configs[task_name]


@attr.define(frozen=True, kw_only=True)
class LabelStudioTaskData(abc.ABC):
    """Base for the typed task.data payload of one Label Studio task. A subclass declares
    one attrs field per key it writes and names the project config whose export contract it
    has to satisfy, and construction checks the payload against that config.
    """

    @classmethod
    @abc.abstractmethod
    def task_name(cls) -> str:
        """Returns the task_name of the project config this payload is annotated under."""

    @classmethod
    def project_config(cls) -> LabelStudioProjectConfig:
        """Returns the project config this payload is annotated under."""
        return _project_config_for_task_name(cls.task_name())

    def __attrs_post_init__(self) -> None:
        """Checks this payload against its project config, so a payload the export could not
        parse back cannot be constructed at all, rather than existing and failing later on the
        way out.

        A subclass must not override this. attrs calls only the most derived
        __attrs_post_init__, so an override would silently drop the check. Put a subclass's own
        invariants in validate_payload instead.
        """
        self.project_config().validate_task_data(self.task_data)
        self.validate_payload()

    def validate_payload(self) -> None:
        """Checks a subclass's own cross-field invariants, once the payload has satisfied its
        project config. Does nothing by default; override this rather than
        __attrs_post_init__.
        """

    @property
    def task_data(self) -> dict[str, LabelStudioTaskDataValue]:
        """Returns this payload as the task.data object Label Studio stores.

        The dict comes straight off the attrs field declarations, so the subclass's field
        names are the only place the outbound key names are written.
        """
        data: dict[str, LabelStudioTaskDataValue] = attr.asdict(self)
        return data

    def to_import_json(self) -> str:
        """Returns the JSON body Label Studio imports for this task, a single-element array
        of task objects, matching the one-file-per-task layout the exporters write to GCS.
        """
        return json.dumps([{_TASK_DATA_KEY: self.task_data}], indent=2)
