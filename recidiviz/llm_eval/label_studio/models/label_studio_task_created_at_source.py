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
"""Where the submitted tasks view reads each task's task_created_at timestamp from."""
from enum import Enum

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict


class LabelStudioTaskCreatedAtSourceType(Enum):
    """Kinds of place a task's task_created_at can come from."""

    JOINED_TABLE = "joined_table"
    """A column of another BigQuery table, joined to the submitted tasks view."""


@attr.define(frozen=True, kw_only=True)
class LabelStudioTaskCreatedAtSource:
    """A table the submitted tasks view LEFT JOINs to fill task_created_at.

    For example, a Meetings task keyed on meeting_id can take its timestamp from the
    Meetings DB's Meeting table, matching meeting_id to Meeting.id and reading
    Meeting.startTime.
    """

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Description of the task_created_at column for this task, saying what the timestamp
    actually records, since each task's source measures something different."""

    table_address: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )
    """Table holding the timestamps, in the same GCP project as the view."""

    table_join_column: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Column of table_address matched against task_join_column."""

    task_join_column: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Column of the submitted tasks view to join on, which must be one of the task's key
    fields."""

    column: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """TIMESTAMP column of table_address that becomes task_created_at."""

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LabelStudioTaskCreatedAtSource":
        """Returns a LabelStudioTaskCreatedAtSource parsed from the task_created_at
        section of a task config.
        """
        source_type = LabelStudioTaskCreatedAtSourceType(d.pop("source", str))
        if source_type is not LabelStudioTaskCreatedAtSourceType.JOINED_TABLE:
            raise ValueError(f"Unexpected task_created_at source [{source_type}]")
        source = cls(
            description=d.pop("description", str),
            table_address=BigQueryAddress.from_str(d.pop("table", str)),
            table_join_column=d.pop("table_join_column", str),
            task_join_column=d.pop("task_join_column", str),
            column=d.pop("column", str),
        )
        if d:
            raise ValueError(f"Unexpected keys in task_created_at: {repr(d.get())}")
        return source
