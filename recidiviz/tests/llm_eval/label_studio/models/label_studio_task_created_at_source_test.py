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
"""Tests for label_studio_task_created_at_source.py."""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.llm_eval.label_studio.models.label_studio_task_created_at_source import (
    LabelStudioTaskCreatedAtSource,
)
from recidiviz.utils.yaml_dict import YAMLDict


def _joined_table_yaml(**overrides: str) -> YAMLDict:
    return YAMLDict(
        {
            "source": "joined_table",
            "table": "meetings_db.Meeting",
            "table_join_column": "id",
            "task_join_column": "meeting_id",
            "column": "startTime",
            "description": "Start time of the meeting this task is for.",
            **overrides,
        }
    )


class LabelStudioTaskCreatedAtSourceTest(unittest.TestCase):
    """Tests for LabelStudioTaskCreatedAtSource."""

    def test_joined_table_source_is_parsed(self) -> None:
        self.assertEqual(
            LabelStudioTaskCreatedAtSource(
                description="Start time of the meeting this task is for.",
                table_address=BigQueryAddress(
                    dataset_id="meetings_db", table_id="Meeting"
                ),
                table_join_column="id",
                task_join_column="meeting_id",
                column="startTime",
            ),
            LabelStudioTaskCreatedAtSource.from_yaml_dict(_joined_table_yaml()),
        )

    def test_unknown_source(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"'gcs_object_metadata' is not a valid"
        ):
            LabelStudioTaskCreatedAtSource.from_yaml_dict(
                _joined_table_yaml(source="gcs_object_metadata")
            )

    def test_unexpected_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Unexpected keys in task_created_at: .*'timestamp_columns'"
        ):
            LabelStudioTaskCreatedAtSource.from_yaml_dict(
                _joined_table_yaml(timestamp_columns="startTime")
            )
