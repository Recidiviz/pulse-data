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
    LabelStudioGcsObjectMetadataTaskCreatedAtSource,
    LabelStudioJoinedTableTaskCreatedAtSource,
    task_created_at_source_from_yaml_dict,
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


def _gcs_object_metadata_yaml(**overrides: str) -> YAMLDict:
    return YAMLDict(
        {
            "source": "gcs_object_metadata",
            "description": "When this task's file was written to GCS.",
            **overrides,
        }
    )


class TaskCreatedAtSourceFromYamlDictTest(unittest.TestCase):
    """Tests for task_created_at_source_from_yaml_dict."""

    def test_joined_table_source_is_parsed(self) -> None:
        self.assertEqual(
            LabelStudioJoinedTableTaskCreatedAtSource(
                description="Start time of the meeting this task is for.",
                table_address=BigQueryAddress(
                    dataset_id="meetings_db", table_id="Meeting"
                ),
                table_join_column="id",
                task_join_column="meeting_id",
                column="startTime",
            ),
            task_created_at_source_from_yaml_dict(_joined_table_yaml()),
        )

    def test_gcs_object_metadata_source_is_parsed(self) -> None:
        self.assertEqual(
            LabelStudioGcsObjectMetadataTaskCreatedAtSource(
                description="When this task's file was written to GCS."
            ),
            task_created_at_source_from_yaml_dict(_gcs_object_metadata_yaml()),
        )

    def test_unknown_source(self) -> None:
        with self.assertRaisesRegex(ValueError, r"'bucket_listing' is not a valid"):
            task_created_at_source_from_yaml_dict(
                _joined_table_yaml(source="bucket_listing")
            )

    def test_unexpected_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Unexpected keys in task_created_at: .*'timestamp_columns'"
        ):
            task_created_at_source_from_yaml_dict(
                _joined_table_yaml(timestamp_columns="startTime")
            )

    def test_joined_table_missing_column(self) -> None:
        yaml_dict = _joined_table_yaml()
        yaml_dict.pop("column", str)
        with self.assertRaisesRegex(KeyError, r"column"):
            task_created_at_source_from_yaml_dict(yaml_dict)

    def test_gcs_object_metadata_with_joined_table_keys(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Unexpected keys in task_created_at: .*'table'"
        ):
            task_created_at_source_from_yaml_dict(
                _gcs_object_metadata_yaml(table="meetings_db.Meeting")
            )

    def test_gcs_object_metadata_missing_description(self) -> None:
        yaml_dict = _gcs_object_metadata_yaml()
        yaml_dict.pop("description", str)
        with self.assertRaisesRegex(KeyError, r"description"):
            task_created_at_source_from_yaml_dict(yaml_dict)


class SourceClassFromYamlDictTest(unittest.TestCase):
    """Tests for each source class's own from_yaml_dict."""

    def test_class_rejects_the_other_source(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expected task_created_at source \[joined_table\], found "
            r"\[gcs_object_metadata\]$",
        ):
            LabelStudioJoinedTableTaskCreatedAtSource.from_yaml_dict(
                _gcs_object_metadata_yaml()
            )
        with self.assertRaisesRegex(
            ValueError,
            r"^Expected task_created_at source \[gcs_object_metadata\], found "
            r"\[joined_table\]$",
        ):
            LabelStudioGcsObjectMetadataTaskCreatedAtSource.from_yaml_dict(
                _joined_table_yaml()
            )
