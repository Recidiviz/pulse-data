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
"""Tests for LabelStudioProjectConfig."""
import unittest

from google.cloud import bigquery

from recidiviz.llm_eval.label_studio.models.label_studio_annotation_field import (
    LabelStudioAnnotationField,
)
from recidiviz.llm_eval.label_studio.models.label_studio_field_transform import (
    LabelStudioFieldTransform,
)
from recidiviz.llm_eval.label_studio.models.label_studio_project_config import (
    LabelStudioProjectConfig,
    collect_label_studio_project_configs,
)
from recidiviz.llm_eval.label_studio.models.label_studio_task_data_field import (
    LabelStudioTaskDataField,
)


def _data_field(column_name: str) -> LabelStudioTaskDataField:
    return LabelStudioTaskDataField(
        column_name=column_name,
        description=f"The {column_name}.",
        bq_type=bigquery.StandardSqlTypeNames.STRING,
        extract_as_json=False,
    )


def _annotation_field() -> LabelStudioAnnotationField:
    return LabelStudioAnnotationField(
        column_name="is_correct",
        description="Whether the value is correct.",
        ls_type="choices",
        bq_type=bigquery.StandardSqlTypeNames.BOOL,
        transform=LabelStudioFieldTransform.CHOICES_TO_BOOL,
        irr_included=True,
        value_map=None,
        ordinal_values=None,
    )


def _config(
    *,
    task_import_source_uri: str = "gs://{project_id}-label-studio/imports/test_task/*",
    task_data_fields: list[LabelStudioTaskDataField] | None = None,
    primary_key_fields: list[str] | None = None,
    task_import_path_excludes: list[str] | None = None,
    task_import_path_field_segments: dict[str, int] | None = None,
) -> LabelStudioProjectConfig:
    return LabelStudioProjectConfig(
        task_name="test_task",
        description="A test task.",
        labelstudio_project_ids={"recidiviz-staging": 1},
        gcs_export_prefix="exports/test_task",
        task_import_source_uri=task_import_source_uri,
        task_import_source_project_mapping={},
        task_import_path_contains="/test_task/",
        task_import_path_excludes=task_import_path_excludes or [],
        task_import_path_field_segments=task_import_path_field_segments or {},
        task_data_fields=task_data_fields
        or [
            _data_field("document_id"),
            _data_field("state_code"),
        ],
        annotation_fields=[_annotation_field()],
        primary_key_fields=primary_key_fields or ["document_id"],
    )


class LabelStudioProjectConfigTest(unittest.TestCase):
    """Tests for LabelStudioProjectConfig."""

    def test_table_and_view_ids_are_derived_from_task_name(self) -> None:
        config = _config()
        self.assertEqual(
            "test_task_submitted_tasks_raw", config.submitted_tasks_raw_table_id
        )
        self.assertEqual("test_task_submitted_tasks", config.submitted_tasks_view_id)

    def test_import_source_uri_without_project_id(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has task_import_source_uri "
            r"\[gs://recidiviz-ls-raw-data/task_imports/cni/\*\], which has no "
            r"\{project_id\} format argument\. The source table framework substitutes "
            r"\{project_id\} per environment and fails on a URI without it\.$",
        ):
            _config(
                task_import_source_uri="gs://recidiviz-ls-raw-data/task_imports/cni/*"
            )

    def test_import_source_uri_with_two_wildcards(self) -> None:
        """BigQuery rejects more than one asterisk per sourceUris entry, and no test can
        catch it downstream because nothing creates the external table.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has task_import_source_uri "
            r"\[gs://\{project_id\}-label-studio/imports/test_task/\*/\*\.json\] with 2 "
            r"'\*' wildcards, but BigQuery accepts exactly one per sourceUris entry and "
            r"rejects more with 'Using multiple asterisks in Google Cloud Storage source "
            r"URI is not supported'\. Use a single '\*', which matches across '/' in "
            r"BigQuery, so one wildcard spans every directory beneath it\. Fix the URI in "
            r"recidiviz/llm_eval/label_studio/config/test_task\.yaml\.$",
        ):
            _config(
                task_import_source_uri="gs://{project_id}-label-studio/imports/test_task/*/*.json"
            )

    def test_import_source_uri_without_wildcard(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has task_import_source_uri "
            r"\[gs://\{project_id\}-label-studio/imports/test_task\.json\] with 0 '\*' "
            r"wildcards, but BigQuery accepts exactly one per sourceUris entry and "
            r"rejects more with 'Using multiple asterisks in Google Cloud Storage source "
            r"URI is not supported'\. Use a single '\*', which matches across '/' in "
            r"BigQuery, so one wildcard spans every directory beneath it\. Fix the URI in "
            r"recidiviz/llm_eval/label_studio/config/test_task\.yaml\.$",
        ):
            _config(
                task_import_source_uri="gs://{project_id}-label-studio/imports/test_task.json"
            )

    def test_missing_state_code_data_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has no task_data_field named \[state_code\], which every "
            r"deployed view must output\. Add it to task_data_fields\.$",
        ):
            _config(task_data_fields=[_data_field("document_id")])

    def test_primary_key_fields_cannot_include_state_code(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has primary_key_fields \['state_code'\] that "
            r"duplicate columns the submitted tasks view always projects\. Remove "
            r"\['state_code'\] from primary_key_fields\.$",
        ):
            _config(primary_key_fields=["document_id", "state_code"])

    def test_path_field_segment_for_a_non_key_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] declares task_import_path_field_segments for "
            r"\['note_text'\], which are not key fields \['document_id', 'state_code'\]\. "
            r"The submitted tasks view only reads key fields from the import path, so a "
            r"segment declared for any other field does nothing\. Remove it from "
            r"recidiviz/llm_eval/label_studio/config/test_task\.yaml\.$",
        ):
            _config(task_import_path_field_segments={"note_text": 0})

    def test_negative_path_field_segment(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] declares negative task_import_path_field_segments for "
            r"\['state_code'\]\. Path segments are counted forward from the first segment "
            r"after the bucket name, so the index must be 0 or greater\. Fix it in "
            r"recidiviz/llm_eval/label_studio/config/test_task\.yaml\.$",
        ):
            _config(task_import_path_field_segments={"state_code": -1})

    def test_blank_path_exclude(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Task \[test_task\] has blank entries in task_import_path_excludes "
            r"\['   '\], which would exclude every import object\. Remove them from "
            r"recidiviz/llm_eval/label_studio/config/test_task\.yaml\.$",
        ):
            _config(task_import_path_excludes=["   "])

    def test_load_all_configs(self) -> None:
        # Raises if any real config fails to parse or validate.
        self.assertEqual(
            {"cni_accuracy_per_field", "meetings_module_quality"},
            set(collect_label_studio_project_configs()),
        )
