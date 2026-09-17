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
"""The authored definition of one Label Studio annotation project, loaded from YAML.

A project is the standing setup for one kind of annotation work: which Label Studio
project holds it in each environment, where its exports land, what the annotators are
shown, and what they answer. LabelStudioTaskData is the other half of the picture, since one
project accumulates thousands of those payloads, one per unit of work.
"""
import os
from collections.abc import Mapping
from pathlib import Path

import attr

import recidiviz.llm_eval.label_studio as _label_studio_pkg
from recidiviz.common import attr_validators
from recidiviz.llm_eval.label_studio.models.label_studio_annotation_field import (
    LabelStudioAnnotationField,
)
from recidiviz.llm_eval.label_studio.models.label_studio_task_data_field import (
    LabelStudioTaskDataField,
)
from recidiviz.utils.yaml_dict import YAMLDict

_CONFIGS_DIR = os.path.join(os.path.dirname(_label_studio_pkg.__file__), "config")

# Name of the task_data_field every config must declare, since every deployed view has to
# output a state_code column.
STATE_CODE_COLUMN_NAME = "state_code"

# Name of the single column the submitted tasks external table exposes: one raw line of a
# task file. generate_raw_table_yamls.py declares the column and the submitted tasks view
# reads it, so it lives here rather than in either of them.
SUBMITTED_TASKS_LINE_COLUMN_NAME = "line"


@attr.define(frozen=True, kw_only=True)
class LabelStudioProjectConfig:
    """The authored definition of one Label Studio annotation project. See the module
    docstring.
    """

    task_name: str = attr.ib(validator=attr_validators.is_str)
    """Unique identifier for the kind of task this project collects annotations for; used
    as a BQ table/view name suffix. Named for the task rather than the project because it
    is the annotation work that is stable, whereas the Label Studio project holding it
    differs per environment (see labelstudio_project_ids) and can be recreated."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of what annotators are labeling."""

    labelstudio_project_ids: dict[str, int] = attr.ib(
        validator=attr_validators.is_dict_of(str, int)
    )
    """Label Studio project ID keyed by GCP project ID (e.g. 'recidiviz-staging',
    'recidiviz-123'). Different environments have separate LS projects."""

    gcs_export_prefix: str = attr.ib(validator=attr_validators.is_str)
    """GCS path prefix (within the runtime project's bucket) for this project's exports."""

    task_import_source_uri: str = attr.ib(validator=attr_validators.is_str)
    """Source URI pattern for the GCS objects Label Studio imports this task's tasks from,
    one object per task. Must contain a {project_id} format argument, which the source table
    framework substitutes per environment, and exactly one '*' wildcard, which is all
    BigQuery accepts per sourceUris entry."""

    task_import_source_project_mapping: dict[str, str] = attr.ib(
        validator=attr_validators.is_dict_of(str, str)
    )
    """GCP project the import objects live in, keyed by runtime GCP project, for tasks whose
    files another project writes. Empty when the objects live in the runtime project's own
    bucket."""

    task_import_path_contains: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Substring every one of this task's import objects has in its path, used to scope the
    view when the import URI necessarily matches more than this task.

    See the WHERE clause in submitted_tasks.py for why a source URI cannot narrow to one
    task type on its own."""

    task_import_path_excludes: list[str] = attr.ib(
        validator=attr_validators.is_list_of(str)
    )
    """Substrings that disqualify an import object, applied after task_import_path_contains.
    Empty for a task whose prefix holds nothing but real tasks. Use this for objects that
    match the task's own path shape but are not work anyone should be measured on, such as
    demo data staged into the same bucket."""

    task_import_path_field_segments: dict[str, int] = attr.ib(
        validator=attr_validators.is_dict_of(str, int)
    )
    """0-indexed segment of the import object's path holding each named key field, for
    fields the task payload may omit. The segments are counted from the first segment after
    the bucket name.

    Used only as a fallback: a value the payload carries always wins, so declaring a field
    here never overrides what the task itself says. Declare a field only when older objects
    in the prefix predate its addition to the payload, and only for fields the path really
    does carry. Empty for a task whose payloads have always been complete."""

    task_data_fields: list[LabelStudioTaskDataField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LabelStudioTaskDataField),
        ]
    )
    """Columns extracted from task.data (the input shown to annotators)."""

    annotation_fields: list[LabelStudioAnnotationField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LabelStudioAnnotationField),
        ]
    )
    """Columns extracted from annotation result JSON (the annotator's output)."""

    primary_key_fields: list[str] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(str),
        ]
    )
    """Column names (from task_data_fields) that form the natural key for coverage
    analysis. Used to generate the annotation summary view grouped by these columns."""

    def __attrs_post_init__(self) -> None:
        data_field_names = {f.column_name for f in self.task_data_fields}
        if not any(f.irr_included for f in self.annotation_fields):
            raise ValueError(
                f"Task [{self.task_name}] has no schema fields with irr_included: true. "
                f"At least one field must be included in IRR computation."
            )
        unknown = [k for k in self.primary_key_fields if k not in data_field_names]
        if unknown:
            raise ValueError(
                f"Task [{self.task_name}] has primary_key_fields that are not in "
                f"task_data_fields: {unknown}"
            )
        if "{project_id}" not in self.task_import_source_uri:
            raise ValueError(
                f"Task [{self.task_name}] has task_import_source_uri "
                f"[{self.task_import_source_uri}], which has no {{project_id}} format "
                f"argument. The source table framework substitutes {{project_id}} per "
                f"environment and fails on a URI without it."
            )
        wildcard_count = self.task_import_source_uri.count("*")
        if wildcard_count != 1:
            raise ValueError(
                f"Task [{self.task_name}] has task_import_source_uri "
                f"[{self.task_import_source_uri}] with {wildcard_count} '*' wildcards, but "
                f"BigQuery accepts exactly one per sourceUris entry and rejects more with "
                f"'Using multiple asterisks in Google Cloud Storage source URI is not "
                f"supported'. Use a single '*', which matches across '/' in BigQuery, so "
                f"one wildcard spans every directory beneath it. Fix the URI in "
                f"recidiviz/llm_eval/label_studio/config/{self.task_name}.yaml."
            )
        if STATE_CODE_COLUMN_NAME not in data_field_names:
            raise ValueError(
                f"Task [{self.task_name}] has no task_data_field named "
                f"[{STATE_CODE_COLUMN_NAME}], which every deployed view must output. Add it "
                f"to task_data_fields."
            )
        reserved_primary_key_fields = sorted(
            {STATE_CODE_COLUMN_NAME} & set(self.primary_key_fields)
        )
        if reserved_primary_key_fields:
            raise ValueError(
                f"Task [{self.task_name}] has primary_key_fields "
                f"{reserved_primary_key_fields} that duplicate columns the submitted "
                f"tasks view always projects. Remove {reserved_primary_key_fields} from "
                f"primary_key_fields."
            )
        if blank_excludes := [
            s for s in self.task_import_path_excludes if not s.strip()
        ]:
            raise ValueError(
                f"Task [{self.task_name}] has blank entries in task_import_path_excludes "
                f"{blank_excludes}, which would exclude every import object. Remove them "
                f"from recidiviz/llm_eval/label_studio/config/{self.task_name}.yaml."
            )
        key_fields = set(self.task_key_fields)
        if unknown_segment_fields := sorted(
            set(self.task_import_path_field_segments) - key_fields
        ):
            raise ValueError(
                f"Task [{self.task_name}] declares task_import_path_field_segments for "
                f"{unknown_segment_fields}, which are not key fields "
                f"{sorted(key_fields)}. The submitted tasks view only reads key fields "
                f"from the import path, so a segment declared for any other field does "
                f"nothing. Remove it from "
                f"recidiviz/llm_eval/label_studio/config/{self.task_name}.yaml."
            )
        if negative_segments := sorted(
            name
            for name, segment in self.task_import_path_field_segments.items()
            if segment < 0
        ):
            raise ValueError(
                f"Task [{self.task_name}] declares negative "
                f"task_import_path_field_segments for {negative_segments}. Path segments "
                f"are counted forward from the first segment after the bucket name, so "
                f"the index must be 0 or greater. Fix it in "
                f"recidiviz/llm_eval/label_studio/config/{self.task_name}.yaml."
            )

    def validate_task_data(self, task_data: Mapping[str, object]) -> None:
        """Raises unless the uploaded payload for one task of this kind carries every field
        this project's parsed annotations view projects out of task.data, each holding a value
        of the type that view casts it to.

        Extra keys pass, because a task may show an annotator context that is not worth a
        column in the exported table. The presence check runs in this direction because it is
        the one that catches a name drifting apart from this config. The parsed annotations
        view reads task.data by column name, so a key that isn't there yields an all-NULL
        column rather than an error, and a wrongly typed one yields a failed CAST.
        """
        if missing := sorted(
            field.column_name
            for field in self.task_data_fields
            if field.column_name not in task_data
        ):
            raise ValueError(
                f"Task data uploaded for [{self.task_name}] is missing field(s) "
                f"{missing}, which its parsed annotations view projects into columns. "
                f"Fields present: {sorted(task_data)}."
            )
        for field in self.task_data_fields:
            try:
                field.validate_value(task_data[field.column_name])
            except ValueError as e:
                raise ValueError(
                    f"Task data uploaded for [{self.task_name}] has an unusable value: "
                    f"{e}"
                ) from e

    def labelstudio_project_id_for(self, gcp_project: str) -> int:
        """Returns the Label Studio project ID for the given GCP project.

        Raises KeyError if the GCP project is not configured for this task.
        """
        if gcp_project not in self.labelstudio_project_ids:
            raise KeyError(
                f"Task [{self.task_name}] has no Label Studio project ID configured "
                f"for GCP project [{gcp_project}]. "
                f"Configured projects: {sorted(self.labelstudio_project_ids)}"
            )
        return self.labelstudio_project_ids[gcp_project]

    @property
    def irr_annotation_fields(self) -> list[LabelStudioAnnotationField]:
        """Returns the annotation fields that participate in IRR computation."""
        return [f for f in self.annotation_fields if f.irr_included]

    @property
    def task_key_fields(self) -> list[str]:
        """Returns the columns that identify one logical task, and so the columns the
        submitted tasks view dedupes on: the task's primary key plus state_code.
        """
        return [*self.primary_key_fields, STATE_CODE_COLUMN_NAME]

    @property
    def raw_table_id(self) -> str:
        """Returns the BQ table ID for the raw annotations table."""
        return f"{self.task_name}_annotations_raw"

    @property
    def submitted_tasks_raw_table_id(self) -> str:
        """Returns the BQ table ID for the raw submitted tasks external table."""
        return f"{self.task_name}_submitted_tasks_raw"

    @property
    def submitted_tasks_view_id(self) -> str:
        """Returns the BQ view ID for the submitted tasks view."""
        return f"{self.task_name}_submitted_tasks"

    @property
    def annotations_view_id(self) -> str:
        """Returns the BQ view ID for the parsed annotations view."""
        return f"{self.task_name}_annotations_parsed"

    @property
    def overrides_view_id(self) -> str:
        """Returns the BQ view ID for the parsed reviewer-overrides view."""
        return f"{self.task_name}_annotation_overrides"

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "LabelStudioProjectConfig":
        """Returns a LabelStudioProjectConfig parsed from a YAML file."""
        d = YAMLDict.from_path(yaml_path)
        task_name = d.pop("task_name", str)
        description = d.pop("description", str)
        project_ids_raw = d.pop("labelstudio_project_ids", dict)
        project_ids = {str(k): int(v) for k, v in project_ids_raw.items()}
        prefix = d.pop("gcs_export_prefix", str)
        import_source_uri = d.pop("task_import_source_uri", str)
        import_project_mapping_raw = (
            d.pop_optional("task_import_source_project_mapping", dict) or {}
        )
        import_project_mapping = {
            str(k): str(v) for k, v in import_project_mapping_raw.items()
        }
        path_contains = d.pop("task_import_path_contains", str)
        path_excludes = [
            str(e) for e in d.pop_optional("task_import_path_excludes", list) or []
        ]
        path_field_segments_raw = (
            d.pop_optional("task_import_path_field_segments", dict) or {}
        )
        path_field_segments = {
            str(k): int(v) for k, v in path_field_segments_raw.items()
        }
        task_data_fields = [
            LabelStudioTaskDataField.from_yaml_dict(fd)
            for fd in d.pop_dicts("task_data_fields")
        ]
        annotation_fields = [
            LabelStudioAnnotationField.from_yaml_dict(sd)
            for sd in d.pop_dicts("annotation_fields")
        ]
        primary_key_fields = [str(k) for k in d.pop("primary_key_fields", list)]
        if d:
            raise ValueError(
                f"Unexpected keys in task config [{task_name}]: {repr(d.get())}"
            )
        return cls(
            task_name=task_name,
            description=description,
            labelstudio_project_ids=project_ids,
            gcs_export_prefix=prefix,
            task_import_source_uri=import_source_uri,
            task_import_source_project_mapping=import_project_mapping,
            task_import_path_contains=path_contains,
            task_import_path_excludes=path_excludes,
            task_import_path_field_segments=path_field_segments,
            task_data_fields=task_data_fields,
            annotation_fields=annotation_fields,
            primary_key_fields=primary_key_fields,
        )


def collect_label_studio_project_configs() -> dict[str, LabelStudioProjectConfig]:
    """Returns all LabelStudioProjectConfig instances discovered in the configs dir,
    keyed by task_name.
    """
    configs: dict[str, LabelStudioProjectConfig] = {}
    for entry in sorted(os.scandir(_CONFIGS_DIR), key=lambda e: e.name):
        if not entry.name.endswith(".yaml"):
            continue
        config = LabelStudioProjectConfig.from_yaml(entry.path)
        if config.task_name in configs:
            raise ValueError(
                f"Duplicate task name [{config.task_name}] found in [{entry.path}]"
            )
        configs[config.task_name] = config
    return configs
