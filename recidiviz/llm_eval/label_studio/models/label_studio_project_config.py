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
    def raw_table_id(self) -> str:
        """Returns the BQ table ID for the raw annotations table."""
        return f"{self.task_name}_annotations_raw"

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
