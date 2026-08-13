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
"""Config class for a Label Studio annotation task, loaded from YAML."""
import os
from enum import Enum
from pathlib import Path

import attr
from google.cloud import bigquery

import recidiviz.llm_eval.label_studio as _label_studio_pkg
from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict

_CONFIGS_DIR = os.path.join(os.path.dirname(_label_studio_pkg.__file__), "config")
SUPPORTED_LS_TYPES = frozenset({"choices", "labels", "textarea", "rating", "number"})


class LSFieldTransform(Enum):
    """How to extract a scalar value from a Label Studio result object."""

    CHOICES_TO_BOOL = "choices_to_bool"
    """choices[0] == 'Yes' → TRUE, anything else → FALSE."""

    CHOICES_SINGLE_SELECT = "choices_single_select"
    """choices[0] as a string. Use for single-select (radio) fields."""

    CHOICES_MULTI_SELECT = "choices_multi_select"
    """All selected choices as a JSON array string. Use for multi-select (checkbox) fields."""

    TEXTAREA_TEXT = "textarea_text"
    """text[0] from a textarea result — the raw string the annotator typed into a
    free-text input box. Use for open-ended notes or comments fields. NULL when
    the annotator left the box empty."""


@attr.define(frozen=True, kw_only=True)
class LabelStudioTaskDataField:
    """One column extracted from task.data in the parsed annotations view."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this field."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    extract_as_json: bool = attr.ib(validator=attr_validators.is_bool)
    """If True, uses JSON_QUERY instead of JSON_VALUE to extract this column,
    preserving the raw JSON fragment as a string. Required for fields whose
    value in task.data is a JSON object or array rather than a scalar."""

    def __attrs_post_init__(self) -> None:
        if (
            self.extract_as_json
            and self.bq_type is not bigquery.StandardSqlTypeNames.STRING
        ):
            raise ValueError(
                f"Data field [{self.column_name}] has extract_as_json=True but "
                f"bq_type [{self.bq_type.value}] — JSON objects can only be stored as STRING."
            )

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LabelStudioTaskDataField":
        """Returns a LabelStudioTaskDataField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        field = cls(
            column_name=column_name,
            description=d.pop("description", str),
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            extract_as_json=d.pop_optional("extract_as_json", bool) or False,
        )
        if d:
            raise ValueError(
                f"Unexpected keys in task_data_field [{column_name}]: {repr(d.get())}"
            )
        return field


@attr.define(frozen=True, kw_only=True)
class LabelStudioAnnotationField:
    """One column extracted from annotation result JSON in the parsed annotations view."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this annotation field."""

    ls_type: str = attr.ib(validator=attr_validators.is_str)
    """Matches the 'type' field in Label Studio result JSON."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    transform: LSFieldTransform = attr.ib(
        validator=attr.validators.instance_of(LSFieldTransform)
    )
    """How to extract the value from the LS result object."""

    irr_included: bool = attr.ib(validator=attr_validators.is_bool)
    """If True, included in IRR computation. Defaults to False."""

    value_map: dict[str, str] | None = attr.ib(validator=attr_validators.is_opt(dict))
    """Optional mapping from raw LS choice strings to internal values.
    Applied after the transform. Useful when LS choice labels contain
    descriptive text (e.g. 'GOOD — Accurate, only minor errors' → 'GOOD').
    Unrecognized values pass through unchanged."""

    ordinal_values: list[str] | None = attr.ib(validator=attr_validators.is_opt_list)
    """Ordered category values (worst-to-best) for linear weighted kappa.
    Only valid on STRING fields with irr_included=True. When set, Cohen's
    kappa in the IRR view is computed as linear weighted kappa using these
    ranks — partial disagreements (e.g. GOOD vs PARTIAL) penalise less than
    full disagreements (e.g. GOOD vs BAD). Values must be the post-value_map
    normalised strings (e.g. ['BAD', 'PARTIAL', 'GOOD'])."""

    def __attrs_post_init__(self) -> None:
        if self.ls_type not in SUPPORTED_LS_TYPES:
            raise ValueError(
                f"Unsupported ls_type [{self.ls_type}] for field "
                f"[{self.column_name}]. Supported: {SUPPORTED_LS_TYPES}"
            )
        if (
            self.ordinal_values is not None
            and self.bq_type is not bigquery.StandardSqlTypeNames.STRING
        ):
            raise ValueError(
                f"ordinal_values is only valid for STRING fields, "
                f"but field [{self.column_name}] has bq_type [{self.bq_type.value}]"
            )

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LabelStudioAnnotationField":
        """Returns a LabelStudioAnnotationField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        raw_transform = d.pop("transform", str)
        try:
            transform = LSFieldTransform(raw_transform)
        except ValueError as e:
            raise ValueError(
                f"Unknown transform for field [{column_name}]: [{raw_transform}]"
            ) from e
        value_map_raw = d.pop_optional("value_map", dict)
        value_map: dict[str, str] | None = (
            {str(k): str(v) for k, v in value_map_raw.items()}
            if value_map_raw is not None
            else None
        )
        ordinal_values_raw = d.pop_optional("ordinal_values", list)
        ordinal_values: list[str] | None = (
            [str(v) for v in ordinal_values_raw]
            if ordinal_values_raw is not None
            else None
        )
        field = cls(
            column_name=column_name,
            description=d.pop("description", str),
            ls_type=d.pop("ls_type", str),
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            transform=transform,
            irr_included=d.pop_optional("irr_included", bool) or False,
            value_map=value_map,
            ordinal_values=ordinal_values,
        )
        if d:
            raise ValueError(
                f"Unexpected keys in schema field [{column_name}]: {repr(d.get())}"
            )
        return field


@attr.define(frozen=True, kw_only=True)
class LabelStudioTaskConfig:
    """Configuration for one Label Studio annotation task."""

    task_name: str = attr.ib(validator=attr_validators.is_str)
    """Unique identifier for the task; used as a BQ table/view name suffix."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of what annotators are labeling."""

    labelstudio_project_ids: dict[str, int] = attr.ib(
        validator=attr_validators.is_dict_of(str, int)
    )
    """Label Studio project ID keyed by GCP project ID (e.g. 'recidiviz-staging',
    'recidiviz-123'). Different environments have separate LS projects."""

    gcs_export_prefix: str = attr.ib(validator=attr_validators.is_str)
    """GCS path prefix (within the runtime project's bucket) for this task's exports."""

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
    def from_yaml(cls, yaml_path: str | Path) -> "LabelStudioTaskConfig":
        """Returns a LabelStudioTaskConfig parsed from a YAML file."""
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


def collect_label_studio_task_configs() -> dict[str, LabelStudioTaskConfig]:
    """Returns all LabelStudioTaskConfig instances discovered in the configs dir."""
    configs: dict[str, LabelStudioTaskConfig] = {}
    for entry in sorted(os.scandir(_CONFIGS_DIR), key=lambda e: e.name):
        if not entry.name.endswith(".yaml"):
            continue
        config = LabelStudioTaskConfig.from_yaml(entry.path)
        if config.task_name in configs:
            raise ValueError(
                f"Duplicate task name [{config.task_name}] found in [{entry.path}]"
            )
        configs[config.task_name] = config
    return configs
