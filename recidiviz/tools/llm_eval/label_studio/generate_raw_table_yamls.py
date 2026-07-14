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
"""Generates source table YAML files for Label Studio raw annotation tables from the
corresponding task config YAMLs.

Run to regenerate after adding or modifying a task config:

    python -m recidiviz.tools.llm_eval.label_studio.generate_raw_table_yamls
"""
import os

import yaml

import recidiviz.source_tables.yaml_managed as _yaml_managed_pkg
from recidiviz.llm_eval.label_studio.labelstudio_task_config import (
    LabelStudioTaskConfig,
    LabelStudioTaskDataField,
    collect_label_studio_task_configs,
)

_OUTPUT_DIR = os.path.join(
    os.path.dirname(_yaml_managed_pkg.__file__),
    "gcs_backed_tables",
    "llm_eval_label_studio",
)

# BQ returns legacy SQL type aliases (INTEGER, FLOAT, BOOLEAN) for deployed fields.
# Use legacy names in YAMLs so the schema comparison logic sees no diff.
_STANDARD_TO_LEGACY_BQ_TYPE: dict[str, str] = {
    "INT64": "INTEGER",
    "FLOAT64": "FLOAT",
    "BOOL": "BOOLEAN",
}


def _bq_type_name(standard_type: str) -> str:
    return _STANDARD_TO_LEGACY_BQ_TYPE.get(standard_type, standard_type)


# Infrastructure fields present in every Label Studio annotation export,
# independent of task config.
_INFRASTRUCTURE_FIELDS: list[dict] = [
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE",
        "description": "Label Studio annotation ID.",
    },
    {
        "name": "created_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "description": "Timestamp when the annotation was submitted.",
    },
    {
        "name": "lead_time",
        "type": "FLOAT",
        "mode": "NULLABLE",
        "description": "Time in seconds the annotator spent on the task.",
    },
    {
        "name": "was_cancelled",
        "type": "BOOLEAN",
        "mode": "NULLABLE",
        "description": "Whether the annotation was cancelled rather than submitted.",
    },
    {
        "name": "completed_by",
        "type": "RECORD",
        "mode": "NULLABLE",
        "description": "Annotator who completed this annotation.",
        "fields": [
            {
                "name": "email",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "Email address of the annotator.",
            }
        ],
    },
]

# result[] structure is the same for every task — from_name values differ per
# task but the RECORD shape does not.
_RESULT_FIELD: dict = {
    "name": "result",
    "type": "RECORD",
    "mode": "REPEATED",
    "description": "Annotation result items, one per annotated field.",
    "fields": [
        {
            "name": "type",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Label Studio result type (e.g. choices, textarea).",
        },
        {
            "name": "from_name",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Name of the annotated field.",
        },
        {
            "name": "value",
            "type": "RECORD",
            "mode": "NULLABLE",
            "description": "The annotated value.",
            "fields": [
                {
                    "name": "choices",
                    "type": "STRING",
                    "mode": "REPEATED",
                    "description": "Selected choices for choice-type fields.",
                },
                {
                    "name": "text",
                    "type": "STRING",
                    "mode": "REPEATED",
                    "description": "Text value for textarea-type fields.",
                },
            ],
        },
    ],
}


def _data_fields_for_config(
    task_data_fields: list[LabelStudioTaskDataField],
) -> list[dict]:
    """Returns schema dicts for task.data columns."""
    return [
        {
            "name": field.column_name,
            "type": _bq_type_name(field.bq_type.value),
            "mode": "NULLABLE",
            "description": field.description,
        }
        for field in task_data_fields
    ]


def build_raw_table_yaml_dict(config: LabelStudioTaskConfig) -> dict:
    """Returns the YAML dict for the raw annotations external table for |config|."""
    task_field: dict = {
        "name": "task",
        "type": "RECORD",
        "mode": "NULLABLE",
        "description": "The Label Studio task this annotation belongs to.",
        "fields": [
            {
                "name": "id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "Label Studio task ID.",
            },
            {
                "name": "inner_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "Task sequence number within the Label Studio project.",
            },
            {
                "name": "data",
                "type": "RECORD",
                "mode": "NULLABLE",
                "description": "Task input data fields.",
                "fields": _data_fields_for_config(config.task_data_fields),
            },
        ],
    }
    return {
        "address": {
            "dataset_id": "label_studio",
            "table_id": config.raw_table_id,
        },
        "description": (
            f"Raw Label Studio annotation exports for the {config.task_name} task. "
            f"GCS-backed external table reading directly from the label-studio export "
            f"bucket. Auto-generated from "
            f"recidiviz/llm_eval/label_studio/config/{config.task_name}.yaml "
            f"— do not edit manually."
        ),
        "schema": [*_INFRASTRUCTURE_FIELDS, task_field, _RESULT_FIELD],
        "external_data_configuration": {
            "sourceUris": [
                f"gs://{{project_id}}-label-studio/{config.gcs_export_prefix}/*.json"
            ],
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "ignoreUnknownValues": True,
            "compression": "NONE",
        },
    }


def generate_raw_table_yamls(output_dir: str = _OUTPUT_DIR) -> None:
    """Writes one raw table YAML per task config into |output_dir|."""
    configs = collect_label_studio_task_configs()
    for config in configs.values():
        output_path = os.path.join(output_dir, f"{config.raw_table_id}.yaml")
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                build_raw_table_yaml_dict(config),
                f,
                sort_keys=False,
                allow_unicode=True,
            )


if __name__ == "__main__":
    generate_raw_table_yamls()
