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
"""Config class for an LLMAJ (LLM-as-judge) evaluation task, loaded from YAML."""
import os
from pathlib import Path
from typing import Literal, cast

import attr
from google.cloud import bigquery

import recidiviz.llm_eval.llmaj as _llmaj_pkg
from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict

_CONFIGS_DIR = os.path.join(os.path.dirname(_llmaj_pkg.__file__), "config")


@attr.define(frozen=True, kw_only=True)
class LLMAJMetadataField:
    """One non-JSON column passed through from the source table."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name (snake_case)."""

    source_column_name: str = attr.ib(validator=attr_validators.is_str)
    """Column name in the source table. May differ from column_name when the source
    uses camelCase (e.g. 'createdAt' → column_name 'created_at')."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this field."""

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LLMAJMetadataField":
        """Returns an LLMAJMetadataField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        field = cls(
            column_name=column_name,
            source_column_name=d.pop_optional("source_column_name", str) or column_name,
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            description=d.pop("description", str),
        )
        if d:
            raise ValueError(
                f"Unexpected keys in metadata_field [{column_name}]: {repr(d.get())}"
            )
        return field


@attr.define(frozen=True, kw_only=True)
class LLMAJScoreField:
    """One field extracted from the scores JSON column."""

    column_name: str = attr.ib(validator=attr_validators.is_str)
    """Output BQ column name."""

    json_path: str = attr.ib(validator=attr_validators.is_str)
    """JSONPath expression within the scores JSON (e.g. '$.overall.grade')."""

    bq_type: bigquery.StandardSqlTypeNames = attr.ib(
        validator=attr.validators.instance_of(bigquery.StandardSqlTypeNames)
    )
    """BigQuery column type."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of this field."""

    bq_mode: Literal["NULLABLE", "REQUIRED", "REPEATED"] = attr.ib(
        validator=attr_validators.is_str
    )
    """BigQuery column mode. Use 'REPEATED' for array fields (uses JSON_VALUE_ARRAY)
    and 'NULLABLE' for scalar fields (uses JSON_VALUE / CAST(JSON_VALUE(...)))."""

    def __attrs_post_init__(self) -> None:
        if (
            self.bq_mode == "REPEATED"
            and self.bq_type is not bigquery.StandardSqlTypeNames.STRING
        ):
            raise ValueError(
                f"Score field [{self.column_name}] has bq_mode='REPEATED' but "
                f"bq_type [{self.bq_type.value}] — ARRAY fields must use STRING elements."
            )

    @classmethod
    def from_yaml_dict(cls, d: YAMLDict) -> "LLMAJScoreField":
        """Returns an LLMAJScoreField parsed from a YAML dict."""
        column_name = d.pop("column_name", str)
        field = cls(
            column_name=column_name,
            json_path=d.pop("json_path", str),
            bq_type=bigquery.StandardSqlTypeNames(d.pop("bq_type", str)),
            description=d.pop("description", str),
            bq_mode=cast(
                Literal["NULLABLE", "REQUIRED", "REPEATED"],
                d.pop_optional("mode", str) or "NULLABLE",
            ),
        )
        if d:
            raise ValueError(
                f"Unexpected keys in scores_field [{column_name}]: {repr(d.get())}"
            )
        return field


@attr.define(frozen=True, kw_only=True)
class LLMAJEvalConfig:
    """Configuration for one LLMAJ evaluation task."""

    task_name: str = attr.ib(validator=attr_validators.is_str)
    """Unique identifier for the task; used as a BQ view name suffix."""

    description: str = attr.ib(validator=attr_validators.is_str)
    """Human-readable description of what this evaluation measures."""

    source_dataset: str = attr.ib(validator=attr_validators.is_str)
    """BigQuery dataset ID containing the source evaluation run table."""

    source_table: str = attr.ib(validator=attr_validators.is_str)
    """BigQuery table name for the source evaluation run table."""

    state_code: str = attr.ib(validator=attr_validators.is_str)
    """State code for all rows in this evaluation (e.g. 'US_NE')."""

    metadata_fields: list[LLMAJMetadataField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMAJMetadataField),
        ]
    )
    """Non-JSON columns passed through directly from the source table."""

    scores_fields: list[LLMAJScoreField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMAJScoreField),
        ]
    )
    """Fields extracted from the scores JSON column."""

    @property
    def scores_parsed_view_id(self) -> str:
        """Returns the BQ view ID for the parsed scores view."""
        return f"{self.task_name}_scores_parsed"

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "LLMAJEvalConfig":
        """Returns an LLMAJEvalConfig parsed from a YAML file."""
        d = YAMLDict.from_path(yaml_path)
        task_name = d.pop("task_name", str)
        description = d.pop("description", str)
        source_dataset = d.pop("source_dataset", str)
        source_table = d.pop("source_table", str)
        state_code = d.pop("state_code", str)
        metadata_fields = [
            LLMAJMetadataField.from_yaml_dict(fd)
            for fd in d.pop_dicts("metadata_fields")
        ]
        scores_fields = [
            LLMAJScoreField.from_yaml_dict(fd) for fd in d.pop_dicts("scores_fields")
        ]
        if d:
            raise ValueError(
                f"Unexpected keys in LLMAJ eval config [{task_name}]: {repr(d.get())}"
            )
        return cls(
            task_name=task_name,
            description=description,
            source_dataset=source_dataset,
            source_table=source_table,
            state_code=state_code,
            metadata_fields=metadata_fields,
            scores_fields=scores_fields,
        )


def collect_llmaj_eval_configs() -> dict[str, LLMAJEvalConfig]:
    """Returns all LLMAJEvalConfig instances discovered in the configs dir."""
    configs: dict[str, LLMAJEvalConfig] = {}
    for entry in sorted(os.scandir(_CONFIGS_DIR), key=lambda e: e.name):
        if not entry.name.endswith(".yaml"):
            continue
        config = LLMAJEvalConfig.from_yaml(entry.path)
        if config.task_name in configs:
            raise ValueError(
                f"Duplicate task name [{config.task_name}] found in [{entry.path}]"
            )
        configs[config.task_name] = config
    return configs
