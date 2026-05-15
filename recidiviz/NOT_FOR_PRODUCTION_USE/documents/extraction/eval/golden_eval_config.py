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
"""Data classes for the per-collection eval schema configuration.

Each collection has a golden_eval_config.yaml alongside its collection.yaml.  The
eval schema specifies accuracy thresholds, a link to the source eval sheet, and
optional ARRAY_OF_STRUCT field configs.  Everything that can be derived from the
sibling collection.yaml is derived automatically:

  - ``columns``: the full ordered list of eval sheet columns (fixed metadata
    columns bracketing the per-collection ``{field}__expected`` columns).

Minimal golden_eval_config.yaml (no ARRAY_OF_STRUCT fields):

    source_uri: https://docs.google.com/spreadsheets/d/...
    accuracy_threshold: 0.80

With ARRAY_OF_STRUCT fields (primary_key_cols required to match elements):

    source_uri: https://docs.google.com/spreadsheets/d/...
    accuracy_threshold: 0.80
    array_of_struct_field_configs:
      employers:
        primary_key_cols: [employer_name]
      employment_changes:
        primary_key_cols: [employment_change_type, employment_change_date]
"""
import os
from dataclasses import dataclass

from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Bool,
    SqlFieldMode,
    String,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.extraction_output_schema import (
    RESERVED_FIELD_NAME_IS_RELEVANT,
    ExtractionFieldType,
    ExtractionInferredField,
    ExtractionOutputSchema,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extractor_collection_metadata import (
    DocumentExtractorCollectionMetadata,
)
from recidiviz.utils.yaml_dict import YAMLDict

# Fixed columns that appear before the collection-specific expected columns.
_PREFIX_METADATA_COLUMNS: list[BigQueryViewColumn] = [
    String(
        name="test_type",
        description="Test type: 'unit' (document designed to test specific behaviors) or 'sample' (real document randomly sampled from state pool).",
        mode="REQUIRED",
    ),
    String(
        name="document_id",
        description="Stable, human-readable ID assigned by the annotator. Unique within this collection.",
        mode="REQUIRED",
    ),
    String(
        name="state_code",
        description="Two-letter state code, e.g. US_IX or US_TN.",
        mode="REQUIRED",
    ),
    String(
        name="document_text",
        description="Full document text.",
        mode="REQUIRED",
    ),
]

# Fixed columns that appear after the collection-specific expected columns.
_SUFFIX_METADATA_COLUMNS: list[BigQueryViewColumn] = [
    String(
        name="difficulty",
        description="Subjective difficulty rating — easy, medium, or hard.",
        mode="NULLABLE",
    ),
    Bool(
        name="is_validated",
        description="True once the row has been reviewed and confirmed by a second annotator.",
        mode="NULLABLE",
    ),
    String(
        name="test_case",
        description="Short label grouping related rows (e.g. 'negation', 'multi_employer').",
        mode="NULLABLE",
    ),
    String(
        name="test_case_description",
        description="Prose description of the test case pattern being exercised.",
        mode="NULLABLE",
    ),
]

_FIELD_TYPE_TO_BQ_COLUMN_CLASS: dict[ExtractionFieldType, type[String] | type[Bool]] = {
    ExtractionFieldType.STRING: String,
    ExtractionFieldType.ENUM: String,
    ExtractionFieldType.BOOLEAN: Bool,
    ExtractionFieldType.INTEGER: String,
    ExtractionFieldType.FLOAT: String,
    ExtractionFieldType.ARRAY_OF_STRUCT: String,  # JSON stored as STRING in BQ
}


@dataclass(frozen=True)
class ArrayOfStructEvalConfig:
    """Config for evaluating an ARRAY_OF_STRUCT field.

    primary_key_cols specifies which sub-fields are used to match expected vs
    actual array elements before comparing them element-wise.
    """

    field_name: str
    primary_key_cols: list[str]

    def __post_init__(self) -> None:
        if not self.primary_key_cols:
            raise ValueError(
                f"ArrayOfStructEvalConfig for '{self.field_name}' must specify "
                f"at least one primary_key_col."
            )


def _expected_col_description(field_name: str, field: ExtractionInferredField) -> str:
    if field.field_type == ExtractionFieldType.ARRAY_OF_STRUCT:
        return (
            f"Expected value for {field_name} (ARRAY_OF_STRUCT). "
            f"JSON-encoded list of objects."
        )
    return f"Expected value for {field_name} ({field.field_type.value})."


def _derive_columns(
    output_schema: ExtractionOutputSchema | None,
) -> list[BigQueryViewColumn]:
    """Derives the ordered eval sheet column list from the collection output schema."""
    expected_columns: list[BigQueryViewColumn] = []
    if output_schema is not None:
        for f in output_schema.inferred_fields:
            col_class = _FIELD_TYPE_TO_BQ_COLUMN_CLASS[f.field_type]
            # is_relevant is always present and required; all other expected
            # columns are nullable (null when the document is not relevant).
            mode: SqlFieldMode = (
                "REQUIRED" if f.name == RESERVED_FIELD_NAME_IS_RELEVANT else "NULLABLE"
            )
            expected_columns.append(
                col_class(
                    name=f"{f.name}__expected",
                    description=_expected_col_description(f.name, f),
                    mode=mode,
                )
            )
    return [*_PREFIX_METADATA_COLUMNS, *expected_columns, *_SUFFIX_METADATA_COLUMNS]


@dataclass(frozen=True)
class GoldenEvalConfig:
    """Config describing the golden eval set for a collection and how to score it.

    Loaded from golden_eval_config.yaml in the collection's config directory.
    Columns are derived automatically from the sibling collection.yaml.
    Only array_of_struct_field_configs must be specified manually, to tell the
    scorer which sub-fields identify matching elements.
    """

    source_uri: str
    accuracy_threshold: float
    # Ordered eval sheet columns derived from the collection output schema.
    columns: list[BigQueryViewColumn]
    # Keyed by field name. Only ARRAY_OF_STRUCT fields need an entry here.
    array_of_struct_configs: dict[str, ArrayOfStructEvalConfig]

    def __post_init__(self) -> None:
        if self.accuracy_threshold <= 0.0:
            raise ValueError(
                f"accuracy_threshold must be > 0.0, got {self.accuracy_threshold}"
            )

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "GoldenEvalConfig":
        """Loads a GoldenEvalConfig from a golden_eval_config.yaml file.

        Automatically loads the sibling collection.yaml (if present) to derive
        column definitions.
        """
        yaml_dict = YAMLDict.from_path(yaml_path)

        source_uri = yaml_dict.pop("source_uri", str)
        accuracy_threshold = yaml_dict.pop("accuracy_threshold", float)

        collection_yaml_path = os.path.join(
            os.path.dirname(yaml_path), "collection.yaml"
        )
        output_schema: ExtractionOutputSchema | None = None
        if os.path.exists(collection_yaml_path):
            output_schema = DocumentExtractorCollectionMetadata.from_yaml(
                collection_yaml_path
            ).output_schema

        columns = _derive_columns(output_schema)

        array_of_struct_configs: dict[str, ArrayOfStructEvalConfig] = {}
        configs_raw = yaml_dict.pop_dict_optional("array_of_struct_field_configs")
        if configs_raw is not None:
            for fname, fconfig_raw in configs_raw.raw_yaml.items():
                if not isinstance(fconfig_raw, dict):
                    raise ValueError(
                        f"Expected a dict for array_of_struct_field_configs entry "
                        f"'{fname}' in {yaml_path}, got {type(fconfig_raw)}"
                    )
                fconfig_dict = YAMLDict(fconfig_raw)
                primary_key_cols = fconfig_dict.pop_list("primary_key_cols", str)
                array_of_struct_configs[fname] = ArrayOfStructEvalConfig(
                    field_name=fname,
                    primary_key_cols=primary_key_cols,
                )

        return cls(
            source_uri=source_uri,
            accuracy_threshold=accuracy_threshold,
            columns=columns,
            array_of_struct_configs=array_of_struct_configs,
        )


def golden_eval_config_path_for_collection_dir(collection_dir: str) -> str:
    """Returns the path to golden_eval_config.yaml for the given collection directory."""
    return os.path.join(collection_dir, "golden_eval_config.yaml")
