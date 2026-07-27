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
"""Tests for document_collection_config.py."""
import unittest
from pathlib import Path

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import (
    BigQueryFieldMode,
    to_validated_schema_field,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store import yaml_schema
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
    collect_document_collection_config_yaml_paths,
    load_first_order_document_collection_configs,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    PERSON_EXTERNAL_ID_COLUMN_NAME,
    PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
    PERSON_ID_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
    STAFF_EXTERNAL_ID_COLUMN_NAME,
    STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
    STAFF_ID_COLUMN_NAME,
)
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStatePersonExternalId,
    NormalizedStateStaffExternalId,
)
from recidiviz.tests.documents.store import config as fake_config_module
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.utils.yaml_dict_validator import validate_yaml_matches_schema

# A REPEATED RECORD generation-output-only column, for exercising the
# other_document_generation_output_columns handling (the class of column the ER
# composite-document collection uses for its entry_source_map).
_GENERATION_OUTPUT_COLUMN = bigquery.SchemaField(
    "extra_generation_output",
    "RECORD",
    mode="REPEATED",
    fields=[bigquery.SchemaField("value", "STRING")],
)


def _make_config(
    *,
    root_entity_id_type: DocumentRootEntityIdType = DocumentRootEntityIdType.PERSON_ID,
    document_primary_key_columns: list[bigquery.SchemaField] | None = None,
    other_metadata_columns: list[bigquery.SchemaField] | None = None,
    other_document_generation_output_columns: list[bigquery.SchemaField] | None = None,
) -> DocumentCollectionConfig:
    """Builds a minimal valid DocumentCollectionConfig, with the fields relevant to
    these tests overridable. document_primary_key_columns defaults to a single
    note_id column; pass [] explicitly to test the no-document-column case."""
    return DocumentCollectionConfig(
        state_code=StateCode.US_XX,
        name="TEST_COLLECTION",
        description="test document collection",
        root_entity_id_type=root_entity_id_type,
        document_primary_key_columns=(
            [bigquery.SchemaField("note_id", "STRING")]
            if document_primary_key_columns is None
            else document_primary_key_columns
        ),
        other_metadata_columns=other_metadata_columns or [],
        document_generation_query_template="SELECT 1",
        other_document_generation_output_columns=(
            other_document_generation_output_columns or []
        ),
    )


class TestDocumentCollectionConfig(unittest.TestCase):
    """Tests for DocumentCollectionConfig."""

    def test_get_document_collection_config_doesnt_exist(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"No document collection \[non_existent_collection\]"
        ):
            get_document_collection_config(
                StateCode.US_XX, "non_existent_collection", fake_config_module
            )

    def test_get_document_collection_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "FAKE_CASE_NOTES", fake_config_module
        )

        expected_query_template = (
            "SELECT\n"
            "  person_id AS person_external_id,\n"
            "  'US_XX_DOC' AS person_external_id_type,\n"
            "  note_id,\n"
            "  note_type,\n"
            "  note_body AS document_text,\n"
            "  CAST(created_at AS TIMESTAMP) AS document_update_datetime\n"
            "FROM `{project_id}.us_xx_raw_data.fake_notes`\n"
        )
        self.assertEqual(
            DocumentCollectionConfig(
                state_code=StateCode.US_XX,
                name="FAKE_CASE_NOTES",
                description=(
                    "Fake case notes for testing document collection config parsing."
                ),
                root_entity_id_type=DocumentRootEntityIdType.PERSON_EXTERNAL_ID,
                document_primary_key_columns=[
                    to_validated_schema_field(
                        field_name="note_id",
                        field_type=SqlTypeNames.STRING,
                        description="The unique identifier for the case note",
                        mode=BigQueryFieldMode.REQUIRED,
                    ),
                ],
                other_metadata_columns=[
                    to_validated_schema_field(
                        field_name="note_type",
                        field_type=SqlTypeNames.STRING,
                        description="The type of note",
                        mode=BigQueryFieldMode.NULLABLE,
                    ),
                ],
                document_generation_query_template=expected_query_template,
                # Never authored in YAML — always empty for a first-order collection.
                other_document_generation_output_columns=[],
            ),
            config,
        )

    def test_collect_configs(self) -> None:
        configs = load_first_order_document_collection_configs(
            StateCode.US_XX, fake_config_module
        )
        self.assertEqual(
            configs.keys(),
            {
                "FAKE_CASE_NOTES",
                "FAKE_PERSON_ID_NOTES",
                "FAKE_STAFF_ID_REPORTS",
                "FAKE_STAFF_REPORTS",
            },
        )

    def test_get_staff_external_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "FAKE_STAFF_REPORTS", fake_config_module
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(
            pk_col_names,
            ["staff_external_id", "staff_external_id_type", "report_id"],
        )

    def test_get_person_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "FAKE_PERSON_ID_NOTES", fake_config_module
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(pk_col_names, ["person_id", "note_id"])

    def test_get_staff_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "FAKE_STAFF_ID_REPORTS", fake_config_module
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(pk_col_names, ["staff_id"])

    def test_collect_configs_nonexistent_state(self) -> None:
        configs = load_first_order_document_collection_configs(
            StateCode.US_LL, fake_config_module
        )
        self.assertEqual(configs, {})

    def test_validation_duplicate_columns(self) -> None:
        with self.assertRaisesRegex(ValueError, "has duplicate column names"):
            DocumentCollectionConfig(
                state_code=StateCode.US_XX,
                name="TEST_COLLECTION",
                description="test collection for validation",
                root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
                document_primary_key_columns=[
                    bigquery.SchemaField("duplicate_column", "STRING"),
                ],
                other_metadata_columns=[
                    bigquery.SchemaField("duplicate_column", "INTEGER"),
                ],
                document_generation_query_template="SELECT 1",
                other_document_generation_output_columns=[],
            )

    def test_invalid_collection_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            DocumentCollectionConfig(
                state_code=StateCode.US_XX,
                name="bad-name",
                description="test collection with bad name",
                root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
                document_primary_key_columns=[
                    bigquery.SchemaField("pk_col", "STRING"),
                ],
                other_metadata_columns=[],
                document_generation_query_template="SELECT 1",
                other_document_generation_output_columns=[],
            )

    def test_primary_key_columns_derives_root_then_document_columns(self) -> None:
        config = _make_config(
            root_entity_id_type=DocumentRootEntityIdType.PERSON_EXTERNAL_ID,
            document_primary_key_columns=[bigquery.SchemaField("note_id", "STRING")],
        )
        self.assertEqual(
            config.primary_key_column_names,
            ["person_external_id", "person_external_id_type", "note_id"],
        )

    def test_primary_key_columns_with_no_document_columns(self) -> None:
        # An ER composite-document collection has one document per root entity, so
        # its primary key is just the root entity column(s).
        config = _make_config(
            root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
            document_primary_key_columns=[],
        )
        self.assertEqual(config.primary_key_column_names, ["person_id"])

    def test_temp_updates_schema_includes_generation_output_columns(self) -> None:
        config = _make_config(
            root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
            document_primary_key_columns=[bigquery.SchemaField("note_id", "STRING")],
            other_metadata_columns=[bigquery.SchemaField("note_type", "STRING")],
            other_document_generation_output_columns=[_GENERATION_OUTPUT_COLUMN],
        )
        temp_schema = config.build_bq_temp_document_metadata_updates_schema()
        self.assertEqual(
            [field.name for field in temp_schema],
            [
                "person_id",
                "note_id",
                "note_type",
                DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                DOCUMENT_TEXT_COLUMN_NAME,
                DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
                "extra_generation_output",
            ],
        )
        # The generation-output column is carried through unchanged (REPEATED RECORD).
        self.assertIn(_GENERATION_OUTPUT_COLUMN, temp_schema)

    def test_metadata_schema_excludes_generation_output_columns(self) -> None:
        config = _make_config(
            root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
            document_primary_key_columns=[bigquery.SchemaField("note_id", "STRING")],
            other_metadata_columns=[bigquery.SchemaField("note_type", "STRING")],
            other_document_generation_output_columns=[_GENERATION_OUTPUT_COLUMN],
        )
        metadata_schema_names = [
            field.name for field in config.build_bq_metadata_schema()
        ]
        self.assertEqual(
            metadata_schema_names,
            [
                "person_id",
                "note_id",
                "note_type",
                DOCUMENT_CONTENTS_ID_COLUMN_NAME,
                DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
                ROW_CREATE_DATETIME_COLUMN_NAME,
            ],
        )
        self.assertNotIn("extra_generation_output", metadata_schema_names)

    def test_other_document_generation_output_column_names(self) -> None:
        config = _make_config(
            other_document_generation_output_columns=[_GENERATION_OUTPUT_COLUMN],
        )
        self.assertEqual(
            config.other_document_generation_output_column_names,
            ["extra_generation_output"],
        )

    def test_ordinary_config_has_no_generation_output_columns(self) -> None:
        config = _make_config()
        self.assertEqual(config.other_document_generation_output_column_names, [])
        temp_schema_names = [
            field.name
            for field in config.build_bq_temp_document_metadata_updates_schema()
        ]
        metadata_schema_names = [
            field.name for field in config.build_bq_metadata_schema()
        ]
        self.assertNotIn("extra_generation_output", temp_schema_names)
        self.assertNotIn("extra_generation_output", metadata_schema_names)

    def test_validation_duplicate_generation_output_column(self) -> None:
        with self.assertRaisesRegex(ValueError, "has duplicate column names"):
            _make_config(
                other_metadata_columns=[bigquery.SchemaField("shared_name", "STRING")],
                other_document_generation_output_columns=[
                    bigquery.SchemaField("shared_name", "STRING", mode="REPEATED"),
                ],
            )

    def test_validation_document_pk_collides_with_root_entity_column(self) -> None:
        # A document PK column named like the derived root-entity column would make
        # the derived primary_key_columns contain it twice.
        with self.assertRaisesRegex(ValueError, "has duplicate column names"):
            _make_config(
                root_entity_id_type=DocumentRootEntityIdType.PERSON_ID,
                document_primary_key_columns=[
                    bigquery.SchemaField("person_id", "INTEGER"),
                ],
            )

    def test_internal_id_type(self) -> None:
        self.assertEqual(
            {t: t.internal_id_type for t in DocumentRootEntityIdType},
            {
                DocumentRootEntityIdType.PERSON_ID: DocumentRootEntityIdType.PERSON_ID,
                DocumentRootEntityIdType.PERSON_EXTERNAL_ID: DocumentRootEntityIdType.PERSON_ID,
                DocumentRootEntityIdType.STAFF_ID: DocumentRootEntityIdType.STAFF_ID,
                DocumentRootEntityIdType.STAFF_EXTERNAL_ID: DocumentRootEntityIdType.STAFF_ID,
            },
        )

    def test_id_column_name(self) -> None:
        self.assertEqual(
            {t: t.id_column_name for t in DocumentRootEntityIdType},
            {
                DocumentRootEntityIdType.PERSON_ID: PERSON_ID_COLUMN_NAME,
                DocumentRootEntityIdType.PERSON_EXTERNAL_ID: PERSON_EXTERNAL_ID_COLUMN_NAME,
                DocumentRootEntityIdType.STAFF_ID: STAFF_ID_COLUMN_NAME,
                DocumentRootEntityIdType.STAFF_EXTERNAL_ID: STAFF_EXTERNAL_ID_COLUMN_NAME,
            },
        )

    def test_id_type_column_name(self) -> None:
        # Only the external-id types carry a qualifying id_type column; the
        # internal-id types have none.
        self.assertEqual(
            {t: t.id_type_column_name for t in DocumentRootEntityIdType},
            {
                DocumentRootEntityIdType.PERSON_ID: None,
                DocumentRootEntityIdType.PERSON_EXTERNAL_ID: PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
                DocumentRootEntityIdType.STAFF_ID: None,
                DocumentRootEntityIdType.STAFF_EXTERNAL_ID: STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
            },
        )

    def test_id_column_type(self) -> None:
        # Internal ids are INTEGER; external ids are STRING.
        self.assertEqual(
            {t: t.id_column_type for t in DocumentRootEntityIdType},
            {
                DocumentRootEntityIdType.PERSON_ID: SqlTypeNames.INTEGER,
                DocumentRootEntityIdType.PERSON_EXTERNAL_ID: SqlTypeNames.STRING,
                DocumentRootEntityIdType.STAFF_ID: SqlTypeNames.INTEGER,
                DocumentRootEntityIdType.STAFF_EXTERNAL_ID: SqlTypeNames.STRING,
            },
        )

    def test_resolved_internal_id_column_name(self) -> None:
        # External-id types collapse onto their internal id column.
        self.assertEqual(
            {t: t.resolved_internal_id_column_name for t in DocumentRootEntityIdType},
            {
                DocumentRootEntityIdType.PERSON_ID: PERSON_ID_COLUMN_NAME,
                DocumentRootEntityIdType.PERSON_EXTERNAL_ID: PERSON_ID_COLUMN_NAME,
                DocumentRootEntityIdType.STAFF_ID: STAFF_ID_COLUMN_NAME,
                DocumentRootEntityIdType.STAFF_EXTERNAL_ID: STAFF_ID_COLUMN_NAME,
            },
        )

    def test_external_id_entity_cls(self) -> None:
        self.assertEqual(
            NormalizedStatePersonExternalId,
            DocumentRootEntityIdType.PERSON_EXTERNAL_ID.external_id_entity_cls,
        )
        self.assertEqual(
            NormalizedStateStaffExternalId,
            DocumentRootEntityIdType.STAFF_EXTERNAL_ID.external_id_entity_cls,
        )
        # The internal-id types have no external-id table to resolve through.
        for internal_type in (
            DocumentRootEntityIdType.PERSON_ID,
            DocumentRootEntityIdType.STAFF_ID,
        ):
            with self.subTest(root_entity_id_type=internal_type):
                with self.assertRaisesRegex(
                    ValueError, r"has no external-id entity class"
                ):
                    _ = internal_type.external_id_entity_cls

    def test_metadata_table_address(self) -> None:
        config = _make_config()
        self.assertEqual(
            BigQueryAddress.from_str("us_xx_document_store_metadata.test_collection"),
            config.metadata_table_address,
        )

    def test_document_contents_table_address(self) -> None:
        config = _make_config()
        self.assertEqual(
            BigQueryAddress.from_str(
                "us_xx_document_contents.test_collection_document_contents"
            ),
            config.document_contents_table_address,
        )

    def test_temp_document_metadata_updates_table_address(self) -> None:
        config = _make_config()
        self.assertEqual(
            BigQueryAddress.from_str(
                "us_xx_document_store_temp."
                "temp_document_metadata_updates_test_collection_run_1"
            ),
            config.temp_document_metadata_updates_table_address("run-1"),
        )

    def test_temp_new_document_contents_table_address(self) -> None:
        config = _make_config()
        self.assertEqual(
            BigQueryAddress.from_str(
                "us_xx_document_store_temp."
                "temp_new_document_contents_test_collection_run_1"
            ),
            config.temp_new_document_contents_table_address("run-1"),
        )

    def test_all_yaml_configs_conform_to_schema(self) -> None:
        schema_path = Path(yaml_schema.__file__).parent / "schema.json"

        for state_code in StateCode:
            yaml_files = collect_document_collection_config_yaml_paths(state_code)
            yaml_files.extend(
                collect_document_collection_config_yaml_paths(
                    state_code, config_module=fake_config_module
                )
            )

            for yaml_file in yaml_files:
                with self.subTest(yaml_file=yaml_file):
                    validate_yaml_matches_schema(
                        yaml_dict=YAMLDict.from_path(yaml_file),
                        json_schema_path=str(schema_path),
                    )
