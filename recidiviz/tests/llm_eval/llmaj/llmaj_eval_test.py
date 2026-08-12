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
"""Tests for LLMAJEvalConfig parsing and the scores_parsed view builder."""
import os
import textwrap
from unittest import TestCase
from unittest.mock import patch

from google.cloud import bigquery

from recidiviz.llm_eval.llmaj.llmaj_eval_config import (
    LLMAJEvalConfig,
    LLMAJMetadataField,
    LLMAJScoreField,
    collect_llmaj_eval_configs,
)
from recidiviz.llm_eval.llmaj.views.scores_parsed import (
    build_scores_parsed_view_builder,
)
from recidiviz.tests.big_query.sqlglot_helpers import check_query_selects_output_columns
from recidiviz.tests.ingest import fixtures

_FIXTURE = fixtures.as_filepath


def _minimal_config(
    *,
    metadata_fields: list[LLMAJMetadataField] | None = None,
    scores_fields: list[LLMAJScoreField] | None = None,
) -> LLMAJEvalConfig:
    """Returns a minimal valid LLMAJEvalConfig for use in view-builder tests."""
    return LLMAJEvalConfig(
        task_name="test_eval",
        description="A test eval.",
        source_dataset="test_dataset",
        source_table="TestEvaluationRun",
        metadata_fields=metadata_fields
        or [
            LLMAJMetadataField(
                column_name="id",
                source_column_name="id",
                bq_type=bigquery.StandardSqlTypeNames.STRING,
                description="Run ID.",
            ),
        ],
        scores_fields=scores_fields
        or [
            LLMAJScoreField(
                column_name="overall_grade",
                json_path="$.overall.grade",
                bq_type=bigquery.StandardSqlTypeNames.STRING,
                description="Overall grade.",
                bq_mode="NULLABLE",
            ),
        ],
    )


class TestLLMAJEvalConfigParsing(TestCase):
    """Tests for LLMAJEvalConfig.from_yaml."""

    def test_parse_valid_config_fixture(self) -> None:
        config = LLMAJEvalConfig.from_yaml(_FIXTURE("valid_config.yaml"))

        self.assertEqual(
            config,
            LLMAJEvalConfig(
                task_name="test_eval",
                description="A test LLMAJ evaluation task.",
                source_dataset="test_dataset",
                source_table="TestEvaluationRun",
                metadata_fields=[
                    LLMAJMetadataField(
                        column_name="id",
                        source_column_name="id",
                        bq_type=bigquery.StandardSqlTypeNames.STRING,
                        description="Unique identifier.",
                    ),
                    LLMAJMetadataField(
                        column_name="created_at",
                        source_column_name="createdAt",
                        bq_type=bigquery.StandardSqlTypeNames.TIMESTAMP,
                        description="Timestamp when the run was created.",
                    ),
                ],
                scores_fields=[
                    LLMAJScoreField(
                        column_name="overall_grade",
                        json_path="$.overall.grade",
                        bq_type=bigquery.StandardSqlTypeNames.STRING,
                        description="Overall grade (GOOD/PARTIAL/BAD).",
                        bq_mode="NULLABLE",
                    ),
                    LLMAJScoreField(
                        column_name="overall_score",
                        json_path="$.overall.score",
                        bq_type=bigquery.StandardSqlTypeNames.FLOAT64,
                        description="Numeric overall score.",
                        bq_mode="NULLABLE",
                    ),
                    LLMAJScoreField(
                        column_name="overall_omissions",
                        json_path="$.overall.omissions",
                        bq_type=bigquery.StandardSqlTypeNames.STRING,
                        description="JSON array of omissions.",
                        bq_mode="REPEATED",
                    ),
                ],
            ),
        )

    def test_parse_bad_bq_type(self) -> None:
        with self.assertRaisesRegex(ValueError, r"NOT_A_VALID_TYPE"):
            LLMAJEvalConfig.from_yaml(_FIXTURE("bad_bq_type.yaml"))

    def test_parse_missing_required_key(self) -> None:
        with self.assertRaisesRegex(KeyError, r"json_path"):
            LLMAJEvalConfig.from_yaml(_FIXTURE("missing_required_key.yaml"))

    def test_parse_unexpected_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Unexpected keys in LLMAJ eval config \[unexpected_key\]"
        ):
            LLMAJEvalConfig.from_yaml(_FIXTURE("unexpected_key.yaml"))

    def test_load_all_real_configs(self) -> None:
        configs = collect_llmaj_eval_configs()
        self.assertGreater(len(configs), 0)

    def test_collect_raises_on_duplicate_task_name(self) -> None:
        fixture_dir = os.path.dirname(_FIXTURE("valid_config.yaml"))
        with patch(
            "recidiviz.llm_eval.llmaj.llmaj_eval_config._CONFIGS_DIR", fixture_dir
        ):
            with patch("os.scandir") as mock_scandir:
                entry = type(
                    "Entry",
                    (),
                    {
                        "name": "valid_config.yaml",
                        "path": _FIXTURE("valid_config.yaml"),
                    },
                )()
                mock_scandir.return_value = [entry, entry]
                with self.assertRaisesRegex(
                    ValueError, r"Duplicate task name \[test_eval\]"
                ):
                    collect_llmaj_eval_configs()


class TestLLMAJScoreFieldAttrsPostInit(TestCase):
    """Tests for LLMAJScoreField.__attrs_post_init__ invariants."""

    def test_repeated_mode_with_non_string_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Score field \[my_field\] has bq_mode='REPEATED' but bq_type \[FLOAT64\]",
        ):
            LLMAJScoreField(
                column_name="my_field",
                json_path="$.path",
                bq_type=bigquery.StandardSqlTypeNames.FLOAT64,
                description="A field.",
                bq_mode="REPEATED",
            )

    def test_repeated_mode_with_string_type_is_valid(self) -> None:
        field = LLMAJScoreField(
            column_name="my_field",
            json_path="$.path",
            bq_type=bigquery.StandardSqlTypeNames.STRING,
            description="A field.",
            bq_mode="REPEATED",
        )
        self.assertEqual(field.bq_mode, "REPEATED")


class TestScoresParsedViewBuilder(TestCase):
    """Tests for build_scores_parsed_view_builder SQL generation."""

    def test_string_scalar_field_uses_json_value(self) -> None:
        config = _minimal_config(
            scores_fields=[
                LLMAJScoreField(
                    column_name="overall_grade",
                    json_path="$.overall.grade",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="Grade.",
                    bq_mode="NULLABLE",
                ),
            ]
        )
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn("JSON_VALUE(scores, '$.overall.grade') AS overall_grade", sql)
        self.assertNotIn("JSON_VALUE_ARRAY", sql)
        self.assertNotIn("CAST", sql)

    def test_non_string_scalar_field_uses_cast_json_value(self) -> None:
        config = _minimal_config(
            scores_fields=[
                LLMAJScoreField(
                    column_name="overall_score",
                    json_path="$.overall.score",
                    bq_type=bigquery.StandardSqlTypeNames.FLOAT64,
                    description="Score.",
                    bq_mode="NULLABLE",
                ),
            ]
        )
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn(
            "CAST(JSON_VALUE(scores, '$.overall.score') AS FLOAT64) AS overall_score",
            sql,
        )

    def test_repeated_field_uses_json_value_array(self) -> None:
        config = _minimal_config(
            scores_fields=[
                LLMAJScoreField(
                    column_name="overall_omissions",
                    json_path="$.overall.omissions",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="Omissions.",
                    bq_mode="REPEATED",
                ),
            ]
        )
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn(
            "JSON_VALUE_ARRAY(scores, '$.overall.omissions') AS overall_omissions", sql
        )
        self.assertNotIn("JSON_VALUE(", sql)

    def test_metadata_field_with_alias(self) -> None:
        config = _minimal_config(
            metadata_fields=[
                LLMAJMetadataField(
                    column_name="created_at",
                    source_column_name="createdAt",
                    bq_type=bigquery.StandardSqlTypeNames.TIMESTAMP,
                    description="Timestamp.",
                ),
            ]
        )
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn("createdAt AS created_at", sql)

    def test_metadata_field_without_alias(self) -> None:
        config = _minimal_config(
            metadata_fields=[
                LLMAJMetadataField(
                    column_name="id",
                    source_column_name="id",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="ID.",
                ),
            ]
        )
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn("  id,\n", sql)
        self.assertNotIn("id AS id", sql)

    def test_from_clause_references_correct_source(self) -> None:
        config = _minimal_config()
        sql = build_scores_parsed_view_builder(config).view_query_template
        self.assertIn("  state_code,\n", sql)
        self.assertIn("FROM `{project_id}.test_dataset.TestEvaluationRun`", sql)

    def test_sql_columns_match_declared_schema(self) -> None:
        config = _minimal_config(
            metadata_fields=[
                LLMAJMetadataField(
                    column_name="id",
                    source_column_name="id",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="ID.",
                ),
                LLMAJMetadataField(
                    column_name="created_at",
                    source_column_name="createdAt",
                    bq_type=bigquery.StandardSqlTypeNames.TIMESTAMP,
                    description="Timestamp.",
                ),
            ],
            scores_fields=[
                LLMAJScoreField(
                    column_name="overall_grade",
                    json_path="$.overall.grade",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="Grade.",
                    bq_mode="NULLABLE",
                ),
                LLMAJScoreField(
                    column_name="overall_omissions",
                    json_path="$.overall.omissions",
                    bq_type=bigquery.StandardSqlTypeNames.STRING,
                    description="Omissions.",
                    bq_mode="REPEATED",
                ),
            ],
        )
        builder = build_scores_parsed_view_builder(config)
        declared_columns = {col.name for col in builder.schema}
        check_query_selects_output_columns(
            builder.view_query_template.replace("{project_id}", "recidiviz-123"),
            declared_columns,
        )

    def test_snapshot_notetaking_evaluation_sql(self) -> None:
        configs = collect_llmaj_eval_configs()
        config = configs["notetaking_evaluation"]
        builder = build_scores_parsed_view_builder(config)
        expected_sql = textwrap.dedent(
            """\
            SELECT
              state_code,
              id,
              createdAt AS created_at,
              pipelineRunId AS pipeline_run_id,
              evaluatorVersion AS evaluator_version,
              langsmithTraceId AS langsmith_trace_id,
              JSON_VALUE(scores, '$.overall.grade') AS overall_grade,
              JSON_VALUE(scores, '$.overall.rationale') AS overall_rationale,
              JSON_VALUE_ARRAY(scores, '$.overall.omissions') AS overall_omissions,
              JSON_VALUE_ARRAY(scores, '$.overall.hallucinations') AS overall_hallucinations,
              JSON_VALUE(scores, '$.caseNote.grade') AS case_note_grade,
              JSON_VALUE(scores, '$.caseNote.rationale') AS case_note_rationale,
              JSON_VALUE_ARRAY(scores, '$.caseNote.omissions') AS case_note_omissions,
              JSON_VALUE_ARRAY(scores, '$.caseNote.hallucinations') AS case_note_hallucinations,
              JSON_VALUE(scores, '$.actionItems.grade') AS action_items_grade,
              JSON_VALUE(scores, '$.actionItems.rationale') AS action_items_rationale,
              JSON_VALUE_ARRAY(scores, '$.actionItems.omissions') AS action_items_omissions,
              JSON_VALUE_ARRAY(scores, '$.actionItems.hallucinations') AS action_items_hallucinations,
              JSON_VALUE(scores, '$.transcriptComparison.winner') AS transcript_comparison_winner,
              JSON_VALUE(scores, '$.transcriptComparison.rationale') AS transcript_comparison_rationale,
              JSON_VALUE(scores, '$.transcriptComparison.deepgramGrade') AS transcript_comparison_deepgram_grade,
              JSON_VALUE(scores, '$.transcriptComparison.assemblyAiGrade') AS transcript_comparison_assembly_ai_grade
            FROM `{project_id}.meetings_dashboards_db_export.NotetakingEvaluationRun_materialized`"""
        )
        self.assertEqual(builder.view_query_template, expected_sql)
