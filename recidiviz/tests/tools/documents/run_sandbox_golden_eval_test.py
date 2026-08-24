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
"""Tests for the sandbox golden eval dev CLI."""
import io
from contextlib import redirect_stderr, redirect_stdout
from unittest import TestCase
from unittest.mock import patch

import attr

from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.common.constants.operations.llm_extraction_job import (
    LLMExtractionJobDocumentResultType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_golden_eval_results_dataset,
    document_extraction_raw_results_dataset_for_region,
    document_extraction_validated_results_dataset_for_region,
    document_extraction_validation_audit_dataset_for_region,
)
from recidiviz.documents.extraction.eval.golden_eval_result import (
    GoldenEvalFieldScore,
    GoldenEvalResult,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.llm_client.types import (
    LLMDocumentExtractionTokenCounts,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_golden_eval_results_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.documents import fake_config
from recidiviz.tools.documents.run_sandbox_golden_eval import (
    DEFAULT_TABLE_EXPIRATION_HOURS,
    golden_eval_sandbox_source_table_collection,
    main,
    parse_arguments,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_MODULE = "recidiviz.tools.documents.run_sandbox_golden_eval"

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_SANDBOX_PREFIX = "my_prefix"
_REQUESTER = "test-user"

_ONE_HOUR_MS = 60 * 60 * 1000

_GOLDEN_EVAL_SANDBOX_DATASET = document_extraction_golden_eval_results_dataset(
    _SANDBOX_PREFIX
)
_EXTRACTION_RESULT_SANDBOX_DATASETS = {
    document_extraction_raw_results_dataset_for_region(_STATE_CODE, _SANDBOX_PREFIX),
    document_extraction_validated_results_dataset_for_region(
        _STATE_CODE, _SANDBOX_PREFIX
    ),
    document_extraction_validation_audit_dataset_for_region(
        _STATE_CODE, _SANDBOX_PREFIX
    ),
}


def _fake_config() -> LLMExtractorConfig:
    return get_first_order_llm_extractor_config(
        _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
    )


def _eval_result() -> GoldenEvalResult:
    return GoldenEvalResult(
        field_scores=[
            GoldenEvalFieldScore(
                golden_document_id="doc_1",
                test_type=GoldenEvalTestType.UNIT,
                test_case="base_case",
                field_name="location",
                element_index=None,
                expected_value="Boise",
                actual_value="Boise",
                is_correct=True,
            )
        ],
        actual_llm_result_type_by_document_id={
            "doc_1": LLMExtractionJobDocumentResultType.SUCCESS
        },
        total_token_counts=LLMDocumentExtractionTokenCounts.empty(),
    )


class TestGoldenEvalSandboxSourceTableCollection(TestCase):
    """Tests for the sandbox source table collection the CLI creates before a run.

    Runs against the real production extractor collections, since the point of the
    collection is to narrow those down to the one being evaluated.
    """

    @staticmethod
    def _some_production_collection_name() -> str:
        """Returns the name of one real extractor collection, and asserts there is
        more than one, so a test that the others are filtered out is not vacuous.
        """
        collection = collect_golden_eval_results_source_table_collection()
        table_ids = sorted(table.address.table_id for table in collection.source_tables)
        if len(table_ids) < 2:
            raise ValueError(
                f"Expected more than one extractor collection to filter among, "
                f"found: {table_ids}."
            )
        return table_ids[0].upper()

    def test_holds_only_the_evaluated_collections_table(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            collection_name = self._some_production_collection_name()

            collection = golden_eval_sandbox_source_table_collection(
                collection_name=collection_name,
                sandbox_prefix=_SANDBOX_PREFIX,
                table_expiration_ms=72 * _ONE_HOUR_MS,
            )

            self.assertEqual(_GOLDEN_EVAL_SANDBOX_DATASET, collection.dataset_id)
            self.assertEqual(
                [
                    GoldenEvalResultsBQTable.address(
                        collection_name=collection_name,
                        sandbox_prefix=_SANDBOX_PREFIX,
                    )
                ],
                [table.address for table in collection.source_tables],
            )

    def test_applies_the_requested_expiration(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            collection = golden_eval_sandbox_source_table_collection(
                collection_name=self._some_production_collection_name(),
                sandbox_prefix=_SANDBOX_PREFIX,
                table_expiration_ms=72 * _ONE_HOUR_MS,
            )

            self.assertEqual(72 * _ONE_HOUR_MS, collection.table_expiration_ms)
            self.assertNotEqual(
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS, collection.table_expiration_ms
            )

    def test_unknown_collection_name(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            with self.assertRaisesRegex(
                ValueError,
                r"^No golden eval results table for extractor collection "
                r"\[NOT_A_COLLECTION\]\.",
            ):
                golden_eval_sandbox_source_table_collection(
                    collection_name="NOT_A_COLLECTION",
                    sandbox_prefix=_SANDBOX_PREFIX,
                    table_expiration_ms=72 * _ONE_HOUR_MS,
                )


class TestRunSandboxGoldenEval(TestCase):
    """Tests for the CLI's main()."""

    def setUp(self) -> None:
        self.config = _fake_config()

        self.get_config_patcher = patch(
            f"{_MODULE}.get_first_order_llm_extractor_config",
            return_value=self.config,
        )
        self.mock_get_config = self.get_config_patcher.start()
        self.addCleanup(self.get_config_patcher.stop)

        self.collect_golden_eval_patcher = patch(
            f"{_MODULE}.collect_golden_eval_results_source_table_collection",
            side_effect=lambda: collect_golden_eval_results_source_table_collection(
                config_module=fake_config
            ),
        )
        self.collect_golden_eval_patcher.start()
        self.addCleanup(self.collect_golden_eval_patcher.stop)

        self.bq_client_patcher = patch(f"{_MODULE}.BigQueryClientImpl")
        self.bq_client_patcher.start()
        self.addCleanup(self.bq_client_patcher.stop)

        self.update_manager_patcher = patch(f"{_MODULE}.SourceTableUpdateManager")
        self.mock_update_manager = self.update_manager_patcher.start().return_value
        self.addCleanup(self.update_manager_patcher.stop)

        self.runner_patcher = patch(f"{_MODULE}.GoldenEvalRunner")
        self.mock_runner_class = self.runner_patcher.start()
        self.mock_runner_class.return_value.run_eval.return_value = _eval_result()
        self.addCleanup(self.runner_patcher.stop)

        # The runner's requester reads the local git username; pin it so the
        # tests do not depend on the machine's git config.
        self.username_patcher = patch(
            f"{_MODULE}.get_normalized_git_username", return_value=_REQUESTER
        )
        self.username_patcher.start()
        self.addCleanup(self.username_patcher.stop)

    def _run_main(
        self,
        *,
        persist_results: bool = False,
        table_expiration_hours: int = DEFAULT_TABLE_EXPIRATION_HOURS,
    ) -> str:
        """Runs the CLI and returns everything it printed."""
        console_output = io.StringIO()
        with local_project_id_override(GCP_PROJECT_STAGING):
            with redirect_stdout(console_output):
                main(
                    state_code=_STATE_CODE,
                    collection_name=_COLLECTION_NAME,
                    sandbox_prefix=_SANDBOX_PREFIX,
                    persist_results=persist_results,
                    table_expiration_hours=table_expiration_hours,
                )
        return console_output.getvalue()

    def _updated_datasets(self) -> set[str]:
        """Returns the datasets the run created or updated source tables in."""
        return {
            call.args[0].dataset_id
            for call in self.mock_update_manager.update.call_args_list
        }

    def test_creates_the_sandbox_golden_eval_table(self) -> None:
        self._run_main()

        self.assertEqual({_GOLDEN_EVAL_SANDBOX_DATASET}, self._updated_datasets())
        collection = self.mock_update_manager.update.call_args.args[0]
        self.assertEqual(
            [
                GoldenEvalResultsBQTable.address(
                    collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
                )
            ],
            [table.address for table in collection.source_tables],
        )

    def test_creates_tables_with_the_default_expiration(self) -> None:
        self._run_main()

        collection = self.mock_update_manager.update.call_args.args[0]
        self.assertEqual(
            DEFAULT_TABLE_EXPIRATION_HOURS * _ONE_HOUR_MS,
            collection.table_expiration_ms,
        )

    def test_creates_tables_with_an_overridden_expiration(self) -> None:
        self._run_main(table_expiration_hours=1)

        collection = self.mock_update_manager.update.call_args.args[0]
        self.assertEqual(_ONE_HOUR_MS, collection.table_expiration_ms)

    def test_resolves_the_config_for_the_requested_extractor(self) -> None:
        self._run_main()

        self.mock_get_config.assert_called_once_with(_STATE_CODE, _COLLECTION_NAME)

    def test_runs_the_eval_against_the_sandbox(self) -> None:
        self._run_main()

        self.mock_runner_class.assert_called_once_with(
            sandbox_prefix=_SANDBOX_PREFIX,
            requester=_REQUESTER,
            persist_processed_results=False,
        )
        self.mock_runner_class.return_value.run_eval.assert_called_once_with(
            config=self.config
        )

    def test_creates_the_tables_before_running_the_eval(self) -> None:
        """The runner does not create tables, so a run that streamed its scores
        before the table existed would lose every one of them.
        """
        call_order: list[str] = []

        def _record_update(_collection: SourceTableCollection) -> None:
            call_order.append("update")

        def _record_run_eval(**_kwargs: object) -> GoldenEvalResult:
            call_order.append("run_eval")
            return _eval_result()

        self.mock_update_manager.update.side_effect = _record_update
        self.mock_runner_class.return_value.run_eval.side_effect = _record_run_eval

        self._run_main()

        self.assertEqual(["update", "run_eval"], call_order)

    def test_prints_the_accuracy_summary(self) -> None:
        console_output = self._run_main()

        self.assertIn(
            "Golden eval results for extractor [US_XX_FAKE_EXTRACTOR_COLLECTION]",
            console_output,
        )
        self.assertIn("Accuracy by test type:", console_output)
        self.assertIn("Accuracy by field:", console_output)
        self.assertIn("Document outcomes:", console_output)
        self.assertIn(
            GoldenEvalResultsBQTable.address(
                collection_name=_COLLECTION_NAME, sandbox_prefix=_SANDBOX_PREFIX
            ).to_str(),
            console_output,
        )

    def test_does_not_create_result_tables_without_persist_results(self) -> None:
        self._run_main()

        self.assertEqual({_GOLDEN_EVAL_SANDBOX_DATASET}, self._updated_datasets())

    def test_persist_results_creates_the_sandbox_result_tables(self) -> None:
        self._run_main(persist_results=True)

        self.assertEqual(
            {_GOLDEN_EVAL_SANDBOX_DATASET, *_EXTRACTION_RESULT_SANDBOX_DATASETS},
            self._updated_datasets(),
        )

    def test_persist_results_routes_the_runners_writes_to_the_sandbox(self) -> None:
        self._run_main(persist_results=True)

        self.mock_runner_class.assert_called_once_with(
            sandbox_prefix=_SANDBOX_PREFIX,
            requester=_REQUESTER,
            persist_processed_results=True,
        )

    def test_refuses_an_extractor_with_no_golden_eval_config(self) -> None:
        """An extractor with no eval set fails before a single sandbox table is
        created, rather than after.
        """
        self.mock_get_config.return_value = attr.evolve(self.config, golden_eval=None)

        with self.assertRaisesRegex(
            ValueError,
            r"^Extractor \[US_XX_FAKE_EXTRACTOR_COLLECTION\] declares no golden_eval "
            r"config",
        ):
            self._run_main()

        self.mock_update_manager.update.assert_not_called()
        self.mock_runner_class.assert_not_called()


class TestParseArguments(TestCase):
    """Tests for the CLI's argument parsing."""

    def test_parses_required_arguments(self) -> None:
        args = parse_arguments(
            [
                "--project-id",
                GCP_PROJECT_STAGING,
                "--sandbox-prefix",
                _SANDBOX_PREFIX,
                "--collection",
                _COLLECTION_NAME,
                "--state-code",
                "US_XX",
            ]
        )

        self.assertEqual(GCP_PROJECT_STAGING, args.project_id)
        self.assertEqual(_SANDBOX_PREFIX, args.sandbox_prefix)
        self.assertEqual(_COLLECTION_NAME, args.collection)
        self.assertEqual(StateCode.US_XX, args.state_code)
        self.assertFalse(args.persist_results)
        self.assertEqual(DEFAULT_TABLE_EXPIRATION_HOURS, args.table_expiration_hours)

    def test_parses_optional_arguments(self) -> None:
        args = parse_arguments(
            [
                "--project-id",
                GCP_PROJECT_STAGING,
                "--sandbox-prefix",
                _SANDBOX_PREFIX,
                "--collection",
                _COLLECTION_NAME,
                "--state-code",
                "US_XX",
                "--persist-results",
                "--table-expiration-hours",
                "24",
            ]
        )

        self.assertTrue(args.persist_results)
        self.assertEqual(24, args.table_expiration_hours)

    def test_requires_a_sandbox_prefix(self) -> None:
        """The CLI has no production mode: golden eval against the production table
        is the CI entry point's job.
        """
        with self.assertRaises(SystemExit):
            with redirect_stderr(io.StringIO()):
                parse_arguments(
                    [
                        "--project-id",
                        GCP_PROJECT_STAGING,
                        "--collection",
                        _COLLECTION_NAME,
                        "--state-code",
                        "US_XX",
                    ]
                )
