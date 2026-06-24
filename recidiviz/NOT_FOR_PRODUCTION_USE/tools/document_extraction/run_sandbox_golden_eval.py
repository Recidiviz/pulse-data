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
"""Runs the golden eval pipeline for a single extractor in a sandbox.

Deploys the collection's eval Google Sheet as a BQ external table, runs the
extractor's LLM concurrently against each labeled document, and prints a
per-field accuracy report comparing predicted values to the golden labels.

Requires Application Default Credentials: gcloud auth application-default login

Usage:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_golden_eval \\
        --project_id recidiviz-staging \\
        --extractor_id US_IX_CASE_NOTE_EMPLOYMENT_INFO \\
        --sandbox_dataset_prefix mayukas \\
        --sandbox_documents_bucket recidiviz-staging-anna-scratch
"""
import argparse
import logging
import sys

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_extractor_configs import (
    collect_extractor_collection_eval_configs,
    collect_extractors,
    get_extractor,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.eval.golden_eval_runner import (
    GoldenEvalRunner,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def main(
    *,
    extractor_id: str,
    sandbox_dataset_prefix: str,
    sandbox_documents_bucket: str,
    batch_size: int,
    show_mismatches: bool,
) -> None:
    """Runs the golden eval pipeline for the given extractor in a sandbox."""
    extractor = get_extractor(extractor_id)
    if not isinstance(extractor, LLMPromptExtractorMetadata):
        raise ValueError(
            f"Extractor '{extractor_id}' is not an LLMPromptExtractorMetadata. "
            f"Only LLM prompt extractors support golden eval."
        )

    collection_eval_configs = collect_extractor_collection_eval_configs()
    collection_name = extractor.collection_name
    if collection_name not in collection_eval_configs:
        raise ValueError(
            f"No golden_eval_config.yaml found for collection '{collection_name}'. "
            f"Create one alongside collection.yaml before running eval."
        )

    eval_config = collection_eval_configs[collection_name]
    if eval_config.output_schema is None:
        raise ValueError(
            f"Collection '{collection_name}' has a golden_eval_config.yaml but no "
            f"sibling collection.yaml — cannot derive output schema for scoring."
        )

    runner = GoldenEvalRunner(
        extractor=extractor,
        eval_config=eval_config,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
        sandbox_documents_bucket=sandbox_documents_bucket,
        batch_size=batch_size,
    )

    bq_client = BigQueryClientImpl()
    runner.run(bq_client, show_mismatches=show_mismatches)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses command-line arguments for the golden eval runner."""
    parser = argparse.ArgumentParser(
        description="Run the golden eval pipeline for a single extractor in a sandbox."
    )
    parser.add_argument(
        "--list_extractors",
        action="store_true",
        help="List all extractors that have a golden eval config and exit.",
    )
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=False,
        help="The GCP project ID to use.",
    )
    parser.add_argument(
        "--extractor_id",
        dest="extractor_id",
        type=str,
        required=False,
        help="The extractor ID to evaluate (e.g., US_IX_CASE_NOTE_EMPLOYMENT_INFO).",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=False,
        help="Prefix for the sandbox BQ dataset (e.g., 'mayukas').",
    )
    parser.add_argument(
        "--sandbox_documents_bucket",
        dest="sandbox_documents_bucket",
        type=str,
        required=False,
        help="GCS bucket for sandbox eval documents and batch progress state "
        "(e.g., 'recidiviz-staging-my-scratch').",
    )
    parser.add_argument(
        "--batch_size",
        dest="batch_size",
        type=int,
        default=10,
        help="Number of concurrent LLM requests per batch (default: 10).",
    )
    parser.add_argument(
        "--show_mismatches",
        dest="show_mismatches",
        action="store_true",
        default=False,
        help="After the summary, print every field-level mismatch with expected and actual values.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])

    if args.list_extractors:
        all_extractors = collect_extractors()
        eval_configs = collect_extractor_collection_eval_configs()
        print("Extractors with a golden eval config:")
        for ext_id, ext in sorted(all_extractors.items()):
            if not isinstance(ext, LLMPromptExtractorMetadata):
                continue
            if ext.collection_name not in eval_configs:
                continue
            print(f"  {ext_id}")
            print(f"    State:      {ext.state_code.value}")
            print(f"    Collection: {ext.collection_name}")
        sys.exit(0)

    if not args.project_id:
        print("Error: --project_id is required")
        sys.exit(1)
    if not args.extractor_id:
        print("Error: --extractor_id is required")
        sys.exit(1)
    if not args.sandbox_dataset_prefix:
        print("Error: --sandbox_dataset_prefix is required")
        sys.exit(1)
    if not args.sandbox_documents_bucket:
        print("Error: --sandbox_documents_bucket is required")
        sys.exit(1)

    with local_project_id_override(args.project_id):
        main(
            extractor_id=args.extractor_id,
            sandbox_dataset_prefix=args.sandbox_dataset_prefix,
            sandbox_documents_bucket=args.sandbox_documents_bucket,
            batch_size=args.batch_size,
            show_mismatches=args.show_mismatches,
        )
