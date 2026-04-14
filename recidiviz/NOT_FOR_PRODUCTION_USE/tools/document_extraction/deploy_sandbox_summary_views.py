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
"""Deploys LLM sessions and current summary views to a sandbox dataset.

These views are derived from document extraction results (employment info,
housing info) and include:
  - employment_status_llm_sessions
  - employer_llm_sessions
  - current_employment_status_summary
  - housing_status_llm_sessions
  - current_housing_status_summary

Must be run after first-order extraction jobs have completed and their
extraction result views have been deployed to the sandbox.

Usage:
    python -m recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.deploy_sandbox_summary_views \
        --project_id recidiviz-staging \
        --sandbox_dataset_prefix my_prefix
"""
import argparse
import logging
import sys

from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.views.view_config import (
    get_document_extraction_current_summary_view_builders,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_document_extraction_job import (
    SANDBOX_DATASET_EXPIRATION_MS,
    _build_not_for_prod_address_overrides,
)
from recidiviz.tools.load_views_to_sandbox import load_collected_views_to_sandbox
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def main(
    *, project_id: str, sandbox_dataset_prefix: str, collection_name: str | None = None
) -> None:
    """Deploys LLM sessions and current summary views to sandbox.

    If collection_name is provided, only deploys views for that collection
    (e.g. 'CASE_NOTE_EMPLOYMENT_INFO'). Otherwise deploys all collections.
    """
    with local_project_id_override(project_id):
        all_builders = get_document_extraction_current_summary_view_builders()
        if collection_name is not None:
            builders = [b for b in all_builders if collection_name.lower() in b.view_id]
        else:
            builders = all_builders

        if not builders:
            logging.warning("No summary view builders found — nothing to deploy.")
            return

        logging.info("Deploying %d summary view builders to sandbox...", len(builders))
        for b in builders:
            logging.info("  %s.%s", b.dataset_id, b.view_id)

        input_source_table_overrides = _build_not_for_prod_address_overrides(
            sandbox_dataset_prefix
        )

        load_collected_views_to_sandbox(
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            state_code_filter=None,
            collected_builders=builders,
            input_source_table_dataset_overrides_dict=None,
            input_source_table_overrides=input_source_table_overrides,
            allow_slow_views=True,
            rematerialize_changed_views_only=False,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_FAST,
            schemas_only=False,
            default_table_expiration_ms=SANDBOX_DATASET_EXPIRATION_MS,
        )

        logging.info("Summary views deployed successfully.")


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy LLM sessions and current summary views to a sandbox dataset."
    )
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
        help="The GCP project ID to use.",
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=True,
        help="Prefix for sandbox BQ datasets.",
    )
    parser.add_argument(
        "--collection_name",
        dest="collection_name",
        type=str,
        default=None,
        help=(
            "If provided, only deploys summary views for this collection "
            "(e.g. CASE_NOTE_EMPLOYMENT_INFO). Deploys all collections if omitted."
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args = parse_arguments(sys.argv[1:])
    main(
        project_id=known_args.project_id,
        sandbox_dataset_prefix=known_args.sandbox_dataset_prefix,
        collection_name=known_args.collection_name,
    )
