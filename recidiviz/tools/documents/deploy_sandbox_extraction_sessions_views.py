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
"""Deploys cross-state UNION ALL extraction results views and LLM sessions views
to sandbox datasets in a single pass.

Must be run after all extractor VIEW_DEPLOY phases have completed, so that the
state-specific extraction results views exist in the sandbox before the union-all
views are built on top of them.

Usage (all collections for a state):
    python -m recidiviz.tools.documents.deploy_sandbox_extraction_sessions_views \\
        --project_id recidiviz-staging \\
        --sandbox_dataset_prefix my_prefix \\
        --state_code US_CO

Usage (specific collections only):
    python -m recidiviz.tools.documents.deploy_sandbox_extraction_sessions_views \\
        --project_id recidiviz-staging \\
        --sandbox_dataset_prefix my_prefix \\
        --state_code US_CO \\
        --collections CASE_NOTE_EMPLOYMENT_INFO CASE_NOTE_HOUSING_INFO
"""
import argparse
import logging
import sys

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
    load_first_order_llm_extractor_configs,
)
from recidiviz.tools.documents.sandbox_extraction_bq_helpers import (
    deploy_extraction_downstream_state_agnostic_views,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DEFAULT_TABLE_EXPIRATION_DAYS = 30


def main(
    *,
    project_id: str,
    sandbox_dataset_prefix: str,
    state_code: StateCode,
    collections: list[str] | None,
    table_expiration_days: int,
) -> None:
    """Deploys union-all extraction results views and LLM sessions views to sandbox."""
    table_expiration_ms = table_expiration_days * 24 * 60 * 60 * 1000
    first_order_configs = (
        [
            get_first_order_llm_extractor_config(state_code, collection)
            for collection in collections
        ]
        if collections is not None
        else list(load_first_order_llm_extractor_configs().get(state_code, {}).values())
    )
    with local_project_id_override(project_id):
        logging.info(
            "Deploying union-all extraction results views and LLM sessions views "
            "to %s...",
            sandbox_dataset_prefix,
        )
        deploy_extraction_downstream_state_agnostic_views(
            sandbox_dataset_prefix=sandbox_dataset_prefix,
            table_expiration_ms=table_expiration_ms,
            first_order_configs=first_order_configs,
        )
        logging.info("Extraction sessions views deployed successfully.")


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Deploy cross-state UNION ALL extraction results views and LLM "
            "sessions views to sandbox datasets."
        )
    )
    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--sandbox_dataset_prefix",
        type=str,
        required=True,
        help="Prefix for sandbox BQ datasets.",
    )
    parser.add_argument(
        "--state_code",
        type=str,
        required=True,
        help="State code whose extraction results to union (e.g. US_CO).",
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        default=None,
        help=(
            "Extractor collection names to deploy (e.g. CASE_NOTE_EMPLOYMENT_INFO). "
            "When omitted, all collections for the state are deployed."
        ),
    )
    parser.add_argument(
        "--table_expiration_days",
        type=int,
        default=_DEFAULT_TABLE_EXPIRATION_DAYS,
        help=f"Days until sandbox tables expire (default: {_DEFAULT_TABLE_EXPIRATION_DAYS}).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])
    main(
        project_id=args.project_id,
        sandbox_dataset_prefix=args.sandbox_dataset_prefix,
        state_code=StateCode(args.state_code),
        collections=args.collections,
        table_expiration_days=args.table_expiration_days,
    )
