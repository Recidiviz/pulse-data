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
"""Runs one extractor's golden eval against a sandbox, for local prompt iteration.

Creates the sandbox-prefixed golden eval results table, runs the shared eval
runner against it, and prints a per-field and per-test-type accuracy summary. The
production golden eval dataset is deliberately out of reach here — writing to it is
the CI entry point's job.

Requires application default credentials with Google Sheets access:
    gcloud auth login --enable-gdrive-access --update-adc

Usage:
    python -m recidiviz.tools.documents.run_sandbox_golden_eval \\
        --project-id recidiviz-staging \\
        --sandbox-prefix my_prefix \\
        --collection CASE_NOTE_EMPLOYMENT_INFO \\
        --state-code US_CO

Add --persist-results to additionally land each document's raw, validated, and
audit rows in the sandbox extraction result tables, for debugging what the model
actually returned:
    python -m recidiviz.tools.documents.run_sandbox_golden_eval \\
        --project-id recidiviz-staging \\
        --sandbox-prefix my_prefix \\
        --collection CASE_NOTE_EMPLOYMENT_INFO \\
        --state-code US_CO \\
        --persist-results
"""
import argparse
import logging
import sys

import attr

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.common.git import get_normalized_git_username
from recidiviz.documents.extraction.eval.golden_eval_console_summary import (
    render_golden_eval_console_summary,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GoldenEvalResultsBQTable,
)
from recidiviz.documents.extraction.eval.golden_eval_runner import GoldenEvalRunner
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_extraction_results_source_table_collections,
    collect_golden_eval_results_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DEFAULT_TABLE_EXPIRATION_HOURS = 72
"""How long a sandbox eval run's tables live by default — long enough to compare a
run against yesterday's while iterating on a prompt, short enough that abandoned
sandboxes clean themselves up.
"""

_MS_PER_HOUR = 60 * 60 * 1000


def main(
    *,
    state_code: StateCode,
    collection_name: str,
    sandbox_prefix: str,
    persist_results: bool,
    table_expiration_hours: int,
) -> None:
    """Runs golden eval for |state_code|'s |collection_name| extractor against a
    sandbox, and prints its accuracy summary.
    """
    config = get_first_order_llm_extractor_config(state_code, collection_name)
    if config.golden_eval is None:
        raise ValueError(
            f"Extractor [{config.extractor_id}] declares no golden_eval config, so it "
            f"cannot be golden eval'd. Entity-resolution extractors are exempt from "
            f"golden eval."
        )

    bq_client = BigQueryClientImpl()
    update_manager = SourceTableUpdateManager(bq_client)
    # The runner creates no tables, and streams its scored rows as soon as it has
    # them, so every table it could write to must exist before it runs.
    for collection in _sandbox_source_table_collections(
        config=config,
        sandbox_prefix=sandbox_prefix,
        persist_results=persist_results,
        table_expiration_ms=table_expiration_hours * _MS_PER_HOUR,
    ):
        logging.info("Creating sandbox source tables in [%s]...", collection.dataset_id)
        update_manager.update(collection)

    result = GoldenEvalRunner(
        sandbox_prefix=sandbox_prefix,
        requester=get_normalized_git_username(),
        persist_processed_results=persist_results,
    ).run_eval(config=config)

    print(
        render_golden_eval_console_summary(
            config=config,
            result=result,
            results_table_address=GoldenEvalResultsBQTable.address(
                collection_name=collection_name, sandbox_prefix=sandbox_prefix
            ),
        )
    )


def _sandbox_source_table_collections(
    *,
    config: LLMExtractorConfig,
    sandbox_prefix: str,
    persist_results: bool,
    table_expiration_ms: int,
) -> list[SourceTableCollection]:
    """Returns every sandbox source table collection this run writes to: the golden
    eval results table always, plus the extractor's raw, validated, and audit result
    tables when the run is also persisting its processed results.
    """
    collections = [
        golden_eval_sandbox_source_table_collection(
            collection_name=config.extractor_collection.name,
            sandbox_prefix=sandbox_prefix,
            table_expiration_ms=table_expiration_ms,
        )
    ]
    if persist_results:
        collections.extend(
            collection.as_sandbox_collection(
                sandbox_prefix, table_expiration_ms=table_expiration_ms
            )
            for collection in collect_extraction_results_source_table_collections(
                configs={config.state_code: {config.extractor_collection.name: config}}
            )
        )
    return collections


def golden_eval_sandbox_source_table_collection(
    *, collection_name: str, sandbox_prefix: str, table_expiration_ms: int
) -> SourceTableCollection:
    """Returns the sandbox collection holding just |collection_name|'s golden eval
    results table.

    Narrowed to the one table because a run evaluates one extractor: creating every
    collection's table would leave a sandbox dataset whose contents suggest evals
    that never ran.
    """
    collection = collect_golden_eval_results_source_table_collection()
    address = GoldenEvalResultsBQTable.address(collection_name=collection_name)
    if address not in collection.source_tables_by_address:
        raise ValueError(
            f"No golden eval results table for extractor collection "
            f"[{collection_name}]. Known collections: "
            f"{sorted(a.table_id.upper() for a in collection.source_tables_by_address)}."
        )

    return attr.evolve(
        collection,
        source_tables_by_address={
            address: collection.source_tables_by_address[address]
        },
    ).as_sandbox_collection(sandbox_prefix, table_expiration_ms=table_expiration_ms)


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parses the arguments needed to run a sandbox golden eval."""
    parser = argparse.ArgumentParser(
        description=(
            "Run one extractor's golden eval against a sandbox-prefixed dataset."
        )
    )
    parser.add_argument(
        "--project-id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
        help="The GCP project holding the eval sheet, the model, and the sandbox.",
    )
    parser.add_argument(
        "--sandbox-prefix",
        dest="sandbox_prefix",
        type=str,
        required=True,
        help="Prefix for the sandbox datasets this run writes to. Should be your "
        "github username or some personal unique identifier so it's easy for "
        "others to tell who created the dataset.",
    )
    parser.add_argument(
        "--collection",
        dest="collection",
        type=str,
        required=True,
        help="Name of the extractor collection to evaluate, e.g. "
        "CASE_NOTE_EMPLOYMENT_INFO.",
    )
    parser.add_argument(
        "--state-code",
        dest="state_code",
        type=StateCode,
        required=True,
        help="The state whose extractor for this collection is evaluated.",
    )
    parser.add_argument(
        "--persist-results",
        dest="persist_results",
        action="store_true",
        default=False,
        help="Also write each document's raw, validated, and audit rows to the "
        "sandbox extraction result tables, keyed by the run's synthetic job id. A "
        "debugging aid only — these rows are never scored.",
    )
    parser.add_argument(
        "--table-expiration-hours",
        dest="table_expiration_hours",
        type=int,
        default=DEFAULT_TABLE_EXPIRATION_HOURS,
        help=f"How long the sandbox tables this run creates live (default: "
        f"{DEFAULT_TABLE_EXPIRATION_HOURS}).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv[1:])

    with local_project_id_override(args.project_id):
        main(
            state_code=args.state_code,
            collection_name=args.collection,
            sandbox_prefix=args.sandbox_prefix,
            persist_results=args.persist_results,
            table_expiration_hours=args.table_expiration_hours,
        )
