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
"""Temporary Airflow entrypoint that re-keys this DAG's legacy tables to CMEK.

Part of the CJIS migration (Recidiviz/zenhub-tasks#2606). The DAG wires this
task upstream of the pipeline through a barrier with trigger_rule=ALL_DONE:
the pipeline waits for this task to FINISH (succeed or fail) before any of
its own writes start, so a copy can never race a load job — but a failed
re-key still cannot block the pipeline.

Because the pipeline waits, each run works within a fixed time budget and
stops when it is spent; already-keyed tables are skipped on later runs, so
the backlog converges run by run. Once converged, the task costs one
metadata read per table.

A content mismatch (CRITICAL_MISMATCH) raises so the task turns red — with
nothing blocked downstream, a red task is pure signal. Every other error is
logged and swallowed.

No-ops outside recidiviz-staging, and only touches the datasets in
_PILOT_DATASET_ALLOWLIST until that allowlist is widened away. Delete this
entrypoint, its DAG tasks, and its registration once every table carries the
key.
"""
import argparse
import logging
import time

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from recidiviz.big_query.rekey_tables_to_cmek import (
    BACKUP_SUFFIX,
    RekeyAction,
    RekeyResult,
    recently_streamed_tables,
    rekey_table,
)
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING

_STAGING_KMS_KEY = (
    "projects/cmek-82ade411-5705-4461-b2cb-9/locations/us"
    "/keyRings/data-cjis/cryptoKeys/recidiviz-staging-bq-default"
)

_SCOPE_RAW_DATA = "raw_data"
_SCOPE_CALCULATION_OUTPUTS = "calculation_outputs"

# The pipeline waits (without depending on success) for this task, so each
# run's re-key work is bounded. Interrupted sweeps converge across runs.
_RUN_TIME_BUDGET_SECONDS = 15 * 60

# Pilot rollout: while this allowlist is set, only these datasets are
# re-keyed. us_oz is the synthetic demo state, so its raw tables exercise the
# real pipeline path on fake data. Widen with one-line PRs as runs verify
# cleanly; set to None to cover every dataset in scope.
_PILOT_DATASET_ALLOWLIST: frozenset[str] | None = frozenset(
    {
        "us_oz_raw_data",
        "us_oz_raw_data_secondary",
        "supplemental_data",
    }
)


def _datasets_for_scope(scope: str) -> list[str]:
    if scope == _SCOPE_RAW_DATA:
        return [
            raw_tables_dataset_for_region(state_code, instance)
            for state_code in get_direct_ingest_states_existing_in_env()
            for instance in DirectIngestInstance
        ]
    if scope == _SCOPE_CALCULATION_OUTPUTS:
        return [DATAFLOW_METRICS_DATASET, SUPPLEMENTAL_DATA_DATASET]
    raise ValueError(f"Unknown scope: [{scope}]")


def _pilot_filtered(datasets: list[str]) -> list[str]:
    if _PILOT_DATASET_ALLOWLIST is None:
        return datasets
    return [d for d in datasets if d in _PILOT_DATASET_ALLOWLIST]


def _rekey_datasets(scope: str) -> list[RekeyResult]:
    """Re-keys as many of the scope's tables as fit in the run budget,
    stopping immediately on a content mismatch."""
    project_id = metadata.project_id()
    if project_id != GCP_PROJECT_STAGING:
        logging.info(
            "Skipping CMEK re-key: only [%s] is migrating (this is [%s]).",
            GCP_PROJECT_STAGING,
            project_id,
        )
        return []

    client = bigquery.Client(project=project_id)
    targets: list[tuple[str, str]] = []
    for dataset_id in _pilot_filtered(_datasets_for_scope(scope)):
        try:
            targets.extend(
                (dataset_id, t.table_id)
                for t in client.list_tables(dataset_id)
                if t.table_type == "TABLE" and not t.table_id.endswith(BACKUP_SUFFIX)
            )
        except NotFound:
            continue

    streamed_tables = recently_streamed_tables(
        client=client, project_id=project_id, bq_region="us"
    )
    started = time.time()
    results: list[RekeyResult] = []
    for index, (dataset_id, table_id) in enumerate(targets):
        if time.time() - started > _RUN_TIME_BUDGET_SECONDS:
            logging.info(
                "Run budget spent with [%s] of [%s] tables left; the next run "
                "continues where this one stopped (keyed tables are skipped).",
                len(targets) - index,
                len(targets),
            )
            break
        try:
            table_result = rekey_table(
                client=client,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                kms_key=_STAGING_KMS_KEY,
                apply=True,
                # The barrier orders this task before the DAG's own writes;
                # 2h still covers the previous run's streaming-buffer drain.
                min_quiet_hours=2,
                streamed_tables=streamed_tables,
                checksum_max_gb=100,
                keep_backups=False,
            )
        except Exception as e:  # pylint: disable=broad-except
            table_result = RekeyResult(
                dataset_id=dataset_id,
                table_id=table_id,
                action=RekeyAction.FAILED,
                detail=f"{type(e).__name__}: {e}"[:300],
            )
        results.append(table_result)
        logging.info("%s", table_result.to_line())
        if table_result.action == RekeyAction.CRITICAL_MISMATCH:
            break
    return results


class RekeyTablesToCmekEntrypoint(EntrypointInterface):
    """Temporary entrypoint that re-keys legacy tables to CMEK, ordered ahead
    of its DAG's writes through an ALL_DONE barrier."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--scope",
            required=True,
            choices=[_SCOPE_RAW_DATA, _SCOPE_CALCULATION_OUTPUTS],
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        try:
            results = _rekey_datasets(args.scope)
        except Exception:  # pylint: disable=broad-except
            # Unexpected failures must not block the pipeline this task
            # fronts; the engine already kept any backups it made.
            logging.exception("CMEK re-key failed; continuing the DAG.")
            return
        mismatches = [r for r in results if r.action == RekeyAction.CRITICAL_MISMATCH]
        if mismatches:
            # A red task is pure signal here: nothing downstream depends on
            # this task succeeding, so raising blocks nothing.
            first = mismatches[0]
            raise RuntimeError(
                f"CMEK re-key found a content mismatch on "
                f"[{first.dataset_id}.{first.table_id}]: {first.detail}"
            )
