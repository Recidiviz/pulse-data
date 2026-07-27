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
"""Entrypoint for scheduled eOMIS writeback runs.

The unattended counterpart to the attended CLIs in recidiviz/tools/eomis/:
no confirmation prompts and plain logging, with the same flow, runner, and
safety guardrails. Dry-run is the default; --commit performs real writes, and
committing against the production eOMIS instance additionally requires
--allow-prod-write, mirroring the CLI's guard.

Run as a module (this is how the Cloud Run job invokes it):
python -m recidiviz.entrypoints.eomis_writeback --flow ar_ged

Running locally:
IS_DEV=true GOOGLE_CLOUD_PROJECT=recidiviz-staging python \
    -m recidiviz.entrypoints.eomis_writeback --flow ar_ged
"""
import argparse
import logging

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.eomis.client import EomisClient
from recidiviz.eomis.credentials import resolve_eomis_credentials
from recidiviz.eomis.flow import EomisWritebackFlow, ResultStatus
from recidiviz.eomis.runner import LoggingAuditRecorder, run_writeback
from recidiviz.eomis.scheduled_flows import SCHEDULED_FLOWS, ScheduledFlow
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import set_development_project_id_override

FLOW_NAMES = list(SCHEDULED_FLOWS)

# Matches the attended CLI's default pacing between candidates.
PAUSE_MIN_SECONDS = 1.0
PAUSE_MAX_SECONDS = 2.0


def scheduled_flow_for_name(flow_name: str) -> ScheduledFlow:
    """Returns the registered scheduling config for |flow_name|."""
    if flow_name not in SCHEDULED_FLOWS:
        raise ValueError(f"Unexpected flow name: [{flow_name}]")
    return SCHEDULED_FLOWS[flow_name]


def build_flow(flow_name: str, *, bq_view: str | None) -> EomisWritebackFlow:
    """Returns the named writeback flow, configured for an unattended
    (bq-sourced, unlimited) run."""
    return scheduled_flow_for_name(flow_name).build(
        bq_view=bq_view, project_id=metadata.project_id()
    )


def base_url_for_project(flow_name: str, project_id: str) -> str:
    """Returns the eOMIS instance the given project may write to for this flow:
    the production instance only from the production project, the test instance
    everywhere else."""
    return scheduled_flow_for_name(flow_name).base_url_for_project(project_id)


class EomisWritebackEntrypoint(EntrypointInterface):
    """Entrypoint for scheduled eOMIS writeback runs."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--flow",
            help="The writeback flow to run",
            choices=FLOW_NAMES,
            required=True,
        )
        parser.add_argument(
            "--commit",
            help="Perform real writes; without it the run is a dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--max-writes",
            help="Override the flow's volume circuit breaker for this run",
            type=int,
        )
        parser.add_argument(
            "--bq-view",
            help="Override the flow's default candidate source view",
            type=str,
        )
        parser.add_argument(
            "--allow-prod-write",
            help="Required to --commit against the production eOMIS instance",
            action="store_true",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        """Runs the named flow through the shared runner and raises (exiting
        non-zero) if any candidate errored."""
        scheduled_flow = scheduled_flow_for_name(args.flow)
        base_url = scheduled_flow.base_url_for_project(metadata.project_id())
        if (
            args.commit
            and base_url == scheduled_flow.prod_base_url
            and not args.allow_prod_write
        ):
            raise ValueError("production writes require --allow-prod-write")

        flow = build_flow(args.flow, bq_view=args.bq_view)
        candidates = flow.load_candidates()
        selected = [candidate for candidate in candidates if candidate.is_actionable]
        logging.info(
            "%s: loaded [%d] candidates, [%d] actionable; mode [%s] against [%s]",
            flow.flow_name,
            len(candidates),
            len(selected),
            "COMMIT" if args.commit else "dry-run",
            base_url,
        )
        if not selected:
            return

        credentials = resolve_eomis_credentials(
            flow.state_code, username_override=None, password_override=None
        )
        client = EomisClient(base_url, credentials.username, credentials.password)
        results = run_writeback(
            flow=flow,
            client=client,
            candidates=selected,
            commit=args.commit,
            recorders=[LoggingAuditRecorder()],
            max_writes=(
                args.max_writes
                if args.max_writes is not None
                else flow.max_writes_per_run
            ),
            pause_min=PAUSE_MIN_SECONDS,
            pause_max=PAUSE_MAX_SECONDS,
        )

        errored = [result for result in results if result.status is ResultStatus.ERROR]
        if errored:
            raise RuntimeError(
                f"{flow.flow_name}: [{len(errored)}] of [{len(results)}] candidates "
                "errored; see the audit log above for details."
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if in_development():
        set_development_project_id_override(GCP_PROJECT_STAGING)

    EomisWritebackEntrypoint.run_entrypoint(
        args=EomisWritebackEntrypoint.get_parser().parse_args()
    )
