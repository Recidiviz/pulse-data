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
"""CLI for the AR GED Program Referral writeback.

Thin by design: parses flags, builds the AR flow, prints the run plan,
confirms, and hands off to the shared runner. The scheduled production job
runs the exact same flow + runner; this CLI only adds the manual conveniences
(csv/ids sources, confirmation prompt, rich output).

Usage (dry-run against the AR test instance):
    uv run python -m recidiviz.tools.eomis.us_ar.ar_program_referral_writeback

Real writes require --commit; production writes also require
--allow-prod-write.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from typing import Sequence

from rich.console import Console

from recidiviz.tools.eomis.client import EomisClient
from recidiviz.tools.eomis.flow import Candidate, ResultStatus
from recidiviz.tools.eomis.runner import (
    CsvAuditRecorder,
    render_plan,
    render_results,
    run_writeback,
)
from recidiviz.tools.eomis.us_ar.program_referral_flow import (
    DEFAULT_PROJECT_ID,
    DEFAULT_VIEW,
    PROD_BASE_URL,
    TEST_BASE_URL,
    ArProgramReferralCandidate,
    ArProgramReferralFlow,
    build_id_candidates,
    load_csv_candidates,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parses CLI flags for an attended writeback run."""
    parser = argparse.ArgumentParser(
        description="AR GED Program Referral writeback tool."
    )
    parser.add_argument("--domain", choices=["test", "prod", "custom"], default="test")
    parser.add_argument("--base-url", help="Used only with --domain custom")
    parser.add_argument("--source", choices=["bq", "csv", "ids"], default="bq")
    parser.add_argument("--bq-view", default=DEFAULT_VIEW)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--csv")
    parser.add_argument("--offender-id", action="append", default=[])
    parser.add_argument(
        "--action", choices=["auto", "read", "create", "update"], default="auto"
    )
    parser.add_argument("--referral-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--out",
        default=os.path.join(tempfile.gettempdir(), "ar_eomis_writeback_results.csv"),
    )
    parser.add_argument("--comment", default="")
    parser.add_argument("--add-comment", default="")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--allow-prod-write", action="store_true")
    parser.add_argument("--max-writes", type=int)
    parser.add_argument("--pause-min", type=float, default=1.0)
    parser.add_argument("--pause-max", type=float, default=2.0)
    return parser.parse_args(argv)


def base_url_from_args(args: argparse.Namespace) -> str:
    if args.domain == "test":
        return TEST_BASE_URL
    if args.domain == "prod":
        return PROD_BASE_URL
    if not args.base_url:
        raise ValueError("--domain custom requires --base-url")
    return args.base_url.rstrip("/")


def credentials_from_args(args: argparse.Namespace) -> tuple[str, str]:
    username = args.username or os.environ.get("EOMIS_AR_USERNAME")
    password = args.password or os.environ.get("EOMIS_AR_PASSWORD")
    if not username or not password:
        raise ValueError(
            "Set EOMIS_AR_USERNAME/EOMIS_AR_PASSWORD or pass --username/--password"
        )
    return username, password


def load_candidates(
    args: argparse.Namespace, flow: ArProgramReferralFlow
) -> list[ArProgramReferralCandidate]:
    if args.source == "bq":
        return flow.load_candidates()
    if args.source == "csv":
        if not args.csv:
            raise ValueError("--source csv requires --csv")
        return load_csv_candidates(args.csv, args.limit)
    if not args.offender_id:
        raise ValueError("--source ids requires at least one --offender-id")
    return build_id_candidates(args.offender_id, args.action, args.referral_date)


def confirm_writes(
    args: argparse.Namespace, selected: Sequence[Candidate], base_url: str
) -> bool:
    if not args.commit or args.yes:
        return True
    answer = input(f"Write {len(selected)} row(s) to {base_url}? Type 'yes': ")
    return answer.strip() == "yes"


def main(argv: list[str]) -> int:
    """Loads candidates, shows the plan, confirms, and runs the writeback.
    Returns a non-zero exit code if any candidate errored."""
    console = Console()
    args = parse_args(argv)
    base_url = base_url_from_args(args)
    if args.commit and base_url == PROD_BASE_URL and not args.allow_prod_write:
        raise ValueError("production writes require --allow-prod-write")

    flow = ArProgramReferralFlow(
        bq_view=args.bq_view,
        project_id=args.project_id,
        limit=args.limit,
        comment=args.comment,
        add_comment=args.add_comment,
    )
    candidates = load_candidates(args, flow)
    selected = [candidate for candidate in candidates if candidate.is_actionable]
    render_plan(
        console,
        candidates,
        selected,
        title=flow.flow_name,
        base_url=base_url,
        commit=args.commit,
    )
    if not selected:
        return 0
    if not confirm_writes(args, selected, base_url):
        console.print("Aborted.")
        return 1

    username, password = credentials_from_args(args)
    client = EomisClient(base_url, username, password)
    with CsvAuditRecorder(args.out) as recorder:
        results = run_writeback(
            flow=flow,
            client=client,
            candidates=selected,
            commit=args.commit,
            recorders=[recorder],
            max_writes=(
                args.max_writes
                if args.max_writes is not None
                else flow.max_writes_per_run
            ),
            pause_min=args.pause_min,
            pause_max=args.pause_max,
        )

    render_results(console, results, args.out)
    return 1 if any(result.status == ResultStatus.ERROR for result in results) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
