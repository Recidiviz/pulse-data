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
"""
Investigates stable counts validation failures for state supervision or incarceration
period tables by computing per-field change rates grouped by start/admission date month.

Outputs two tables:
  1. Month overview: total_rows, distinct_people, and month-over-month change %.
     Months already exempted in stable_counts.py are shown with "(exempted)" instead
     of "**" so you can focus on genuinely new anomalies.
  2. Field change rates: percent of periods where each field differs from the previous
     period for the same person, filtered to fields that exceed a change-rate threshold
     in at least one *non-exempted* month.

Usage:
    uv run python -m recidiviz.tools.ingest.investigations.stable_counts_change_detection \\
        --project-id recidiviz-staging \\
        --state-code US_NC \\
        --entity state_supervision_period

    # Show incarceration periods, lower the field-filter threshold:
    uv run python -m recidiviz.tools.ingest.investigations.stable_counts_change_detection \\
        --project-id recidiviz-staging \\
        --state-code US_ID \\
        --entity state_incarceration_period \\
        --field-threshold 10
"""

import argparse
import csv
import sys
from datetime import date

from google.cloud import bigquery
from tabulate import tabulate

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.validation.views.state.stable_counts.stable_counts import (
    ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME,
)

_ENTITY_CONFIG: dict[str, dict] = {
    "state_supervision_period": {
        "date_col": "start_date",
        "ignore_cols": {
            "supervision_period_id",
            "external_id",
            "state_code",
            "county_code",
            "sequence_num",
            "start_date",
            "termination_date",
        },
    },
    "state_incarceration_period": {
        "date_col": "admission_date",
        "ignore_cols": {
            "incarceration_period_id",
            "external_id",
            "state_code",
            "county_code",
            "sequence_num",
            "admission_date",
            "release_date",
        },
    },
}


def _get_exempted_months(
    entity: str, state_code: StateCode, date_col: str
) -> set[date]:
    """Returns the set of month-start dates already exempted in stable_counts.py."""
    table_config = ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME.get(entity)
    if table_config is None:
        return set()
    for col in table_config.date_columns_to_check:
        if col.date_column_name == date_col:
            return set(col.exemptions.get(state_code, []))
    return set()


def _get_columns(
    client: bigquery.Client,
    project_id: str,
    entity: str,
    ignore_cols: set[str],
) -> list[str]:
    query = f"""
        SELECT column_name
        FROM `{project_id}.normalized_state.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @entity
          AND column_name NOT IN UNNEST(@ignore_cols)
        ORDER BY ordinal_position
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entity", "STRING", entity),
            bigquery.ArrayQueryParameter("ignore_cols", "STRING", list(ignore_cols)),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return [row["column_name"] for row in rows]


def _build_change_detection_query(
    project_id: str,
    entity: str,
    date_col: str,
    cols: list[str],
    lookback_months: int,
) -> str:
    lag_expressions = ",\n        ".join(
        f"LAG({c}) OVER (PARTITION BY person_id ORDER BY sequence_num) AS prev_{c}"
        for c in cols
    )
    change_expressions = ",\n      ".join(
        f"COUNTIF({c} IS DISTINCT FROM prev_{c}) AS change_in_{c},\n"
        f"      SAFE_DIVIDE(COUNTIF({c} IS DISTINCT FROM prev_{c}), COUNT(*)) * 100"
        f" AS pct_{c}"
        for c in cols
    )
    return f"""
    WITH prep AS (
      SELECT
        *,
        DATE_TRUNC({date_col}, MONTH) AS month,
        {lag_expressions}
      FROM `{project_id}.normalized_state.{entity}`
      WHERE state_code = @state_code
        AND {date_col} >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {lookback_months} MONTH)
    )
    SELECT
      month,
      {change_expressions},
      COUNT(DISTINCT person_id) AS distinct_people,
      COUNT(*) AS total_rows
    FROM prep
    GROUP BY month
    ORDER BY month DESC
    """


def _print_month_overview(
    rows: list[dict],
    exempted_months: set[date],
    total_rows_col: str = "total_rows",
) -> None:
    """Prints month-over-month row count changes, flagging anomalous months."""
    headers = ["month", "total_rows", "distinct_people", "MoM change %"]
    table = []
    sorted_rows = sorted(rows, key=lambda r: r["month"])
    for i, row in enumerate(sorted_rows):
        month_date = row["month"]
        is_exempted = month_date in exempted_months

        prev_total = sorted_rows[i - 1][total_rows_col] if i > 0 else None
        if prev_total and prev_total > 0:
            mom_pct = (
                (row[total_rows_col] - prev_total)
                / max(row[total_rows_col], prev_total)
                * 100
            )
            if is_exempted:
                mom_str = f"{mom_pct:+.1f}%  (exempted)"
            elif abs(mom_pct) > 25:
                mom_str = f"** {mom_pct:+.1f}% **"
            else:
                mom_str = f"{mom_pct:+.1f}%"
        else:
            mom_str = "(exempted)" if is_exempted else "—"

        table.append(
            [
                str(month_date)[:7],
                row[total_rows_col],
                row["distinct_people"],
                mom_str,
            ]
        )
    table.reverse()
    print("\n=== Month Overview ===")
    print(tabulate(table, headers=headers, tablefmt="simple"))


def _print_field_change_rates(
    rows: list[dict],
    cols: list[str],
    exempted_months: set[date],
    field_threshold: float,
) -> None:
    """Prints per-field change rates, filtered to fields exceeding the threshold."""
    # Filter to fields that exceed the threshold in at least one non-exempted month.
    notable_cols = [
        c
        for c in cols
        if any(
            (row.get(f"pct_{c}") or 0) > field_threshold
            and row["month"] not in exempted_months
            for row in rows
        )
    ]
    if not notable_cols:
        print(
            f"\nNo fields exceed the {field_threshold:.0f}% change-rate threshold "
            "in any non-exempted month."
        )
        return

    headers = ["month"] + notable_cols
    table = []
    for row in rows:
        month_date = row["month"]
        month_str = str(month_date)[:7]
        if month_date in exempted_months:
            month_str += " (ex)"
        table.append(
            [month_str] + [f"{row.get(f'pct_{c}', 0):.1f}%" for c in notable_cols]
        )
    print(
        f"\n=== Field Change Rates (fields >{field_threshold:.0f}% in any non-exempted month) ==="
    )
    print(tabulate(table, headers=headers, tablefmt="simple"))


def run(
    project_id: str,
    state_code: StateCode,
    entity: str,
    field_threshold: float = 15.0,
    lookback_months: int = 18,
    output_csv: str | None = None,
) -> None:
    """Runs change detection and prints results to stdout."""
    if entity not in _ENTITY_CONFIG:
        raise ValueError(
            f"Unsupported entity '{entity}'. "
            f"Choose from: {list(_ENTITY_CONFIG.keys())}"
        )
    config = _ENTITY_CONFIG[entity]
    date_col = config["date_col"]
    ignore_cols = config["ignore_cols"]

    exempted_months = _get_exempted_months(entity, state_code, date_col)
    if exempted_months:
        exempt_strs = ", ".join(str(d)[:7] for d in sorted(exempted_months))
        print(
            f"Exempted months for {state_code.value} (from stable_counts.py): {exempt_strs}",
            file=sys.stderr,
        )

    client = bigquery.Client(project=project_id)

    print(
        f"Fetching columns for {entity} in {project_id}.normalized_state ...",
        file=sys.stderr,
    )
    cols = _get_columns(client, project_id, entity, ignore_cols)
    print(f"Found {len(cols)} columns to analyze.", file=sys.stderr)

    query = _build_change_detection_query(
        project_id, entity, date_col, cols, lookback_months
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state_code", "STRING", state_code.value),
        ]
    )

    print("Running change-detection query ...", file=sys.stderr)
    rows = [dict(row) for row in client.query(query, job_config=job_config).result()]
    print(f"Done. {len(rows)} months returned.\n", file=sys.stderr)

    _print_month_overview(rows, exempted_months)
    _print_field_change_rates(rows, cols, exempted_months, field_threshold)

    if output_csv:
        all_keys = list(rows[0].keys()) if rows else []
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nFull results written to {output_csv}", file=sys.stderr)


def _parse_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--project-id",
        default=GCP_PROJECT_STAGING,
        help="GCP project ID (default: recidiviz-staging)",
    )
    parser.add_argument(
        "--state-code",
        required=True,
        type=StateCode,
        choices=list(StateCode),
        help="State code to investigate, e.g. US_NC",
    )
    parser.add_argument(
        "--entity",
        required=True,
        choices=list(_ENTITY_CONFIG.keys()),
        help="Normalized state entity table to analyze",
    )
    parser.add_argument(
        "--field-threshold",
        type=float,
        default=15.0,
        help="Only show fields whose change rate exceeds this %% in at least one "
        "non-exempted month (default: 15)",
    )
    parser.add_argument(
        "--lookback-months",
        type=int,
        default=18,
        help="How many months back to include in the analysis (default: 18)",
    )
    parser.add_argument(
        "--output-csv",
        help="If set, write full results to this CSV path in addition to printing",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        project_id=args.project_id,
        state_code=args.state_code,
        entity=args.entity,
        field_threshold=args.field_threshold,
        lookback_months=args.lookback_months,
        output_csv=args.output_csv,
    )
