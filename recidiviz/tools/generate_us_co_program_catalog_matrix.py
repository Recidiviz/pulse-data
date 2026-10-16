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
"""Renders the US_CO Achievement Earned Time Matrix as a print-ready HTML doc.

Queries the us_co_policy_program_catalog view, groups programs by category
group, and writes an HTML file styled to match CDOC's AR Form 625-02B. Open the
output in Chrome and print to PDF (Cmd-P -> Save as PDF). Don't include headers
and footers, but do include "Background graphics" when printing.

Section order and within-section order come entirely from the view (via its
category_group_sort_order / program_sort_order columns). If the view emits a row
with no category group -- meaning the source data has a category the view has
not mapped to a group -- this tool fails loudly rather than rendering that
program in an arbitrary position.

Usage:
    uv run python -m recidiviz.tools.generate_us_co_program_catalog_matrix \
        --project-id recidiviz-staging \
        [--output ET_Matrix.html] \
        [--revision-date MM/DD/YYYY] \
        [--sandbox-dataset-prefix my_prefix]
"""

import argparse
import datetime
import html
import logging
from collections.abc import Mapping, Sequence

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.views.analyst_data.us_co.us_co_policy_program_catalog import (
    CATEGORY_GROUP_COLUMN,
    CATEGORY_GROUP_SORT_ORDER_COLUMN,
    CREDIT_DAYS_COLUMN,
    PROGRAM_CERTIFICATE_NAME_COLUMN,
    PROGRAM_SORT_ORDER_COLUMN,
    US_CO_POLICY_PROGRAM_CATALOG_VIEW_BUILDER,
    US_CO_POLICY_PROGRAM_CATALOG_VIEW_NAME,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type

TITLE = "Achievement Earned Time Matrix"

# Format for the --revision-date argument and the date shown on the document.
REVISION_DATE_FORMAT = "%m/%d/%Y"

# Print styling that matches AR Form 625-02B.
HTML_STYLE = """
  @page { size: letter; margin: 0.5in; }
  body { font-family: "Times New Roman", Georgia, serif; color: #000; margin: 0; }
  .revision { text-align: right; font-size: 9pt; }
  h1 { text-align: center; font-size: 16pt; font-weight: normal; margin: 4px 0 10px; }
  table { width: 100%; border-collapse: collapse; font-size: 9pt; }
  th, td { border: 0.75px solid #000; padding: 1px 5px; vertical-align: top; }
  thead th { background: #d9d9d9; font-weight: bold; text-align: center; font-size: 11pt; }
  th.days, td.days { white-space: nowrap; width: 90px; }
  td.days { text-align: right; }
  tr.group td {
    background: #f0f0f0; font-weight: bold; text-align: center; font-size: 10.5pt;
  }
  /* Repeat the column header on every printed page; don't orphan group headers. */
  thead { display: table-header-group; }
  tr.group { break-inside: avoid; break-after: avoid; }
  tr { break-inside: avoid; }
"""


def _revision_date_arg(value: str) -> str:
    """Validates that a --revision-date value is a MM/DD/YYYY date, returning it as-is."""
    try:
        datetime.datetime.strptime(value, REVISION_DATE_FORMAT)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Revision date [{value}] must be in MM/DD/YYYY format."
        ) from e
    return value


def _credit_label(days: int) -> str:
    """Returns the earned-time label for a credit-day count (e.g. "1 day", "5 days")."""
    return "1 day" if days == 1 else f"{days} days"


def _group_programs(
    rows: Sequence[Mapping[str, str | int | None]],
) -> dict[str, list[tuple[str, int]]]:
    """Returns policy-catalog rows grouped by category group, preserving row order.

    Expects rows already sorted by (category_group_sort_order, program_sort_order).
    Fails loudly if any row has a NULL category_group or category_group_sort_order:
    that means the source data contains a category the view has not assigned to a
    group, and it would otherwise render unordered.
    """
    ungrouped_names = [
        row[PROGRAM_CERTIFICATE_NAME_COLUMN]
        for row in rows
        if row[CATEGORY_GROUP_COLUMN] is None
        or row[CATEGORY_GROUP_SORT_ORDER_COLUMN] is None
    ]
    if ungrouped_names:
        names = "\n".join(f"    - [{name}]" for name in ungrouped_names)
        raise ValueError(
            f"Found {len(ungrouped_names)} program(s) with no category group -- "
            "their category is not mapped to a category group in the "
            f"[{US_CO_POLICY_PROGRAM_CATALOG_VIEW_NAME}] view. Map the missing "
            f"category to a group in that view:\n{names}"
        )

    by_group: dict[str, list[tuple[str, int]]] = {}
    for row in rows:
        by_group.setdefault(assert_type(row[CATEGORY_GROUP_COLUMN], str), []).append(
            (
                assert_type(row[PROGRAM_CERTIFICATE_NAME_COLUMN], str),
                assert_type(row[CREDIT_DAYS_COLUMN], int),
            )
        )
    return by_group


def _fetch_programs_by_group(
    *, project_id: str, sandbox_dataset_prefix: str | None
) -> dict[str, list[tuple[str, int]]]:
    """Returns credit-granting programs grouped by category group, in the view's sort order.

    If sandbox_dataset_prefix is set, reads the view from the prefixed sandbox
    dataset instead of the deployed one -- useful before the view is deployed.
    """
    with local_project_id_override(project_id):
        table = US_CO_POLICY_PROGRAM_CATALOG_VIEW_BUILDER.build().table_for_query
    dataset_id = (
        BigQueryAddressOverrides.format_sandbox_dataset(
            prefix=sandbox_dataset_prefix, dataset_id=table.dataset_id
        )
        if sandbox_dataset_prefix
        else table.dataset_id
    )

    query = f"""
    SELECT
        {CATEGORY_GROUP_COLUMN},
        {PROGRAM_CERTIFICATE_NAME_COLUMN},
        {CREDIT_DAYS_COLUMN},
        {CATEGORY_GROUP_SORT_ORDER_COLUMN},
        {PROGRAM_SORT_ORDER_COLUMN}
    FROM `{project_id}.{dataset_id}.{table.table_id}`
    ORDER BY {CATEGORY_GROUP_SORT_ORDER_COLUMN}, {PROGRAM_SORT_ORDER_COLUMN}
    """
    rows = BigQueryClientImpl(project_id=project_id).run_query_async(
        query_str=query, use_query_cache=True
    )
    return _group_programs([dict(row) for row in rows])


def _render_html(
    *, by_group: dict[str, list[tuple[str, int]]], revision_date: str
) -> str:
    """Returns the matrix as an HTML document string styled to match AR Form 625-02B."""
    body_rows: list[str] = []
    for group, programs in by_group.items():
        body_rows.append(
            f'<tr class="group"><td colspan="2">{html.escape(group)}</td></tr>'
        )
        for name, days in programs:
            body_rows.append(
                f"<tr><td>{html.escape(name)}</td>"
                f'<td class="days">{_credit_label(days)}</td></tr>'
            )
    rows_html = "\n".join(body_rows)
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{TITLE}</title>
<style>{HTML_STYLE}</style>
</head>
<body>
<div class="revision">Revision Date: {html.escape(revision_date)}</div>
<h1>{TITLE}</h1>
<table>
  <thead>
    <tr><th class="program">Program - Certificate Name</th><th class="days">Earned Time</th></tr>
  </thead>
  <tbody>
{rows_html}
  </tbody>
</table>
</body>
</html>
"""


def main() -> None:
    """Parses args, fetches the catalog, and writes the rendered matrix HTML."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--project-id",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The project to query the program catalog view from.",
    )
    parser.add_argument(
        "--output",
        default="ET_Matrix.html",
        help="Path to write the rendered HTML file to.",
    )
    parser.add_argument(
        "--revision-date",
        type=_revision_date_arg,
        default=datetime.date.today().strftime(REVISION_DATE_FORMAT),
        help="Revision date shown top-right, in MM/DD/YYYY format. Defaults to today.",
    )
    parser.add_argument(
        "--sandbox-dataset-prefix",
        help="If set, read the view from this sandbox dataset prefix instead of "
        "the deployed dataset (e.g. a load_views_to_sandbox prefix).",
    )
    args = parser.parse_args()

    by_group = _fetch_programs_by_group(
        project_id=args.project_id, sandbox_dataset_prefix=args.sandbox_dataset_prefix
    )
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(_render_html(by_group=by_group, revision_date=args.revision_date))
    total = sum(len(programs) for programs in by_group.values())
    logging.info(
        "Wrote %s: %d programs across %d groups", args.output, total, len(by_group)
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
