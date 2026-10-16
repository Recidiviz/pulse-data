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
"""Tests for the US_CO program catalog matrix rendering tool."""
import unittest

from recidiviz.calculator.query.state.views.analyst_data.us_co.us_co_policy_program_catalog import (
    CATEGORY_GROUP_COLUMN,
    CATEGORY_GROUP_SORT_ORDER_COLUMN,
    CREDIT_DAYS_COLUMN,
    PROGRAM_CERTIFICATE_NAME_COLUMN,
    PROGRAM_SORT_ORDER_COLUMN,
)
from recidiviz.tools.generate_us_co_program_catalog_matrix import (
    HTML_STYLE,
    _credit_label,
    _group_programs,
    _render_html,
)


def _row(
    *,
    group: str | None,
    name: str,
    credit_days: int,
    group_sort_order: int | None,
    program_sort_order: int,
) -> dict[str, str | int | None]:
    return {
        CATEGORY_GROUP_COLUMN: group,
        PROGRAM_CERTIFICATE_NAME_COLUMN: name,
        CREDIT_DAYS_COLUMN: credit_days,
        CATEGORY_GROUP_SORT_ORDER_COLUMN: group_sort_order,
        PROGRAM_SORT_ORDER_COLUMN: program_sort_order,
    }


class CreditLabelTest(unittest.TestCase):
    """Tests for _credit_label."""

    def test_singular_and_plural(self) -> None:
        self.assertEqual("1 day", _credit_label(1))
        self.assertEqual("2 days", _credit_label(2))
        self.assertEqual("30 days", _credit_label(30))

    def test_zero_is_plural(self) -> None:
        self.assertEqual("0 days", _credit_label(0))


class GroupProgramsTest(unittest.TestCase):
    """Tests for _group_programs."""

    def test_groups_in_row_order(self) -> None:
        rows = [
            _row(
                group="THERAPEUTIC PROGRAMS",
                name="Beyond Trauma",
                credit_days=3,
                group_sort_order=0,
                program_sort_order=1,
            ),
            _row(
                group="THERAPEUTIC PROGRAMS",
                name="Beyond Violence",
                credit_days=5,
                group_sort_order=0,
                program_sort_order=2,
            ),
            _row(
                group="OTHER PROGRAMS",
                name="Inside Out Dads",
                credit_days=4,
                group_sort_order=1,
                program_sort_order=1,
            ),
        ]
        self.assertEqual(
            [
                (
                    "THERAPEUTIC PROGRAMS",
                    [("Beyond Trauma", 3), ("Beyond Violence", 5)],
                ),
                ("OTHER PROGRAMS", [("Inside Out Dads", 4)]),
            ],
            list(_group_programs(rows).items()),
        )

    def test_raises_on_null_group_sort_order_naming_program(self) -> None:
        rows = [
            _row(
                group="THERAPEUTIC PROGRAMS",
                name="Beyond Trauma",
                credit_days=3,
                group_sort_order=0,
                program_sort_order=1,
            ),
            # Unmapped category: the view's LEFT JOIN leaves its sort order NULL.
            _row(
                group=None,
                name="Unmapped Program",
                credit_days=7,
                group_sort_order=None,
                program_sort_order=1,
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"^Found 1 program\(s\) with no category group -- their category is not "
            r"mapped to a category group in the \[us_co_policy_program_catalog\] "
            r"view\. Map the missing category to a group in that view:\n"
            r"    - \[Unmapped Program\]$",
        ):
            _group_programs(rows)

    def test_empty_input(self) -> None:
        self.assertEqual({}, _group_programs([]))


class RenderHtmlTest(unittest.TestCase):
    """Tests for _render_html."""

    def test_renders_groups_and_rows_in_order(self) -> None:
        expected = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Achievement Earned Time Matrix</title>
<style>{HTML_STYLE}</style>
</head>
<body>
<div class="revision">Revision Date: 07/16/2026</div>
<h1>Achievement Earned Time Matrix</h1>
<table>
  <thead>
    <tr><th class="program">Program - Certificate Name</th><th class="days">Earned Time</th></tr>
  </thead>
  <tbody>
<tr class="group"><td colspan="2">THERAPEUTIC PROGRAMS</td></tr>
<tr><td>Beyond Trauma</td><td class="days">3 days</td></tr>
<tr><td>PDP Paid Program</td><td class="days">1 day</td></tr>
<tr class="group"><td colspan="2">OTHER PROGRAMS</td></tr>
<tr><td>Inside Out Dads</td><td class="days">4 days</td></tr>
  </tbody>
</table>
</body>
</html>
"""
        self.assertEqual(
            expected,
            _render_html(
                by_group={
                    "THERAPEUTIC PROGRAMS": [
                        ("Beyond Trauma", 3),
                        ("PDP Paid Program", 1),
                    ],
                    "OTHER PROGRAMS": [("Inside Out Dads", 4)],
                },
                revision_date="07/16/2026",
            ),
        )

    def test_escapes_html_in_program_and_group_names(self) -> None:
        expected = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Achievement Earned Time Matrix</title>
<style>{HTML_STYLE}</style>
</head>
<body>
<div class="revision">Revision Date: 07/16/2026</div>
<h1>Achievement Earned Time Matrix</h1>
<table>
  <thead>
    <tr><th class="program">Program - Certificate Name</th><th class="days">Earned Time</th></tr>
  </thead>
  <tbody>
<tr class="group"><td colspan="2">R&amp;D PROGRAMS</td></tr>
<tr><td>R&amp;D &lt;Special&gt; Program</td><td class="days">2 days</td></tr>
  </tbody>
</table>
</body>
</html>
"""
        self.assertEqual(
            expected,
            _render_html(
                by_group={"R&D PROGRAMS": [("R&D <Special> Program", 2)]},
                revision_date="07/16/2026",
            ),
        )


if __name__ == "__main__":
    unittest.main()
