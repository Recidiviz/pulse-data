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
"""Nickname equivalence backed by a vendored nickname data set.

The data file (nicknames.csv) was initially derived from the MIT-licensed
carltonnorthern/nickname-and-diminutive-names-lookup data set
(https://github.com/carltonnorthern/nicknames) though additional nicknames
have been added since. Each row relates a canonical name to one of its
nicknames. Rows are kept in alphabetical order, enforced by a unit test.

When adding rows, put the full name in the canonical column. A row with a
nickname in the canonical column lets that nickname act as a canonical and
bridges every full name listed under it into equivalence: the data set we
started with contained "bill,robert" alongside "bill,william", which made
ROBERT and WILLIAM nickname-equivalent until those rows were pruned.
"""
import csv
import os
from collections import defaultdict
from functools import cache

_NICKNAMES_CSV_PATH = os.path.join(os.path.dirname(__file__), "nicknames.csv")

_EXPECTED_HEADER = ["canonical_name", "nickname"]


def are_names_nickname_equivalent(name_a: str, name_b: str) -> bool:
    """Returns whether the two given names share a canonical name in the
    vendored lookup. Returns True, for example, for "Joe" and "Joey", since
    they both share the canonical name "Joseph", but also for "Joe" and
    "Joseph" since a canonical name ("Joseph") belongs to itself.

    Note: a name absent from the data set is equivalent to nothing, including
    itself, so callers must handle exact matches themselves.
    """

    by_name = _canonicals_by_name()
    # .get rather than []: most real names are absent from the data set, and
    # absence simply means "no nickname relationship known".
    canonicals_a = by_name.get(name_a.lower())
    canonicals_b = by_name.get(name_b.lower())
    if canonicals_a is None or canonicals_b is None:
        return False
    return bool(canonicals_a & canonicals_b)


@cache
def _canonicals_by_name() -> dict[str, frozenset[str]]:
    """Returns, for each lowercased name in the data set, the set of canonical
    names it belongs to (a canonical name belongs to itself). A name that is a
    nickname of several unrelated canonicals belongs to all of them.

    For example, the rows

        terence,terry
        terrance,terry

    produce

        {
            "terence": {"terence"},
            "terry": {"terence", "terrance"},
            "terrance": {"terrance"},
        }

    so terry belongs to two canonicals, while terence and terrance each belong
    only to themselves.
    """
    canonicals: dict[str, set[str]] = defaultdict(set)
    with open(_NICKNAMES_CSV_PATH, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != _EXPECTED_HEADER:
            raise ValueError(
                f"Unexpected header [{header}] in [{_NICKNAMES_CSV_PATH}]; "
                f"expected {_EXPECTED_HEADER}."
            )
        for row in reader:
            if len(row) != 2:
                raise ValueError(f"Malformed row [{row}] in [{_NICKNAMES_CSV_PATH}].")
            canonical, nickname = (cell.strip().lower() for cell in row)
            canonicals[canonical].add(canonical)
            canonicals[nickname].add(canonical)
    return {name: frozenset(c) for name, c in canonicals.items()}
