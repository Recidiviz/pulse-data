# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query parsing helpers that are used in production code to understand BigQuery view
queries.
"""
import re

import sqlglot.expressions as expr

from recidiviz.common.constants.states import StateCode


def get_state_code_literal_references(
    parsed_syntax_tree: expr.Query,
) -> set[StateCode]:
    """Returns any state code literals referenced by this query expression (e.g. "US_XX"
    written in the SQL).
    """

    found_states = set()
    for literal in parsed_syntax_tree.find_all(expr.Literal):
        if not literal.is_string:
            continue
        str_literal_value = literal.this

        if match := re.match(r"^US_[A-Z]{2}$", str_literal_value):
            found_states.add(StateCode(match.string))

    return found_states
