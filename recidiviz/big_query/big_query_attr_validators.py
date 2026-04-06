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
"""BigQuery-specific attrs validators."""
from typing import Any

import attr

from recidiviz.big_query.big_query_utils import validate_unquoted_bq_identifier
from recidiviz.common.attr_validators import is_non_empty_str


def is_valid_unquoted_bq_identifier(
    instance: Any, attribute: attr.Attribute, value: str
) -> None:
    """Validates that the value is a valid unquoted BigQuery identifier
    (only letters, digits, underscores; does not start with a digit;
    not a reserved word).
    """
    is_non_empty_str(instance, attribute, value)
    validate_unquoted_bq_identifier(value)
