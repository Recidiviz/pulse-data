# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Lists views that are currently exempted from checks preventing references to
deprecated views.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress

# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists. Empty since the sentences v1 schema deletion
# removed the last exemptions; the next deprecation effort adds its exemptions here.
DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {}
