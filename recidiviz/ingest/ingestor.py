# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Base for any class that ingest data.
"""

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)


class Ingestor:
    """Base class for scrapers, direct ingest controllers, and anything else that processes data into the system."""

    def get_enum_overrides(self) -> EnumOverrides:
        return get_standard_enum_overrides()
