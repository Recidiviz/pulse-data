# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""An interface for data stores used to hold some sort of state for the Admin Panel."""
from abc import abstractmethod
from typing import Optional

from recidiviz.utils import metadata


class AdminPanelStore:
    def __init__(self, override_project_id: Optional[str] = None) -> None:
        self.project_id = (
            metadata.project_id()
            if override_project_id is None
            else override_project_id
        )

    @abstractmethod
    def recalculate_store(self) -> None:
        """Recalculates the state of the internal store."""
