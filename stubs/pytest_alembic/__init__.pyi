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
# ============================================================================
from typing import Any, ContextManager, Dict, List, Optional

from sqlalchemy.engine import Engine

from .history import AlembicHistory

class MigrationContext:
    @property
    def heads(self) -> List[str]: ...
    @property
    def history(self) -> AlembicHistory: ...
    def generate_revision(self, **kwargs: Any) -> None: ...
    def migrate_down_to(self, revision: str) -> None: ...
    def migrate_up_before(self, revision: str) -> None: ...
    def migrate_up_one(self) -> None: ...
    def migrate_up_to(self, revision: str) -> None: ...

def runner(
    config: Dict[str, Any], engine: Optional[Engine] = None
) -> ContextManager[MigrationContext]: ...
