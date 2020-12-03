# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

from typing import Optional

from alembic.config import Config


def downgrade(config: Config, revision: str, sql: bool = False, tag: Optional[str] = None) -> None: ...
def revision(config: Config, message: Optional[str] = None, autogenerate: bool = False, sql: bool = False) -> None: ...
def upgrade(config: Config, revision: str, sql: bool = False, tag: Optional[str] = None) -> None: ...
