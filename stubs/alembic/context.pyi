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
from sqlalchemy.engine import Connection
from sqlalchemy.schema import MetaData

config: Config

def configure(
    connection: Optional[Connection] = None,
    url: Optional[str] = None,
    target_metadata: Optional[MetaData] = None,
    transaction_per_migration: bool = False,
    literal_binds: bool = False,
    compare_type: bool = False,
) -> None: ...
def is_offline_mode() -> bool: ...
def run_migrations() -> None: ...
