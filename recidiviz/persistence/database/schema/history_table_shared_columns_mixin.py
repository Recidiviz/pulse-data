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
"""SQLAlchemy table mixin that defines all columns common on ahistory table"""

from typing import Any, Dict
from sqlalchemy import Column, DateTime


class HistoryTableSharedColumns:
    """A mixin which defines all columns common to any history table"""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_: Any, **__: Dict[str, Any]) -> 'HistoryTableSharedColumns':
        if cls is HistoryTableSharedColumns:
            raise Exception(f'[{cls}] cannot be instantiated')
        return super().__new__(cls)

    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
