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
"""Fake raw data migrations for test file file_tag_second in fixture region us_xx."""

import datetime
from typing import List

from recidiviz.ingest.direct.controllers.direct_ingest_raw_table_migration import RawTableMigration, \
    UpdateRawTableMigration

COL1 = 'COL1'
DATE_1 = datetime.datetime.fromisoformat('2020-06-10T00:00:00')
DATE_2 = datetime.datetime.fromisoformat('2020-09-21T00:00:00')

MIGRATIONS: List[RawTableMigration] = [
    UpdateRawTableMigration(
        migrations_file=__file__,
        file_update_datetimes=[
            DATE_1,
            DATE_2,
        ],
        filters=[(COL1, '123')],
        updates=[(COL1, '456')],
    ),
]
