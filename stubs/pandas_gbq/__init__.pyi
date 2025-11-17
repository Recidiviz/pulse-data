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

import pandas
from pandas import DataFrame

def read_gbq(
    query: str,
    project_id: Optional[str] = None,
) -> DataFrame: ...
def to_gbq(
    dataframe: pandas.DataFrame,
    destination_table: str,
    project_id: Optional[str] = None,
    if_exists: Optional[str] = "fail",
) -> DataFrame: ...
