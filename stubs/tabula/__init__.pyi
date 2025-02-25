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
"""Implements stubs for the Tabula python library."""
from typing import Union, List, Dict, Any, Optional

import pandas as pd

def read_pdf(
    input_path: str,
    pages: Union[List[int], str, int] = 1,
    lattice: bool = False,
    multiple_tables: bool = True,
    stream: bool = False,
    pandas_options: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame: ...
