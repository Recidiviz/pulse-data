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
from typing import Optional, Tuple

from pandas import DataFrame
from numpy import ndarray

class ARIMAResults:
    def forecast(
        self, steps: int, exog: Optional[ndarray] = None, alpha: float = 0.05
    ) -> Tuple[ndarray, ndarray, ndarray]: ...

class ARIMA:
    def __init__(self, endog: DataFrame, order: Optional[tuple] = None) -> None: ...
    def fit(self, disp: bool) -> ARIMAResults: ...
