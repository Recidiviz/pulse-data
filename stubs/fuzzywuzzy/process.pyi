# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
from typing import Any, Callable, Union

from fuzzywuzzy.fuzz import WRatio
from fuzzywuzzy.utils import full_process

def extractOne(
    query: Any,
    choices: Any,
    processor: Callable[[Any, bool], Any] = full_process,
    scorer: Callable[[Any, Any, bool, bool], int] = WRatio,
    score_cutoff: int = 0,
) -> Union[tuple[Any, int, Any], tuple[Any, int]]: ...
