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
""" Querier utils """
from datetime import date, timedelta
from typing import Dict, Any, Optional


def _json_map_dates_to_strings(
    json_dict: Dict[str, Any], timedelta_shift: Optional[timedelta] = None
) -> Dict[str, Any]:
    """This function is used to pre-emptively convert dates to strings. If
    we don't do this, flask's jsonify tries to be helpful and turns our date
    into a datetime with a GMT timezone, which causes problems downstream."""
    results = {}
    for k, v in json_dict.items():
        if isinstance(v, date):
            if timedelta_shift:
                results[k] = str(v + timedelta_shift)
            else:
                results[k] = str(v)
        else:
            results[k] = v
    return results
