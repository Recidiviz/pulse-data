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
"""General helpers for python endpoint scripts."""
import json
from typing import Any, Optional

from flask import request


def get_value_from_request(
    key: str, default_value: Optional[Any] = None
) -> Optional[Any]:
    json_data_text = request.get_data(as_text=True)
    try:
        json_data = json.loads(json_data_text)
    except (TypeError, json.decoder.JSONDecodeError):
        json_data = {}

    return json_data.get(key, default_value)
