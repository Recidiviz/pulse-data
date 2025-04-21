# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Justice Counts Types"""
from enum import Enum
from typing import Any, NotRequired, Optional, TypedDict


class DatapointJson(TypedDict):
    """Datapoint object serialized for API response"""

    id: int
    report_id: NotRequired[int]
    agency_name: NotRequired[Optional[str]]
    start_date: str
    end_date: str
    metric_definition_key: NotRequired[str]
    metric_display_name: NotRequired[str]
    disaggregation_display_name: NotRequired[Optional[str]]
    dimension_display_name: NotRequired[Optional[str]]
    sub_dimension_name: NotRequired[Optional[str]]
    value: Any
    old_value: NotRequired[Any]
    is_published: NotRequired[bool]
    frequency: str


# Only these filetypes are allowed for bulk upload. See ALLOWED_EXTENSIONS in api.py.
class BulkUploadFileType(Enum):
    CSV = "CSV"
    XLSX = "XLSX"
    XLS = "XLS"

    @staticmethod
    def from_suffix(label: str) -> Any:
        """
        Converts the three letter file extension (e.g. "xlsx") to a BulkUploadFileType enum. If
        extension is invalid, throws an error.
        """
        return BulkUploadFileType[label.upper()]
