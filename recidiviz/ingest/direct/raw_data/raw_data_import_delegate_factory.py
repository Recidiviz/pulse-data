# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Contains factory class for creating BaseRawDataImportDelegate objects"""

from recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate import (
    BaseRawDataImportDelegate,
)


class RawDataImportDelegateFactory:
    @classmethod
    def build(cls, *, region_code: str) -> BaseRawDataImportDelegate:
        region_code = region_code.upper()
        raise ValueError(f"Unexpected region code provided: {region_code}")
