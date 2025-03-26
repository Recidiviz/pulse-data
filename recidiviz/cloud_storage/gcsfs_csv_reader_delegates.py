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
"""Shared implementations of the GcsfsCsvReaderDelegate."""


import pandas as pd

from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReaderDelegate


class SimpleGcsfsCsvReaderDelegate(GcsfsCsvReaderDelegate):
    """A simple, base implementation of the GcsfsCsvReaderDelegate that allows the GcsfsCsvReader to cycle through all
    CSV chunks until we find a valid encoding, but does nothing with the data.
    """

    def on_start_read_with_encoding(self, encoding: str) -> None:
        pass

    def on_file_stream_normalization(
        self, old_encoding: str, new_encoding: str
    ) -> None:
        pass

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        return True

    def on_file_read_success(self, encoding: str) -> None:
        pass
