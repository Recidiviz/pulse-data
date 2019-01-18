# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Common utility functions used across aggregated_ingest."""
import pandas as pd


def collapse_header(columns: pd.MultiIndex) -> pd.MultiIndex:
    """Strip empty information from a multi-index Tabula header."""
    pruned_headers = [filter(_should_keep_word_in_tabula_header, column_header)
                      for column_header in columns.values]
    return [' '.join(word).strip() for word in pruned_headers]


def _should_keep_word_in_tabula_header(word: str) -> bool:
    """
    Tabula may parse headers as a pd.MultiIndex which contains multiple rows of
    header text. Rows that are blank contain the word 'Unnamed' and should be
    ignored.
    """
    return 'Unnamed' not in word
