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
""" Contains functions for building the data discovery HTML report """
from functools import partial
from typing import List

import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype

from recidiviz.admin_panel.data_discovery.types import (
    ConfigsByFileType,
    DataFramesByFileType,
)


def highlight_changes(series: pd.Series) -> List[str]:
    """Bolds the font for cells whose value has changed since the previous row"""
    has_changed = (series.notna()) & (series.shift() != series)
    return [
        "color: #012322; font-weight: bold;" if v else "color: rgba(53,83,98,0.85);"
        for v in has_changed
    ]


def center_null(series: pd.Series) -> List[str]:
    """ " Center aligns null values"""
    return ["text-align: center;" if v else "" for v in series.isna()]


def align_text(series: pd.Series) -> List[str]:
    """Right align numeric data; Left align text data"""
    return [
        "text-align: right;" if is_numeric_dtype(series) else "text-align: left"
        for v in series
    ]


def build_report(
    dataframes: DataFramesByFileType,
    configs: ConfigsByFileType,
) -> str:
    """Build and open an HTML report of discovered data"""
    file_contents = """<style>
        #report { font-family: 'Andale Mono', monospace; overflow: scroll; }
        #report h2 { border-bottom: 1px solid #000;} 
        #report tr:nth-child(even) { background: #FAFAFA; }
        #report td, th { padding: 5px 10px; white-space: nowrap; border: 1px outset; }
        #report table { border-spacing: 0; margin-bottom: 2em; }
        #report thead { font-size: 1.2em; }
        #report thead tr th:first-child,
        #report tbody tr th:first-child {
            display: none; // Hide index column
        }
    </style>"""

    for file_type, dataframes_by_file_tag in dataframes.items():
        for file_tag, dataframe in dataframes_by_file_tag.items():
            row_count = len(dataframe)
            if row_count == 0:
                continue

            file_contents += f"""
            <h2>{file_tag} <span class='ant-tag' style='vertical-align: middle'>{file_type.value}</span></h2>
            """

            if row_count > 25000:
                file_contents += f"Too many rows returned in this dataset. Expected less than 25,000, but got {row_count}"
                continue

            dataframe = dataframe.reset_index().drop(["index"], axis=1)
            config = configs[file_type][file_tag]
            left_columns = [*config.primary_keys, "ingest_processing_date"]
            sorted_columns = sorted(
                dataframe.columns,
                key=partial(
                    lambda column, haystack: column not in haystack,
                    haystack=left_columns,
                ),
            )
            dataframe = dataframe.reindex(columns=sorted_columns)
            dataframe = dataframe.sort_values(by=left_columns)
            file_contents += (
                dataframe.style.apply(highlight_changes)
                .apply(align_text)
                .apply(center_null)
                .set_precision(0)
                .set_na_rep("-")
                .render()
            )

    return file_contents
