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
"""Utilities for (DADS) folks working in Jupyter notebooks.

In a cell in your notebook, run:

    %run "{path}/pulse-data/recidiviz/tools/analyst/notebook_utils.py"

For notebooks in `pulse-data`, this will look something like:
    %run "../../../tools/analyst/notebook_utils.py"

For notebooks in `recidiviz-research`, given a typical file structure with the
`pulse-data` and `recidiviz-research` repositories in the same parent folder, this
will look something like:
    %run "../../../pulse-data/recidiviz/tools/analyst/notebook_utils.py"
"""

# TODO(#40735): Remove reference to `recidiviz-research` in the above docstring once
# that repository is deleted and we've fully migrated to `pulse-data`.

# pylint: disable=W0611, W0614  # unused imports
# pylint: disable=C0411  # wrong-import-order
# pylint: disable=C0413  # wrong-import-position

# imports for notebooks
import datetime  # noqa
import os
import re
import sys
import time
from os.path import abspath, dirname
from typing import Dict, Iterable, List, Optional, Union

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import pandas_gbq
import seaborn as sns
from IPython import get_ipython
from IPython.display import HTML, display
from tqdm.notebook import tqdm

# get path of this file
current_file_path = os.path.dirname(__file__)
# get path of pulse-data from this path, three parent folders up
recidiviz_data_path = os.path.abspath(os.path.join(current_file_path, "../../../"))
# add pulse-data to path
sys.path.append(recidiviz_data_path)

# imports from pulse-data
from recidiviz.tools.analyst.plots import (  # isort:skip
    RECIDIVIZ_COLORS,
    group_into_other,
    adjust_plot_scale,
    plot_settings,
    add_legend,
    line_labels,
)

# IPython magics - only run if in notebook environment
def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        # elif shell == "TerminalInteractiveShell":
        #     return False  # Terminal running IPython
        return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


if is_notebook():
    ipython = get_ipython()
    ipython.run_line_magic("load_ext", "google.cloud.bigquery")
    ipython.run_line_magic("load_ext", "autoreload")
    ipython.run_line_magic(
        "autoreload", "2"
    )  # 2 => reload ALL modules on every code run
    plt.rcParams.update(plt.rcParamsDefault)
    ipython.run_line_magic("matplotlib", "inline")
    display(HTML("<style>.container { width:100% !important; }</style>"))

# change default pandas options to show more of dataframe than default
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 200)

# plotting style - use this to ensure plots look similar across analyses
path = dirname(abspath(__file__))
plt.style.use(path + "/recidiviz.mplstyle")
# run `plt.rcParams` to see all config options


# function for inspecting dataframes
def inspect_df(df: pd.DataFrame) -> None:
    """
    Inspects dataframe `df` for standard stuff.
    """
    print("Types:")
    print(df.dtypes)
    print("\nNull values:")
    print(df.isnull().sum())
    print("\nNumeric var summary:")
    display(df.describe())
    print("\nHead:")
    display(df.head())


# function for converting df types when imported via read_gbq
def convert_df_types(
    df: pd.DataFrame, columns_to_ignore: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Converts types of columns in df. This is useful when using read_gbq since the
    returned dataframe will include types that are challenging to work with in pandas.

    Note that all int columns must have zero missing values, or this function will
    throw an error. Before running this function, convert those columns to float
    if you want to keep the missing values.

    Parameters:
    df (pd.DataFrame): The dataframe to convert.
    columns_to_ignore (Optional[List[str]]): List of columns to ignore during conversion.
    """
    if columns_to_ignore is None:
        columns_to_ignore = []

    for c in df.columns:
        if c in columns_to_ignore:
            continue
        if df[c].dtype == "Int64":
            if sum(df[c].isnull()) > 0:
                raise ValueError(
                    f"Column {c} contains at least one null value and cannot be coerced"
                    f" to int"
                )
            df[c] = df[c].astype(int)
        if df[c].dtype == "Float64":
            df[c] = df[c].astype(float)
        elif df[c].dtype == "dbdate":
            df[c] = pd.to_datetime(df[c])
    return df


# When running a local runtime, we can't import `from google.colab import syntax`. This
# means we can't do syntax.sql('''select * from something''') and expect it to be
# correctly highlighted. However, Colab seems to simply look for syntax.sql() to be
# called, and it applies the syntax highlighting appropriately. So if we create a dummy
# syntax with a dummy sql() method, we can get syntax highlighting even when running a
# local runtime!
# Example:
# query = syntax.sql('''
#   SELECT * FROM SOMETHING -- will be highlighted properly in colab
# ''')
class DummySyntax:
    def sql(self, query_string: str) -> str:
        return query_string


syntax = DummySyntax()


def freq_table(
    df: pd.DataFrame, column: str, decimals: int = 2, sort_index: bool = True
) -> pd.DataFrame:
    """
    Returns a DataFrame with frequency counts and percentages for a given column.

    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame
    column : str
        Column name to analyze
    decimals : int, default=2
        Number of decimal places for percentages
    sort_index : bool, default=True
        Whether to sort by index (categories)

    Returns:
    --------
    pandas.DataFrame
        Table with counts and percentages
    """
    counts = df[column].value_counts(dropna=False)
    percents = df[column].value_counts(normalize=True, dropna=False) * 100

    if sort_index:
        counts = counts.sort_index()
        percents = percents.sort_index()

    return pd.DataFrame({"count": counts, "percent": percents.round(decimals)})
