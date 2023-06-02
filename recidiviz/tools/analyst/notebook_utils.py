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
"""
Utilities for (DADS) folks working in jupyter notebooks.

In a cell in your notebook, run:

%run {path}/pulse-data/recidiviz/tools/analyst/notebook_utils.py

For a typical file structure with both pulse-data and recidiviz-research repos
in the same parent folder, this will look like:

%run ../../pulse-data/recidiviz/tools/analyst/notebook_utils.py
"""

# pylint: disable=W0611  # unused imports
# pylint: disable=C0411  # import order

# imports for notebooks
import datetime
import os
import re
import sys
import time
from os.path import abspath, dirname
from typing import Dict, Iterable, List, Optional, Union

import matplotlib
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf
from dateutil.relativedelta import relativedelta
from fuzzywuzzy import fuzz, process
from IPython import get_ipython
from IPython.display import display
from linearmodels.panel import PanelOLS
from matplotlib.legend import Legend
from matplotlib.lines import Line2D
from scipy.stats import norm
from tqdm.notebook import tqdm

# adds pulse-data repo to path
# note that file structure must be:
# parent folder:
#  - pulse-data repo
#  - recidiviz-research repo
module_path = os.path.abspath(os.path.join("../../pulse-data"))
sys.path.append(module_path)


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

# pandas options better than default
pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 200)

# plotting style
path = dirname(abspath(__file__))
plt.style.use(path + "/recidiviz.mplstyle")
# run `plt.rcParams` to see all config options

# plotting colors
RECIDIVIZ_COLORS = [
    "#25636F",
    "#D9A95F",
    "#BA4F4F",
    "#4C6290",
    "#90AEB5",
    "#CC989C",
    "#B6CC98",
    "#56256F",
    "#4FBABA",
    "#904C84",
    "#5F8FD9",
]


# Plotting resize method
def adjust_plot_scale(scale_factor: float = 1.0) -> None:
    """
    Adjusts size of plot scaling text and plot area proportionally.

    The list of params is not exhaustive and will likely need some additions for
    plots other than scatter and line.

    params:
    ------
    scale_factor: Number, generally 0.25 to 1.0
    """
    # define param list
    param_lst = [
        "axes.axisbelow",
        "axes.labelsize",
        "axes.titlesize",
        "font.size",
        "grid.linewidth",
        "legend.fontsize",
        "legend.title_fontsize",
        "lines.linewidth",
        "lines.markersize",
        "xtick.major.size",
        "xtick.major.width",
        "ytick.major.size",
        "ytick.major.width",
    ]

    plt.rcParams["figure.figsize"] = [
        x * scale_factor**2 for x in plt.rcParams["figure.figsize"]
    ]
    for param in param_lst:
        # Skip boolean params, since can be coerced as numeric
        if isinstance(plt.rcParams[param], bool):
            continue
        # Adjust numeric params by scale_factors
        if isinstance(plt.rcParams[param], (int, float)):
            plt.rcParams[param] *= scale_factor


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
def convert_df_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts types of columns in df. This is useful when using read_gbq since the
    returned dataframe will include types that are challenging to work with in pandas.

    Note that all int columns must have zero missing values, or this function will
    throw an error. Before running this function, convert those columns to float
    if you want to keep the missing values.
    """
    for c in df.columns:
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


# function for adding legend (outside the plot)
def add_legend(title: Optional[str] = None) -> None:
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5), title=title)


# function for calculating partial regression residuals of X & Y variable against a set
# of controls
def partial_regression(
    data: pd.DataFrame,
    x_var: str,
    y_var: str,
    control_vars: Optional[Union[str, list]] = "1",
    return_append_string: Optional[str] = "_resid",
) -> pd.DataFrame:
    """
    Returns pandas DataFrame consisting of X and Y series residualized on a set of
    control variables via ordinary least squares regression.

    Params:
    ------
    data: pandas DataFrame
    x_var : str
        Column name in `data` with the endogenous variable of interest.

    y_var : str
        Column name in `data` with the exogenous variable of interest.

    control_vars : str or list, default '1'
        List of string column names in `data` associated with other exogenous variables,
        or a string of the right side of the OLS formula consisting of other exogenous
        variables. The effect of these variables will be removed by OLS regression.
        Default ('1') performs no residualization and returns df['x_var'] and
        df['y_var'].

    return_append_string (Optional) : str, default '_resid'
        String to append to residualized `x_var` and `y_var` column names in returned
        dataframe. If no value is provided, append default '_resid'.

    """

    # Checks input type of control variables, and converts `control_vars` object to
    # string formula if input is a list
    if isinstance(control_vars, list):
        control_formula = " + ".join(control_vars)
    elif isinstance(control_vars, str):
        control_formula = control_vars
    else:
        raise ValueError(
            "`control_vars` must be either a list of exogenous variables or a string of"
            " the right side OLS formula"
        )

    # Regress exogenous variable on control variables, and calculate residual errors
    res_model_x = smf.ols(f"{x_var} ~ {control_formula}", data=data).fit()
    # actual = predicted + error
    # error = actual - predicted
    x_error = data[x_var] - res_model_x.predict(data)

    # Regress endogenous variable on control variables, and calculate residual errors
    res_model_y = smf.ols(f"{y_var} ~ {control_formula}", data=data).fit()
    y_error = data[y_var] - res_model_y.predict(data)

    residualized_data = pd.DataFrame(
        {
            f"{x_var}{return_append_string}": x_error,
            f"{y_var}{return_append_string}": y_error,
        }
    )

    return residualized_data


# helper function to get difference between two dates in months
def get_month_delta(a: datetime.date, b: datetime.date) -> int:
    return relativedelta(a, b).months + relativedelta(a, b).years * 12


# function to plot "seaweed" style survival curves using date cohorts on a timeline
def plot_timeline_cohort_survival_curves(
    data: pd.DataFrame,
    duration_var: str,
    date_cohort_var: str,
    # TODO(#431): Add support for non-binary event counts and varying time granularity
    event_var: str,
    cohort_length: int = 1,
    plot_timespan: int = 12,
    min_date: Optional[datetime.date] = None,
    max_date: Optional[datetime.date] = None,
    cohort_spacing: int = 3,
    grouping_var: Optional[str] = None,
    title_text: Optional[str] = None,
    x_text: Optional[str] = None,
    save_fig: Optional[str] = None,
) -> None:
    """
    Function that plots survival curves for cohorts defined based on a date variable, at
    a monthly scale.

    Params:
    ------
    data : pd.DataFrame
        Dataframe containing columns indicated by `date_cohort_var`, `duration_var`,
        `event_var`, and optionally `grouping_var`.

    duration_var : str
        Name of integer column in `data` indicating length of time in survival episode
        in months.

    date_cohort_var : str
        Name of date column in `data` indicating date associated with a cohort as a
        datetime.date() object.

    event_var : str
        Name of boolean column `data` indicating whether survival event occurred or not.

    cohort_length (Optional) : int
        Number of months included in each cohort. For example, on date 2020-07-01 with
        cohort_length 2, cohort includes individuals in range (2020-05-01, 2020-07-01].
        Default = 1.

    plot_timespan (Optional) : int
        Number of months to plot for a single survival curve. Default = 12

    min_date (Optional) : datetime.date
        First date cohort to include in plot. If None, uses minimum date present in
        data[`date_cohort_var`]

    max_date (Optional) : datetime.date
        Maximum date cohort to include in plot. If None, uses maximum date present in
        data[`date_cohort_var`] for which we can observe full `plot_timespan`, aligned
        with `cohort_spacing` intervals. If full `plot_timespan` can not be observed for
        specified `max_date`, select the last possible date cohort with full
        observability.

    cohort_spacing (Optional) : int
        Number of months separating each survival cohort curve on the x-axis.
        Default = 3.

    grouping_var (Optional) : str
        If disaggregation by a particular column is desired, column name indicated here.
        If no column is specified, a single curve for each date cohort is plotted.

    title_text (Optional) : str
        Text for plot title.

    x_text (Optional) : str
        Text for x axis.

    save_fig (Optional) : str
        Location and filename to save the figure
    """

    # If min_date is None, infer from `data`
    if min_date is None:
        min_date = data[date_cohort_var].min()

    # If max_date is None, or if full `plot_timespan` can not be observed for max_date
    # cohort, select max date in `data`
    # with full observability of `plot_timespan`.
    max_date_in_data = data[date_cohort_var].max()
    if (max_date is None) or (
        get_month_delta(max_date_in_data, max_date) < plot_timespan
    ):
        max_date = max_date_in_data - relativedelta(months=plot_timespan)

    # mypy: ensure max_date defined
    if max_date is None:
        raise ValueError("max_date not defined")

    if max_date <= min_date:
        raise ValueError(
            f"Insufficient date range in data ({min_date} - {max_date_in_data}) to plot"
            f" timespan of {plot_timespan}"
        )

    # Define array of date cohorts, each of which will have its own survival curve
    date_range = get_month_delta(max_date, min_date)
    date_cohorts = [
        min_date + relativedelta(months=+x)
        for x in range(0, date_range, cohort_spacing)
    ]

    # If no grouping variable is inputted, use placeholder group
    if grouping_var is None:
        data["default_group"] = "All"
        grouping_var = "default_group"

    group_names = list(data[grouping_var].unique())

    # Loop through every date-defined cohort and plot survival curves
    for date_cohort in date_cohorts:
        c_ix = 0
        df_cohort = data[
            (data[date_cohort_var].dt.date <= date_cohort)
            & (
                data[date_cohort_var].dt.date
                > date_cohort - relativedelta(months=cohort_length)
            )
        ]

        for group_name in group_names:
            df_cohort_subgroup = df_cohort[df_cohort[grouping_var] == group_name]

            cohort_size = len(df_cohort_subgroup)
            survival_crosstab = pd.crosstab(
                df_cohort_subgroup[duration_var], df_cohort_subgroup[event_var]
            )

            # Check if there are nonzero observations of survival-defining events for
            # this date cohort and subgroup
            if survival_crosstab.size > 0:
                # Merge with min (0) and max (`plot_timespan`) duration periods to avoid
                # censoring of survival curve
                survival_table = (
                    pd.merge(
                        pd.DataFrame({duration_var: [0.0, plot_timespan]}),
                        survival_crosstab.reset_index(),
                        how="outer",
                        on=duration_var,
                    )
                    .fillna(0)
                    .sort_values(duration_var)
                    .set_index(duration_var)
                    .cumsum()
                )

                # Calculate count of cohort members for whom event never occurs.
                survival_table["not_terminated"] = cohort_size - survival_table.sum(
                    axis=1
                )

                # Convert counts table to proportions
                survival_table_pct = (survival_table.T / survival_table.sum(axis=1)).T

                # Filter survival table durations to desired survival curve timespan
                survival_table_pct = (
                    survival_table_pct[survival_table_pct.index <= plot_timespan]
                ).reset_index()

                # Map `duration_var` back onto dates based on origin date of date cohort
                survival_table_pct["episode_date_bucket"] = (
                    survival_table_pct[duration_var]
                ).apply(lambda x, dc=date_cohort: dc + relativedelta(months=round(x)))
                survival_table_pct = survival_table_pct.set_index("episode_date_bucket")
                if grouping_var == "default_group":
                    survival_table_pct[True].plot()
                else:
                    # If grouping variable is present, use consistent colors for each
                    # group across all date cohorts
                    survival_table_pct[True].plot(color=RECIDIVIZ_COLORS[c_ix])
            c_ix = c_ix + 1

    # Manually create legend for `grouping_var`
    if grouping_var != "default_group":
        legend_patches = []
        c_ix = 0
        for group_name in group_names:
            legend_patches.append(
                mpatches.Patch(color=RECIDIVIZ_COLORS[c_ix], label=group_name)
            )
            c_ix = c_ix + 1
        plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left", handles=legend_patches)

    plt.title(title_text)
    plt.xlabel(x_text)

    # Align x-axis date labels with date cohorts
    x_axis_labels = [
        min_date + relativedelta(months=+x)
        for x in range(0, date_range + plot_timespan, cohort_spacing)
        if min_date + relativedelta(months=+x) <= datetime.date.today()
    ]
    plt.xticks(x_axis_labels, rotation=45)
    plt.xlim(min_date, max(x_axis_labels))

    if save_fig:
        plt.savefig(save_fig)
    plt.show()


def fuzzy_merge(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    exact_on: Optional[List[str]] = None,
    fuzzy_on: Optional[List[str]] = None,
    match_score_vars: Optional[List[str]] = None,
    match_threshold: int = 80,
) -> pd.DataFrame:
    """
    Function that performs a left join of one dataframe to another dataframe using a
    combination of strict and fuzzy text-matched keys. Returns dataframe containing a
    merged dataframes which may contain some fuzzy-matched row values.

    Params:
    -------
    left_df : pd.DataFrame
        Dataframe on the left of the merge, containing potentially erroneous keys that
        may require fuzzy matching.

    right_df : pd.DataFrame
        Dataframe on the right of the merge, containing reference keys.

    exact_on : list[str]
        List of column names in `df1` and `df2` to use for an exact merge between
        dataframes. If None, perform fuzzy matching on the entire dataframe.

    fuzzy_on : list[str]
        List of column names in `df1` and `df2` with which to perform a fuzzy merge
        between dataframes. If None, use the set of all column names that are shared
        across both dataframes.

    match_score_vars : list[str]
        List of column names in `df1` and `df2` to use for calculating match ratio score
        of the merge, calculated as the average of the match ratio score for each
        column. If None, use the set of all column names that are shared across both
        dataframes.

    match_threshold : int
        Number between 0 and 100 indicating the minimum acceptable match ratio for the
        concatenated strings from `match_score_vars` columns across df1 and df2, as
        determined by the fuzzywuzzy package. Default = 80.

    """

    df1 = left_df.copy()
    df2 = right_df.copy()

    common_cols = list(set(df1.columns) & set(df2.columns))
    df1_only_cols = [col for col in df1.columns if col not in common_cols]
    df2_only_cols = [col for col in df2.columns if col not in common_cols]

    # check for valid match score threshold
    if (match_threshold <= 0) or (match_threshold > 100):
        raise ValueError(f"match_threshold {match_threshold} is not in range (0, 100].")

    # if no variables are specified for calculating fuzzy matching score, use all shared
    # columns
    if not match_score_vars:
        match_score_vars = common_cols

    # if no variables are specified for performing fuzzy merge, use all shared columns
    if not fuzzy_on:
        fuzzy_on = common_cols
        print(f"Fuzzy matching on {', '.join(match_score_vars)}")

    # create a fuzzy match key in the reference dataset
    df2["fuzzy_match_key"] = df2[fuzzy_on].fillna("").agg(" ".join, axis=1)

    # create fuzzy match keys for scoring match quality
    for df in [df1, df2]:
        df["match_score_key"] = df[match_score_vars].fillna("").agg(" ".join, axis=1)

    # if no variables are specified for exact merge, perform fuzzy matching on entire
    # dataframe `df1`.
    if exact_on is None:
        merged_df = df1
        merged_df["match_ratio"] = 0
        exact_matches = pd.DataFrame(columns=["match_ratio"])
        exact_on = []
    else:
        # merge dataframes using exact key match
        merged_df = df1.merge(df2, how="left", on=exact_on)[
            exact_on
            + df1_only_cols
            + [f"{var}_x" for var in common_cols if var not in exact_on]
            + df2_only_cols
            + ["match_score_key_x", "match_score_key_y"]
        ]

        # calculate match score of merged dataframe based on string concatenation of
        # all match_score_vars
        merged_df["match_ratio"] = merged_df.apply(
            lambda x: fuzz.token_sort_ratio(
                x["match_score_key_x"], x["match_score_key_y"]
            ),
            axis=1,
        )
        # clean column names
        merged_df.columns = [
            c.replace("_x", "") if c.endswith("_x") else c for c in merged_df.columns
        ]

        # identify rows with match score that passes specified threshold
        exact_matches = merged_df[merged_df.match_ratio >= match_threshold].copy()

    # identify rows with insufficient match score
    bad_matches = merged_df[merged_df.match_ratio < match_threshold].copy()[
        list(set(exact_on + df1_only_cols + common_cols + ["match_score_key"]))
    ]
    print(f"Fuzzy matching {len(bad_matches)} rows...")

    # perform fuzzy text match to get the closest text matches
    bad_matches["fuzzy_match_key"] = pd.DataFrame(
        bad_matches[fuzzy_on]
        .fillna("")
        .agg(" ".join, axis=1)
        .map(
            lambda x: process.extractOne(
                x, df2.fuzzy_match_key, scorer=fuzz.token_sort_ratio
            )[0]
        )
        .tolist(),
        index=bad_matches.index,
    )

    fuzzy_matches = bad_matches.merge(df2, how="left", on="fuzzy_match_key")[
        df1_only_cols
        # for fuzzy join, use values from right side of join
        + [f"{var}_y" for var in common_cols]
        + df2_only_cols
        + ["match_score_key_x", "match_score_key_y"]
    ]
    fuzzy_matches["match_ratio_fuzzy"] = fuzzy_matches.apply(
        lambda x: fuzz.token_sort_ratio(x["match_score_key_x"], x["match_score_key_y"]),
        axis=1,
    )
    # clean column names
    fuzzy_matches.columns = [
        c.replace("_y", "") if c.endswith("_y") else c for c in fuzzy_matches.columns
    ]
    matched_df = pd.concat(
        [
            exact_matches.assign(is_fuzzy_match=False),
            fuzzy_matches.assign(is_fuzzy_match=True),
        ]
    )

    matched_df["match_ratio_fuzzy"] = matched_df.match_ratio_fuzzy.combine_first(
        matched_df.match_ratio
    )
    print(
        f"Num. fuzzy merged rows with match score < {match_threshold} "
        f"= {len(matched_df[matched_df.match_ratio_fuzzy < match_threshold])}"
    )

    return matched_df[
        df1_only_cols
        + df2_only_cols
        + common_cols
        + ["match_ratio_fuzzy", "is_fuzzy_match"]
    ]


# binned scatterplot function
def binned_scatterplot(
    df: pd.DataFrame,
    x_var: str,
    y_var: str,
    num_bins: Optional[int] = None,
    obs_per_bin: Optional[int] = None,
    equal_bin_width: Optional[bool] = False,
    measure: str = "mean",
    percentile: bool = False,
    plot_bar: bool = False,
    ax: Optional[matplotlib.axes.Axes] = None,
    label: Optional[str] = None,
    ylim: Optional[tuple] = None,
    xlim: Optional[tuple] = None,
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    legend: Optional[bool] = False,
    save_fig: Optional[str] = None,
    best_fit_line: Optional[bool] = False,
    control_vars: Optional[Union[str, List[str]]] = None,
) -> Union[None, pd.DataFrame]:
    """
    Plots a binned scatter plot
    Params:
    -------
    df : pd.DataFrame
        DataFrame containing `x_var` and `y_var`

    x_var : str
        Name of the column in `df` to be plotted on x-axis

    y_var : str
        Name of the column in `df` to be plotted on y-axis

    num_bins : int
        Number of bins to be created over `x-var`
        Instead of this a user can also provide `obs_per_bin`

    obs_per_bin : int
        Number of observations per bin
        Instead of this a user can also provide `num_bins`

    equal_bin_width : bool
        If True, equal width bins are created

    measure : str
        Aggregating measure for y column
        Acceptable options are min, max, mean, median or p_* where p_* is for percentile
        at * level, example p_20 = 20th percentile

    percentile : bool
        If True, plots percentile bins over `x-var`

    plot_bar : bool
        If True, plots as a bar plot instead of scatter

    ax : matplotlib.axes.Axes
        If provided, plots using existing axes, otherwise creates new axes object.

    label : str
        If provided, labels series with `label` (for legend).

    xlim : tuple
        Sets the range on x-axis

    ylim : tuple
        Sets the range on y-axis

    title : str
        Title of the plot

    xlabel : str
        Label for x-axis

    ylabel : str
        Label for y-axis

    legend : bool
        If True, adds a legend to the plot.

    save_fig : str
        Location and filename to save the figure

    best_line_fit : bool
        If True plots OLS fitted regression line in the plot

    control_vars : Optional[Union[str, List[str]]], default None
        List of string column names in `data` associated with other exogenous variables,
        or a string of the right side of the OLS formula consisting of other exogenous
        variables. The variation from these variables will be removed by OLS regression.
        Default (None) performs no residualization.
    """

    # warnings
    if num_bins and obs_per_bin:
        raise AttributeError(
            "num_bins and obs_per_bin both provided, please provide only one of them!"
        )

    if (not num_bins) and (not obs_per_bin):
        raise AttributeError(
            "neither num_bins nor obs_per_bin provided, please provide one of them!"
        )

    measures_list = ["mean", "median", "min", "max", "p"]

    if measure.split("_")[0] not in measures_list:
        raise AttributeError(f"Please provide a measure from {measures_list}")

    # init dataframe to be manipulated and returned
    if control_vars:
        main_df = partial_regression(
            data=df,
            x_var=x_var,
            y_var=y_var,
            control_vars=control_vars,
            return_append_string="",
        )
    else:
        main_df = df.copy()

    if obs_per_bin:
        # to get number of bins such that each bin has approximately `obs_per_bin`
        # observations
        num_bins = round(len(main_df) / obs_per_bin)

    # Creating necessary columns for making bins
    main_df["rank"] = main_df[x_var].rank(method="first")
    main_df["x_quantile"] = main_df[x_var].rank(method="first", pct=True) * 100

    # creating bins
    if equal_bin_width:
        main_df["bins"] = pd.cut(main_df[x_var], bins=num_bins)
    else:
        main_df["bins"] = pd.qcut(main_df["rank"], q=num_bins)

    # preparing the dataset for plotting
    if "p_" in measure:
        quantile_num = int(measure.split("_")[1]) / 100

        # a function to get nth percentile value, to be used in groupby aggregation
        def quantile_measure(group_data: pd.Series) -> pd.Series:
            return group_data.quantile(quantile_num)

        # aggregating a percentile for each bin
        plot_df = (
            main_df.groupby("bins")
            .agg({y_var: quantile_measure, x_var: "mean", "x_quantile": "mean"})
            .reset_index()
        )
    else:
        plot_df = (
            main_df.groupby("bins")
            .agg({y_var: measure, x_var: "mean", "x_quantile": "mean"})
            .reset_index()
        )

    # init plot
    if not ax:
        _, ax = plt.subplots()

    # plotting a scatter plot
    if plot_bar:
        if percentile:
            # quantile percentages on x-axis
            ax.bar("x_quantile", y_var, data=plot_df, label=label)
        else:
            ax.bar(x_var, y_var, data=plot_df, label=label)
    else:
        if percentile:
            # quantile percentages on x-axis
            ax.scatter(x=plot_df["x_quantile"], y=plot_df[y_var], label=label)
        else:
            # x_var units on the x-axis
            ax.scatter(x=plot_df[x_var], y=plot_df[y_var], label=label)

    # plotting best fitted (OLS) line
    if best_fit_line:
        reg_model = smf.ols(f"{y_var} ~ {x_var}", data=main_df).fit()

        # model params
        intercept = reg_model.params["Intercept"]
        slope = reg_model.params[x_var]

        x_min = min(plot_df[x_var])
        x_max = max(plot_df[x_var])
        y_hat_min = intercept + slope * x_min
        y_hat_max = intercept + slope * x_max

        # plotting the regression line
        ax.plot(
            [x_min, x_max],
            [y_hat_min, y_hat_max],
            color="r",
            label="Line of best fit",
        )

    # setting x and y axis limits
    ax.set_ylim(ylim)
    ax.set_xlim(xlim)

    # labels for the figure
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)

    # legend
    if legend:
        add_legend()

    if save_fig:
        plt.savefig(save_fig)

    return plot_df


def get_matched_sample(
    match_vars: dict,
    treated_df_query: str = "SELECT * FROM "
    "`recidiviz-staging.us_id_raw_data_up_to_date_views.geo_cis_participants_latest`",
    treated_df_start_date_var: str = "start_date",
    treated_df_variant_id_var: Optional[str] = None,
    external_id_type: str = "US_ID_DOC",
    project_id: str = "recidiviz-staging",
    experiment_id: str = "GEO_CIS_REFERRAL_MATCHED",
    trim_non_matches: bool = True,
    display_query: bool = False,
) -> pd.DataFrame:
    """
    Function that identifies coarsened exact matches to a set of specified treated units,
    and returns a dataframe of treated and matched control units along with `block_id`
    indicating matched pairs.

    Params:
    -------
    match_vars: dict[str, dict]
        Dictionary containing a key for every variable name to use for matching. Values
        are dictionaries mapping a string bucket value (e.g., "Non-White") to a list of
        all values that should be grouped under that value (e.g., ["BLACK", "HISPANIC",
        "ASIAN"]) OR a number indicating the upper bound of that category in ascending
        order.

        Supported keys:

            inflow_from_level_2, gender, prioritized_race_or_ethnicity,
            session_start_month, session_start_quarter, assessment_score,
             compartment_location_start, compartment_level_2, prior_incarceration

        Example `match_vars` that matches on gender, compartment_level_2,
        assessment_score, and prior_incarceration with discrete buckets for
        compartment_level_2 and continuous buckets for assessment_score.
            {
                "gender": {},
                "compartment_level_2": {
                    "PAROLE": ["PAROLE","DUAL"],
                    "PROBATION": ["PROBATION"]
                },
                "assessment_score": {
                    "0-23": (23),
                    "24-29": (29),
                    "30-38": (38),
                    "39+": (55),
                },
                "prior_incarceration": {}
            }


    treated_df_query: str
        A string query with person_external_id as key, consisting of all treated units
        to be matched as well as a `start_date` column indicating start of treatment.

    treated_df_start_date_var: str
        A string indicating the column name indicating treatment start date in the
        output of `treated_df_query`. Default: "start_date"

    treated_df_variant_id_var: str
        A string indicating the column name with the variant_id of a program with
        multiple treatment arms, if present. Default: None

    external_id_type: str
        String indicating `id_type` in `state.state_person_external_id` lookup table.

    project_id: str
        String indicating bigquery project id

    experiment_id: str
        String name of treatment

    trim_non_matches: bool
        If true, trim treated units to only include units with nonzero matching controls

    display_query: bool
        If true, prints final query string. Default: false

    """

    # helper function to generate SQL for bucket-izing columns
    def bucketize_col(name: str, bucket_dict: Optional[Dict[str, float]]) -> str:
        """
        Takes name of the column and a dict of labels:values for that column.

        For categorical columns, provide a list of acceptable values for each key.
        For example:

        name = compartment_level_2, bucket_dict = {
            "PAROLE": ("PAROLE", "DUAL"),
            ... AS compartment_level_2
        }

        Will return:

        CASE
            WHEN compartment_level_2 IN ("PAROLE", "DUAL") THEN "PAROLE"
            ...

        For numeric columns, provide cut-points that define buckets. For example:

        name = assessment_score, bucket_dict = {
            "0-23": 23,
            "24-29": 29,
        }

        Will return:

        CASE
            WHEN assessment_score <= 23 THEN "0-23"
            WHEN assessment_score <= 29 THEN "24-29"
            ... AS assessment_score
        """

        # first check `name` is supported
        if name not in [
            "inflow_from_level_2",
            "gender",
            "prioritized_race_or_ethnicity",
            "session_start_month",
            "session_start_quarter",
            "assessment_score",
            "compartment_location_start",
            "compartment_level_2",
            "prior_incarceration",
        ]:
            raise ValueError(f"{name} not a supported name")

        # ensure bucket_dict a dict
        if not isinstance(bucket_dict, dict):
            raise ValueError("bucket_dict must be a dict.")

        # handle empty bucket_dict non-empty
        if not bucket_dict:
            return name + ",\n"

        # init sql_str for non-empty bucket_dict cases
        sql_str = "CASE\n"
        keys = list(bucket_dict.keys())

        # handle categorical cases
        last_value = bucket_dict[keys[0]]
        if isinstance(last_value, list):
            for key in keys:
                in_group = str(list(bucket_dict[key]))[1:-1]
                sql_str += spacing + f"  WHEN {name} IN ({in_group}) THEN '{key}'\n"

        # handle numeric cases
        elif isinstance(last_value, (int, float)):
            for key in keys:
                sql_str += (
                    spacing + f"  WHEN {name} <= {bucket_dict[key]} THEN '{key}'\n"
                )

                # check that values are ascending
                if last_value > bucket_dict[key]:
                    raise ValueError(
                        "Numeric values in `bucket_dict` must be ascending."
                    )
                last_value = bucket_dict[key]

        else:
            raise ValueError("Format of bucket_dict is not supported.")

        # handle else clause and return
        sql_str += spacing + f"  ELSE 'UNKNOWN' END AS {name},\n"
        return sql_str

    # iterate through match_vars, appending to `bucketize_string` as we go
    spacing = "            "
    for i, var in enumerate(match_vars.keys()):
        buckets = match_vars[var]

        # init new SQL string
        if i == 0:
            bucketize_string = bucketize_col(var, buckets)

        # append new SQL string
        else:
            bucketize_string = spacing + bucketize_col(var, buckets)

    # remove final new line
    bucketize_string = bucketize_string[:-1]

    # get all columns in match_vars
    all_match_cols = ",".join(match_vars.keys())

    # get full query
    query = f"""

    WITH all_attributes_cte AS (
        SELECT
            person_external_id,
            variant_id,
            {bucketize_string}
            start_date,
            min_survival_days,
        FROM (
            SELECT DISTINCT
                external_id AS person_external_id,
                IFNULL(t.variant_id, "MATCHED_CONTROL") AS variant_id,

                -- Get days from supervision start to treatment (for treated units)
                -- or supervision session length (for control units)
                COALESCE(
                    DATE_DIFF(t.start_date, ss.start_date, DAY),
                    ss.session_length_days
                ) AS min_survival_days,

                COALESCE(ss.inflow_from_level_2, "RELEASE") AS inflow_from_level_2,
                s.gender,
                s.prioritized_race_or_ethnicity,
                ss.start_date,
                DATE_TRUNC(ss.start_date, MONTH) session_start_month,
                DATE_TRUNC(ss.start_date, QUARTER) session_start_quarter,
                a.assessment_score,
                SPLIT(s.compartment_location_start, "|")[SAFE_OFFSET(1)] as compartment_location_start,
                CASE WHEN s.compartment_level_2 = "DUAL" THEN "PAROLE"
                    ELSE s.compartment_level_2 END AS compartment_level_2,
                COUNT(history.start_date) OVER (PARTITION BY person_external_id, ss.start_date) > 0 AS prior_incarceration,
            FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized` ss
            INNER JOIN `{project_id}.sessions.compartment_sessions_materialized` s
                ON s.person_id = ss.person_id
                AND s.session_id BETWEEN ss.session_id_start AND ss.session_id_end
            INNER JOIN `{project_id}.state.state_person_external_id` pei
                ON pei.id_type = "{external_id_type}"
                AND pei.person_id = ss.person_id
            LEFT JOIN `{project_id}.sessions.assessment_score_sessions` a
                ON ss.person_id = a.person_id
                AND DATE(ss.start_date) BETWEEN a.assessment_date AND COALESCE(a.score_end_date, "9999-01-01")
            LEFT JOIN (
                -- Get only the first instance of treatment within a treatment arm for a given person.
                -- If no variant_id is provided in treatment dataframe, assign "TREATED" to variant_id.
                SELECT
                    person_external_id,
                    {treated_df_variant_id_var if treated_df_variant_id_var else '"TREATED"'} AS variant_id,
                    DATE({treated_df_start_date_var}) AS start_date
                FROM ({treated_df_query})
                WHERE TRUE
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY
                        person_external_id
                        {", treated_df_variant_id_var" if treated_df_variant_id_var else ""}
                    ORDER BY DATE({treated_df_start_date_var})

                ) = 1
            ) t
                ON
                    pei.external_id = t.person_external_id
                    AND t.start_date BETWEEN ss.start_date AND IFNULL(ss.end_date, "9999-01-01")

            LEFT JOIN `{project_id}.sessions.compartment_sessions_materialized` history
                ON
                    ss.person_id = history.person_id
                    AND ss.session_id_start > history.session_id
                    AND history.compartment_level_1 = "INCARCERATION"
            WHERE
                ss.state_code = "US_ID"
                AND ss.compartment_level_1 = "SUPERVISION"
                AND s.compartment_level_2 IN ("PAROLE", "PROBATION", "DUAL", "INFORMAL_PROBATION")
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY person_external_id, ss.start_date, variant_id
                ORDER BY t.start_date, s.start_date
            ) = 1
        )
    )
    ,
    -- Get distinct cross-sections of matching vars represented among treated units, and assign each a block_id
    -- in descending order of prevalence
    attribute_groups AS (
        SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY n DESC) AS block_id
        FROM (
            SELECT DISTINCT
                {all_match_cols},
                min_survival_days,
                COUNT(*) OVER (PARTITION BY {all_match_cols}) AS n
            FROM
                all_attributes_cte
            WHERE variant_id != "MATCHED_CONTROL"
        )

    ),

    merged_matches AS (
        SELECT
            person_external_id,
            block_id,
            variant_id,
            -- Variant date is treatment date for treated units,
            -- and time-to-treatment days after control unit's supervision start for control units

            DATE_ADD(start_date, INTERVAL attribute_groups.min_survival_days DAY) AS variant_date,
            all_attributes_cte.min_survival_days,
            {all_match_cols},
            n AS treated_block_size,
            COUNT(DISTINCT IF(variant_id = "MATCHED_CONTROL", person_external_id, NULL))
                OVER (PARTITION BY block_id) AS control_block_size,
        FROM
            attribute_groups
        LEFT JOIN
            all_attributes_cte
        USING
            ({all_match_cols})
        WHERE
            (
                all_attributes_cte.variant_id != "MATCHED_CONTROL"
                AND attribute_groups.min_survival_days = all_attributes_cte.min_survival_days
            )
            OR
            (
                all_attributes_cte.variant_id = "MATCHED_CONTROL"
                AND attribute_groups.min_survival_days <= all_attributes_cte.min_survival_days
            )
    )

    SELECT
        *
    FROM
        merged_matches

    """

    if display_query:
        print(query)

    matched_sample = pd.read_gbq(query)

    sample_gbq = (
        matched_sample[
            (matched_sample.treated_block_size > 0)
            & (matched_sample.control_block_size >= 1 if trim_non_matches else 0)
        ]
        .assign(experiment_id=experiment_id, state_code="US_ID")
        .sort_values(["block_id", "variant_id"])
    )

    return sample_gbq


def gen_cohort_time_to_first_event(
    cohort_df: pd.DataFrame,
    event_df: pd.DataFrame,
    cohort_date_field: str,
    event_date_field: str,
    join_field: Optional[Union[str, List[str]]] = "person_id",
    # TODO(#625): Add support for non-binary event counts where subsequent events are
    # not deduplicated
) -> pd.DataFrame:
    """
    Returns pandas DataFrame consisting of cohort starts and outcome events deduplicated
    with the dates of each.

    Params:
    ------
    cohort_df: pandas DataFrame
        Dataframe of all cohort starts that will be evaluated as having a subsequent
        event. Must have a field indicating the cohort start date as well as a id field
        that joins the individual to the events in `event_df`. This dataframe can have
        any other cohort attributes included as well, which will flow through to the
        output.

    event_df: pandas DataFrame
        Dataframe of all events that will be evaluated against cohort start. Must have a
        field indicating the event date as well as an id field that joins to the
        individual in `cohort_df`. This dataframe can have any other event attributes as
        well, which will flow through to the output.

    cohort_date_field: string
        Field name in `cohort_df` that indicates the cohort start date.

    event_date_field: string
        Field name in `event_df` that indicates the event date.

    join_field: Optional[Union[str, List[str]]]
        Field name (or list of field names) in both `cohort_df` and `event_df` joins
        these two tables together when dates are compared.
    """
    if isinstance(join_field, str):
        join_field_list = [join_field]
    elif isinstance(join_field, list):
        join_field_list = join_field
    else:
        raise ValueError("`join_field` not a string or list of strings.")

    # Check that all necessary columns are present in cohort_df
    for var in [cohort_date_field] + join_field_list:
        if var not in cohort_df.columns:
            raise AttributeError(f"Column {var} can not be found in `cohort_df`.")

    # Check that all necessary columns are present in event_df
    for var in [event_date_field] + join_field_list:
        if not var in event_df.columns:
            raise AttributeError(f"Column {var} can not be found in `event_df`.")

    # Convert date fields
    cohort_df[cohort_date_field] = pd.to_datetime(
        cohort_df[cohort_date_field], errors="coerce"
    )
    event_df[event_date_field] = pd.to_datetime(
        event_df[event_date_field], errors="coerce"
    )

    # Join person and cohort dates for each person. Results in a record for every cohort
    # start and every event. Drop the cohort attributes from this one since we will join
    # back to that table at the end
    cohort_x_event = pd.merge(
        cohort_df[join_field_list + [cohort_date_field]],
        event_df,
        on=join_field,
        how="inner",
    )

    # Subset for the set of start/event records for which the event date is after the
    # cohort start date
    # These are the only ones that should be eligible
    cohort_x_event = cohort_x_event[
        cohort_x_event[event_date_field] > cohort_x_event[cohort_date_field]
    ]

    # Create ranking of events within each cohort start, giving a value of 1 to the
    # earliest event that follows the cohort start
    event_ranking = (
        cohort_x_event.groupby(join_field_list + [cohort_date_field])[event_date_field]
        .rank(method="min", ascending=True)
        .astype(int)
    )

    # Create ranking of starts within each event, giving a value of 1 to the latest
    # cohort that precedes the event
    cohort_ranking = (
        cohort_x_event.groupby(join_field_list + [event_date_field])[cohort_date_field]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # Use the rankings to reduce to a one-to-one mapping of cohort start to event
    cohort_x_event = cohort_x_event[(event_ranking == 1) & (cohort_ranking == 1)]

    # Join the matched starts/events back to the original cohort dataframe to capture
    # cohort starts not followed by event
    df = pd.merge(
        cohort_df, cohort_x_event, how="left", on=join_field_list + [cohort_date_field]
    )

    return df


def calendar_unit_date_diff(start_date: str, end_date: str, time_unit: str) -> int:
    """
    Returns an integer representing the number of full calendar units (specified with
    `time_unit`) that have passed between the `start_date` and `end_date`. Supported
    unit options are "days", "months", "years". As an example, with a start_date of
    "2018-02-15" and an end_date of "2018-04-14", 60 days have passed, but only 1 full
    calendar month has passed. With an end_date value of "2018-04-16" the output would
    change to "2" because two full calendar months have passed at that point.

    Params:
    ------
    start_date: string
        Date in string format representing the start date

    end_date: string
        Date in string format representing the end date

    time_unit: string
        Unit of time that date difference calculation outputs. Can be "years", "months",
        or "days"
    """

    # Check that time unit option is in supported list
    time_unit_options = ["days", "months", "years"]
    if time_unit not in time_unit_options:
        raise ValueError(f"Invalid time unit. Expected one of: {time_unit_options}")

    # Convert strings to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # check dates formatted correctly, otherwise throw error
    # this is required for mypy since pd.to_datetime doesn't necessarily return
    # a datetime object.
    if not (
        isinstance(start_date, datetime.datetime)
        and isinstance(end_date, datetime.datetime)
    ):
        raise ValueError(
            "Could not format `start_date` or `end_date` as datetime objects."
        )

    # Check that start date precedes the end date
    if end_date < start_date:
        raise ValueError("`end_date` must be >= `start date`")

    if time_unit == "days":
        diff_result = (end_date - start_date).days

    if time_unit == "months":
        # first calculate the calendar number of months between the dates
        month_num_diff = (end_date.month - start_date.month) + 12 * (
            end_date.year - start_date.year
        )

        # account for day of the month. Subtract 1 from the number of months that have
        # passed if the end date day of the month is less than the start date day of the
        # month because in those cases a "full month" has not passed
        diff_result = (
            month_num_diff - 1 if start_date.day >= end_date.day else month_num_diff
        )

    else:  # time_unit == "years"
        year_num_diff = end_date.year - start_date.year

        # account for day of the year/month. Subtract 1 from the number of years that
        # have passed if the end date day of the month/year is less than the start date
        # day of the month/year because in those cases a "full year" has not passed
        diff_result = (
            year_num_diff - 1
            if start_date.month >= end_date.month and start_date.day >= end_date.day
            else year_num_diff
        )

    return diff_result


def gen_aggregated_cohort_event_df(
    df: pd.DataFrame,
    cohort_date_field: str,
    event_date_field: str,
    time_index: Iterable[int] = (0, 6, 12, 18, 24, 36),
    time_unit: str = "months",
    last_day_of_data: Optional[datetime.datetime] = None,
    cohort_attribute_col: Optional[Union[str, List[str]]] = None,
    event_attribute_col: Optional[Union[str, List[str]]] = None,
) -> pd.DataFrame:
    """
    Returns an dataframe aggregated by cohort time index as well as any cohort or event
    attributes and calculates the cohort size at that time index, the number of events
    that have occurred by that time index, and the event rate by that time index.

    Params:
    ------
    df: pandas DataFrame
        DataFrame with one row for each cohort start. Must have a cohort start date and
        event date which is used to determine when the event occurred relative to the
        cohort start.

    cohort_date_field: string
        Field name in `df` that indicates the cohort start date.

    event_date_field: string
        Field name in `df` that indicates the event date.

    time_index: list
        List of integers that specify the time units from cohort start that event rate
        will be evaluated against. These can represent days, months or years.

    time_unit: str
        Unit of time that the `time_index` represents. Can be "years", "months", or
        "days"

    last_day_of_data: str
        Day representing the last day of available data. If none is provided, the max
        event date is used. This is used to ensure that event rate calculates only use
        cohort starts that have had the full amount of time to mature.

    cohort_attribute_col: Optional[Union[str, List[str]]]
        Single field or list of fields in `df` that represent attributes of the cohort
        that the event rate should be disaggregated by. If no list is specified, the
        overall event rate is calculated.

     event_attribute_col: Optional[Union[str, List[str]]]
        Single field or list of fields in `df` that represent attributes of the event
        that the event rate should be disaggregated by. If no list is specified, the
        overall event rate is calculated.
    """

    # handle attribute columns which can be a list, a string, or None
    if not cohort_attribute_col:
        cohort_attribute_col_list = []
    elif isinstance(cohort_attribute_col, str):
        cohort_attribute_col_list = [cohort_attribute_col]
    else:
        cohort_attribute_col_list = cohort_attribute_col

    if not event_attribute_col:
        event_attribute_col_list = []
    elif isinstance(event_attribute_col, str):
        event_attribute_col_list = [event_attribute_col]
    else:
        event_attribute_col_list = event_attribute_col

    # Check that all specified columns are present in df
    for var in (
        [cohort_date_field, event_date_field]
        + cohort_attribute_col_list
        + event_attribute_col_list
    ):
        if not var in df.columns:
            raise AttributeError(f"Column {var} can not be found in `df`.")

    # Set the last day of data to be the max event date if not specified. Print the date
    # let user know, just to make sure this assumption seems correct. This is used to
    # determine which subset of the cohort starts are eligible for each calculation
    if not last_day_of_data:
        last_day_of_data = pd.to_datetime(df[event_date_field].max())
        if not isinstance(last_day_of_data, datetime.datetime):
            raise ValueError(
                "Could not format `event_date_field` as a datetime object."
            )
        print(
            f"Last day of data not specified. Assuming to be {last_day_of_data.date()} "
            f"based on event dates"
        )

    # create a dataframe with the specified time units
    cohort_index_field = f"cohort_{time_unit}"
    time_df = pd.Series(time_index, name=cohort_index_field).to_frame()

    time_to_mature_field = f"{time_unit}_to_mature"
    time_to_event_field = f"{time_unit}_to_event"

    # Use the date differencing function to calculate time that the cohort has to mature
    # (based on the last day of data) as well as time between the cohort start and the
    # event. Note that the time to event field has one time unit added to it while the
    # time to mature field does not. This is consistent with methodology we use in
    # Looker. If a person starts on '2021-01-01' and has a subsequent event on
    # '2021-02-15' the month difference output would be 1 month since 1 full month has
    # passed, but the event occurred within 2 full months. Therefore we add 1 to this
    # output since we want the event rate to represent the number of events that
    # occurred within that number of months. And if the last day of available data was
    # '2021-03-15', the month date difference output would be 2, and for that field we
    # keep as is because we want that person to be eligible for 1-month and 2-month
    # calculations, but not 3-month since 3 full months have not passed
    df[time_to_mature_field] = df.apply(
        lambda x: calendar_unit_date_diff(
            x[cohort_date_field], last_day_of_data.strftime("%Y-%m-%d"), time_unit
        ),
        1,
    )
    df[time_to_event_field] = (
        df.apply(
            lambda x: calendar_unit_date_diff(
                x[cohort_date_field], x[event_date_field], time_unit
            ),
            1,
        )
        + 1
    )

    # cross join the dataframe with the time index dataframe
    df_unnest = pd.merge(df, time_df, how="cross")

    # subset for those records in which the cohort has fully matured by the time index
    df_unnest = df_unnest[
        df_unnest[time_to_mature_field] >= df_unnest[cohort_index_field]
    ]

    # calculate a field "event" that is populated when the event occurred within the
    # cohort time index value
    df_unnest.loc[
        df_unnest[time_to_event_field] <= df_unnest[cohort_index_field], "event"
    ] = 1
    df_unnest["event"] = df_unnest["event"].fillna(0).astype(int)

    # create aggregated views to generate cohort size and event count
    # the cohort size is only grouped by the cohort index field and any cohort
    # attributes while the event count is also further disaggregated by any event
    # attributes. These series are then joined together based on time index and
    # cohort attributes (any event disaggregations will have the same cohort size).
    cohort_size = df_unnest.groupby(
        [cohort_index_field] + cohort_attribute_col_list
    ).size()
    event_cnt = df_unnest.groupby(
        [cohort_index_field] + cohort_attribute_col_list + event_attribute_col_list
    )["event"].sum()
    cohort_size.name = "cohort_size"
    event_cnt.name = "event_count"

    result_df = event_cnt.to_frame().join(cohort_size)
    result_df["event_rate"] = result_df["event_count"] / result_df["cohort_size"]

    return result_df


def plot_event_curves(
    df: pd.DataFrame,
    x_var: str,
    event_var: str = "event",
    group_by: Optional[Union[str, List[str]]] = None,
    ci: bool = False,
    alpha: float = 0.05,
    n_var: Optional[str] = None,
    label: bool = True,
    title_text: str = "Cumulative event rate",
    legend_text: str = "",
    x_text: str = "",
    y_text: str = "",
    yline: Optional[int] = None,
) -> None:
    """
    Function that takes a dataframe with the pre-aggregated cumulative event rate of a
    binary outcome over time intervals, and generates a line plot with optional
    calculated confidence interval bands.

    Params:
    ------
    df : pd.DataFrame
        Dataframe containing columns indicated by `x_var`, `event_var`, `group_vars`,
        and optionally `n_var` if confidence bands are desired.

    x_var : str
        Name of the column in `df` indicating time interval at which event rate was
        assessed, to be plotted on the x-axis.

    event_var : str
        Name of the column in `df` indicating proportion of cohort that has experienced
        an event by time interval indicated by `x_var`, to be plotted on the y-axis.

    group_by : Optional[Union[str, List[str]]]
        Name of the column or columns in `df` indicating the variable(s) over which
        event rates were disaggregated. If no disaggregation, default is None.

    ci : bool
        If True, plots confidence interval bands at the specified `alpha` level.

    alpha : float
        Significance level for a two-tailed significance test to generate confidence
        interval. Default is 0.05, and will generate a 95% confidence interval band
        using normal standard errors.

    n_var : str
        Name of the column in `df` indicating the number of units for a given row.
        `n_var` must be specified if ci is True.

    label : bool
        If True, line plots include an annotation of the final event rate for each
        cohort at the max value of `x_var`.

    title_text (Optional) : str
        Text for plot title.

    legend_text (Optional) : str
        Text for legend title.

    x_text (Optional) : str
        Text for x axis label.

    y_text (Optional) : str
        Text for y axis label.

    y_line (Optional) : int
        Dotted horizontal line that can be plotted as reference.
    """
    if group_by:
        if isinstance(group_by, str):
            group_by_list = [group_by]
        else:
            group_by_list = group_by
    else:
        group_by_list = []

    # if there are two group by variables specified, use the first one to assign colors
    # and the second one to assign line styles
    if len(group_by_list) == 2:
        color_grouping = group_by_list[0]
        style_grouping = group_by_list[1]
    else:
        color_grouping = None
        style_grouping = None

    # Check that all columns are present in dataframe
    for var in [x_var, event_var] + group_by_list:
        if var not in df.columns:
            raise AttributeError(f"Column {var} can not be found in dataframe.")

    # If there is no specified grouping variable, create a dummy group
    # If there is a number of grouping variables != 2 (ie 1 or >=3), create a dummy
    # secondary variable for line styles
    if not group_by:
        df["group_1"] = 1
        df["group_2"] = 1
    # Concatenate all grouping columns with comma separators into a new "group" column.
    if color_grouping:
        df["group_1"] = df[color_grouping]
        df["group_2"] = df[style_grouping]
    else:
        df["group_1"] = df[group_by_list].apply(
            lambda row: ", ".join(row.values.astype(str)), axis=1
        )
        df["group_2"] = 1

    # Check that there are no duplicate rows by `groupby` variable.
    if df.groupby([x_var] + group_by_list).size().max() > 1:
        raise AttributeError(
            f"Dataframe values are not unique on {x_var} and [{', '.join(group_by_list)}]."
        )

    # check that if ci is true, n_var is provided
    if ci and ((not n_var) or (n_var not in df.columns)):
        raise AttributeError(
            "If ci = True, must provide a valid `n_var` from dataframe."
        )

    # plot line plot with specified color and style groupings if there are two group by
    # variables, otherwise plot by all unique group by combinations with different colors
    bin_labels_1 = df["group_1"].unique()
    bin_labels_2 = df["group_2"].unique()
    marker_list = ["-", "--", "-.", ":", ".", "o", "v", "*", "+", ","]

    _, ax = plt.subplots()
    # for each group in style group
    for ii, _ in enumerate(sorted(bin_labels_2)):
        # for each group in color group
        for i, _ in enumerate(sorted(bin_labels_1)):
            df_subset = df[
                (df["group_1"] == bin_labels_1[i]) & (df["group_2"] == bin_labels_2[ii])
            ]
            ax.plot(
                df_subset[x_var],
                df_subset[event_var],
                linestyle=marker_list[ii % len(marker_list)],
                color=RECIDIVIZ_COLORS[i % len(RECIDIVIZ_COLORS)],
                marker="o",
                markeredgecolor="white",
            )
            if ci:
                z = norm.ppf(1 - alpha / 2)
                se = np.sqrt(
                    df_subset[event_var] * (1 - df_subset[event_var]) / df_subset[n_var]
                )
                y1 = df_subset[event_var] - z * se
                y2 = df_subset[event_var] + z * se

                plt.fill_between(
                    df_subset[x_var],
                    y1,
                    y2,
                    alpha=0.25,
                    color=RECIDIVIZ_COLORS[i % len(RECIDIVIZ_COLORS)],
                )
            if label:
                max_x = max(df_subset[x_var])
                label_coord = df_subset.sort_values(x_var, ascending=False).iloc[0]
                plt.annotate(
                    f"{round(label_coord[event_var] * 100)}%",
                    (max_x, label_coord[event_var]),
                    textcoords="offset points",
                    xytext=(20, -2),
                    ha="center",
                    color=RECIDIVIZ_COLORS[i % len(RECIDIVIZ_COLORS)],
                )
    if yline:
        ax.axhline(y=yline, linestyle="--", color="grey", alpha=0.5)
    # create custom legend
    custom_lines = []
    labels = []
    if color_grouping:
        for i, _ in enumerate(sorted(bin_labels_1)):
            custom_lines.append(
                Line2D(
                    [0], [0], color=RECIDIVIZ_COLORS[i % len(RECIDIVIZ_COLORS)], lw=2
                )
            )
            labels.append(bin_labels_1[i])
        for ii, _ in enumerate(sorted(bin_labels_2)):
            custom_lines.append(
                Line2D(
                    [0],
                    [0],
                    color="black",
                    lw=2,
                    linestyle=marker_list[ii % len(marker_list)],
                )
            )
            labels.append(bin_labels_2[ii])
        ax.legend(
            custom_lines,
            labels,
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            title=legend_text,
        )
    elif group_by:
        for i, _ in enumerate(sorted(bin_labels_1)):
            custom_lines.append(
                Line2D(
                    [0], [0], color=RECIDIVIZ_COLORS[i % len(RECIDIVIZ_COLORS)], lw=2
                )
            )
            labels.append(bin_labels_1[i])
        ax.legend(
            custom_lines,
            labels,
            bbox_to_anchor=(1.05, 1),
            loc="upper left",
            title=legend_text,
        )

    plt.title(title_text)
    plt.xlabel(x_text)
    plt.ylabel(y_text)
    plt.xticks(sorted(df[x_var].unique()))
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1, decimals=0))
    plt.show()
