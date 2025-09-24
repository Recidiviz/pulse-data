# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tools for plotting"""

from __future__ import annotations

import math
from os.path import abspath, dirname

# imports for notebooks
from typing import Any, Callable, List, Optional, Tuple, Union

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from matplotlib import axes
from matplotlib.ticker import MaxNLocator
from matplotx._labels import _move_min_distance

from recidiviz.tools.analyst.estimate_effects import partial_regression

# Recidiviz plotting colors
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


# Plot settings
def plot_settings(
    ax: Optional[plt.Axes] = None,
    *,
    title: str = "",
    subtitle: str = "",
    subtitle_offset_x: float = 0,
    subtitle_offset_y: float = 0,
    y_label: str = "",
    x_label: str = "",
    y_max: Optional[int] = None,
    y_min: int = 0,
    x_max: Optional[int] = None,
    x_min: Optional[int] = None,
    exclude_legend: bool = False,
    remove_gridlines: bool = False,
    only_integer_ticks_in_x_axis: bool = False,
    only_integer_ticks_in_y_axis: bool = False,
) -> None:
    """
    Configures the settings for a plot.

    Args:
        ax (Optional[plt.Axes]): The axes object to configure. If None, uses the current axes (default: None).
        title (str): The main title of the plot.
        subtitle (str): The subtitle of the plot, positioned below the main title.
        subtitle_offset_x (float): The horizontal alignment for the subtitle, relative to the figure area (default: 0).
            Positive values bring it closer to the right edge.
        subtitle_offset_y (float): The vertical alignment for the subtitle, relative to the figure area (default: 0).
            Positive values bring it closer to the top edge.
        y_label (str): The label for the y-axis.
        x_label (str): The label for the x-axis.
        y_max (Optional[int]): The upper limit for the y-axis. If None, the y-axis will be automatically scaled (default: None).
        y_min (int): The lower limit for the y-axis (default: 0).
        x_max (Optional[int]): The upper limit for the x-axis. If None, the x-axis will be automatically scaled (default: None).
        x_min (Optional[int]): The lower limit for the x-axis (default: None).
        include_legend (bool): Whether to include a legend in the plot (default: True). If you want to customize the legend,
            use the `add_legend` function
        remove_gridlines (bool): Whether to remove gridlines from the plot (default: False).
        only_integer_ticks_in_x_axis (bool): Whether to only show integer ticks on the x-axis (default: False).
        only_integer_ticks_in_y_axis (bool): Whether to only show integer ticks on the y-axis (default: False).
    """
    if ax is None:
        ax = plt.gca()

    # Title
    pad = plt.rcParams["axes.titlepad"]
    if subtitle != "":
        # Increase padding if there is a subtitle
        pad *= 2
    ax.set_title(title, pad=pad)

    # Calculate the scale compared to recidiviz.mplstyle
    path = dirname(abspath(__file__))
    recidiviz_params = matplotlib.rc_params_from_file(
        path + "/recidiviz.mplstyle", use_default_template=False
    )
    # Actual parameter / recidiviz parameter
    scale_factor = plt.rcParams["font.size"] / recidiviz_params["font.size"]

    ### Calculate the y position of the subtitle
    # These values were chosen based on a linear regression on the scale factor and
    # what we thought was the right y position for the subtitle. So they minimize
    # the amount of manual adjustment needed for the subtitle.
    estimated_subtitle_y = 1.05 - 0.08 * scale_factor

    ### Subtitle
    # Reminder: plt.suptitle is a Figure-level function, this subtitle functionality
    # is only going to work correctly in cases where you're applying this to a
    # single-paneled plot.
    plt.suptitle(
        t=subtitle,
        x=plt.rcParams["figure.subplot.left"] + subtitle_offset_x,
        y=estimated_subtitle_y + subtitle_offset_y,
        ha="left",
        size=plt.rcParams["axes.titlesize"] / 1.4,
        weight="medium",
        color="#585858",
    )

    # Axis labels
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    # Upper and lower limits for x-axis
    ax.set_xlim(left=x_min, right=x_max)
    # Upper and lower limits for y-axis
    ax.set_ylim(bottom=y_min, top=y_max)

    # Remove legend if requested
    if exclude_legend:
        legend = ax.legend()
        legend.remove()

    # Remove gridlines if requested
    if remove_gridlines:
        ax.grid(False)

    # Set the x-axis and y-axis to have integer ticks only
    if only_integer_ticks_in_x_axis:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    if only_integer_ticks_in_y_axis:
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))


# Plotting resize method
def adjust_plot_scale(scale_factor: float = 1.0) -> None:
    """
    Adjusts size of plot scaling text and plot area proportionally, starting from the
    values set in recidiviz.mplstyle. In other words, we are scaling based off the style
    parameters in our .mplstyle file and not based on the (potentially already re-scaled)
    parameters that may have been used in a previous notebook run

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
        "axes.titlepad",
        "font.size",
        "figure.titlesize",
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

    # We start from recidiviz.mplstyle and scale from there
    path = dirname(abspath(__file__))
    plt.style.use(path + "/recidiviz.mplstyle")

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


# Add a legend to your plot
def add_legend(
    ax: Optional[plt.Axes] = None,
    title: Optional[str] = None,
    location: tuple[float, float] = (1, 0.5),
    labels: Optional[List[str]] = None,
    format_labels: bool = False,
    label_formatter: Callable = lambda s: s.title().replace("_", " ").replace("-", " "),
    reverse: bool = False,
    **kwargs: Optional[Any],
) -> None:
    """
    Add a legend to the plot with the option to customize title, location, labels,
    formatting, and order.

    Args:
        ax (Optional[plt.Axes]): The Axes object to add the legend to (default: None).
        title (Optional[str]): The title of the legend (default: None).
        location (tuple[float, float]): The location of the legend on the plot.
        labels (Optional[List[str]]): A list of labels to replace the original labels in
            the legend (default: None).
        format_labels (bool): Whether to format the legend labels (default: False). If
            True, the labels will be formatted using the formatting function.
        label_formatter (Callable): Function to use to format the legend labels.
            Default title-cases the labels and replaces underscores and hyphens with
            spaces.
        reverse (bool): Whether to reverse the order of labels/handles in the legend.
            Default: False.
        **kwargs: Additional keyword arguments to pass to the `plt.legend()` function.
    """
    if ax is None:
        ax = plt.gca()

    # Get handles and original labels
    handles, legend_labels = ax.get_legend_handles_labels()

    # Overwrite legend labels (if provided)
    if labels:
        legend_labels = labels

    # Format labels (if selected)
    if format_labels:
        legend_labels = [
            label_formatter(str(legend_label)) for legend_label in legend_labels
        ]

    # Reverse order if requested
    if reverse:
        handles, legend_labels = handles[::-1], legend_labels[::-1]

    # Add the legend
    plt.legend(
        handles=handles, labels=legend_labels, title=title, loc=location, **kwargs
    )


# binned scatterplot function
def binned_scatterplot(
    df: pd.DataFrame,
    *,
    x_column: str,
    y_column: str,
    num_bins: Optional[int] = None,
    obs_per_bin: Optional[int] = None,
    equal_bin_width: Optional[bool] = False,
    measure: str = "mean",
    percentile: bool = False,
    plot_bar: bool = False,
    ax: Optional[matplotlib.axes.Axes] = None,
    label: Optional[str] = None,
    ylim: Optional[tuple[float, float]] = None,
    xlim: Optional[tuple[float, float]] = None,
    title: Optional[str] = None,
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    legend: Optional[bool] = False,
    save_fig: Optional[str] = None,
    best_fit_line: Optional[bool] = False,
    control_columns: Optional[Union[str, List[str]]] = None,
) -> Union[None, pd.DataFrame]:
    """
    Plots a binned scatter plot
    Params:
    -------
    df : pd.DataFrame
        DataFrame containing `x_column` and `y_column`

    x_column : str
        Name of the column in `df` to be plotted on x-axis

    y_column : str
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

    control_columns : Optional[Union[str, List[str]]], default None
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
    if control_columns:
        main_df = partial_regression(
            data=df,
            x_column=x_column,
            y_column=y_column,
            control_columns=control_columns,
            return_append_string="",
        )
    else:
        main_df = df.copy()

    if obs_per_bin:
        # to get number of bins such that each bin has approximately `obs_per_bin`
        # observations
        num_bins = round(len(main_df) / obs_per_bin)

    # Creating necessary columns for making bins
    main_df["rank"] = main_df[x_column].rank(method="first")
    main_df["x_quantile"] = main_df[x_column].rank(method="first", pct=True) * 100

    # creating bins
    if equal_bin_width:
        main_df["bins"] = pd.cut(main_df[x_column], bins=num_bins)
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
            .agg({y_column: quantile_measure, x_column: "mean", "x_quantile": "mean"})
            .reset_index()
        )
    else:
        plot_df = (
            main_df.groupby("bins")
            .agg({y_column: measure, x_column: "mean", "x_quantile": "mean"})
            .reset_index()
        )

    # init plot
    axis: axes.Axes = ax or plt.subplots()[1]

    # plotting a scatter plot
    if plot_bar:
        if percentile:
            # quantile percentages on x-axis
            axis.bar("x_quantile", y_column, data=plot_df, label=label)
        else:
            axis.bar(x_column, y_column, data=plot_df, label=label)
    else:
        if percentile:
            # quantile percentages on x-axis
            axis.scatter(x=plot_df["x_quantile"], y=plot_df[y_column], label=label)
        else:
            # x_column units on the x-axis
            axis.scatter(x=plot_df[x_column], y=plot_df[y_column], label=label)

    # plotting best fitted (OLS) line
    if best_fit_line:
        reg_model = smf.ols(f"{y_column} ~ {x_column}", data=main_df).fit()

        # model params
        intercept = reg_model.params["Intercept"]
        slope = reg_model.params[x_column]

        x_min = min(plot_df[x_column])
        x_max = max(plot_df[x_column])
        y_hat_min = intercept + slope * x_min
        y_hat_max = intercept + slope * x_max

        # plotting the regression line
        axis.plot(
            [x_min, x_max],
            [y_hat_min, y_hat_max],
            color="r",
            label="Line of best fit",
        )

    # setting x and y axis limits
    axis.set_ylim(ylim)
    axis.set_xlim(xlim)

    # labels for the figure
    if title:
        axis.set_title(title)
    if ylabel:
        axis.set_ylabel(ylabel)
    if xlabel:
        axis.set_xlabel(xlabel)

    # legend
    if legend:
        add_legend()

    if save_fig:
        plt.savefig(save_fig)

    return plot_df


def group_into_other(
    df: pd.DataFrame,
    col_name: str,
    vals_to_keep: Union[int, list[str]] = 3,
    other_group_name: str = "OTHER",
    verbose: bool = True,
) -> None:
    """
    Adds a column in the specified dataframe called col_name + _other, which renames everything except the specified values as 'OTHER' for plotting purposes
    Params:
    -------
    df : pd.DataFrame()
        existing dataframe

    col_name : str
        name of column to group

    vals_to_keep : int | list
        if int, this means you want to keep the top n values (by frequency) and group the rest into 'OTHER'
            by default, keep the top 3 values
        if list, this means you want to keep the specified list of values and group the rest into 'OTHER'

    other_group_name : str
        string that any value in the "other" group will be replaced with, 'OTHER' by default

    verbose: bool
        if True, this prints out the values that were grouped into other and the new column name
    """

    if isinstance(vals_to_keep, list):
        vals_to_keep_list = vals_to_keep
    else:
        vals_to_keep_list = df[col_name].value_counts()[0:vals_to_keep].index.to_list()

    df[col_name + "_other"] = df[col_name].apply(
        lambda x: x if x in vals_to_keep_list else other_group_name
    )

    all_vals = list(df[col_name].unique())
    other_vals = [x for x in all_vals if x not in vals_to_keep_list]

    if verbose:
        print("New column created: " + col_name + "_other")
        print("Values grouped into other:", end=" ")
        print(other_vals)


# create labels next to lines in a line plot, rather than creating a legend
# modified version of line_labels fxn in matplotx library: https://github.com/nschloe/matplotx/tree/main
def line_labels(
    ax: Optional[plt.Axes] = None,
    vertical_distance: float = float("nan"),
    horizontal_distance: float = 0.98,
    color: str = "auto",
    weight: Union[int, str] = 551,
    format_labels: bool = False,
    label_formatter: Callable = lambda s: s.title().replace("_", " ").replace("-", " "),
    **text_kwargs: Optional[Any],
) -> None:
    """
    Adds labels next to lines rather than in a traditional legend.

    Params:
    -------
    ax: plt.Axes
        axes of existing plot. if no axes is passed in, this fxn creates one using plt.gca()
    vertical_distance: float
        min distance that labels should be separated from one another, larger values means more vertical separation
        default is set to nan, which automatically calculates an ideal vertical separation
    horizontal_distance: float
        distance that labels should be separated from their line, larger values means more space between the line and the label
    color: str
        specifies label color. defaults to 'auto', which sets the labels to be the same color as the lines
    weight: int
        specifies label weight. 400 is normal font, 700 is bold
        default is 551, which is an arbitrary number chosen to look a little bit heavier due to the use of light colors
    format_labels: bool
        Whether to format the legend labels (default: False). If True, the labels will
        be formatted using the formatting function.
    label_formatter: lambda
        specifies the string formatting of the line labels (e.g. line_labels(label_formatter = lambda s: s.lower()))
        by default, converts labels to title case and replaces hyphens and underscores with spaces
        This also accepts string values like 'light', 'normal', 'medium', 'bold', 'extra bold'.
    **text_kwargs
        the user can specify any other matplotlib text arguments (font, size, etc.)
        see https://matplotlib.org/stable/api/text_api.html for all options

    Example usage:
    ----------
    # make sure to run recidiviz/tools/analyst/notebook_utils.py in your notebook first
     df = pd.DataFrame({
        "Reading": [73, 74, 83, 90, 76, 25, 67, 81, 75, 82],
        "Math": [88, 91, 92, 85, 85, 89, 90, 87, 87, 93]
    })
    ax = df.plot()
    line_labels()
    """
    # TODO(#33313) - make this fxn work with stacked plots
    if ax is None:
        ax = plt.gca()

    if legend := ax.get_legend():
        legend.remove()

    logy = ax.get_yscale() == "log"

    if math.isnan(vertical_distance):
        fig_height_inches = plt.gcf().get_size_inches()[1]
        ax_pos = ax.get_position()
        ax_height = ax_pos.y1 - ax_pos.y0
        ax_height_inches = ax_height * fig_height_inches
        ylim = ax.get_ylim()
        if logy:
            ax_height_ylim = math.log10(ylim[1]) - math.log10(ylim[0])
        else:
            ax_height_ylim = ylim[1] - ylim[0]
        # 1 pt = 1/72 in
        fontsize = matplotlib.rcParams["font.size"]
        assert fontsize is not None
        vertical_distance_inches = fontsize / 72
        vertical_distance = vertical_distance_inches / ax_height_inches * ax_height_ylim

    # find all Line2D objects with a valid label and valid data
    lines = [
        child
        for child in ax.get_children()
        if (
            isinstance(child, matplotlib.lines.Line2D)
            and str(child.get_label())[0] != "_"
            and not np.all(np.isnan(child.get_ydata()))
        )
    ]

    if len(lines) == 0:
        return

    targets = []
    for line in lines:
        ydata = np.array(line.get_ydata())
        targets.append(ydata[~np.isnan(ydata)][-1])

    if logy:
        targets = [math.log10(float(t)) for t in targets]

    ymax = ax.get_ylim()[1]
    targets = [min(target, ymax) for target in targets]

    moved_targets = _move_min_distance(targets, vertical_distance)
    if logy:
        moved_targets_t = [10**t for t in moved_targets]
    else:
        moved_targets_t = list(moved_targets)

    labels = [line.get_label() for line in lines]

    if color == "auto":
        colors = [line.get_color() for line in lines]
    else:
        colors = [color for line in lines]

    axis_to_data = ax.transAxes + ax.transData.inverted()
    xpos = axis_to_data.transform([horizontal_distance, 1.0])[0]

    for label, ypos, col in zip(labels, moved_targets_t, colors):
        if format_labels:
            label_string = label_formatter(str(label))
        else:
            label_string = str(label)
        ax.text(
            xpos,
            ypos,
            label_string,
            verticalalignment="center",
            color=col,
            weight=weight,
            **text_kwargs,
        )


def add_point_labels(
    series: pd.Series,
    offset: Tuple[float, float] = (0, 5),
    fmt: Callable[[float], str] = str,
    ax: Optional[plt.Axes] = None,
) -> None:
    """
    Annotate each point of a pandas Series plot with its value.

    Parameters
    ----------
    series : pandas.Series
        The plotted series (x=index, y=values).
    offset : tuple (x_offset, y_offset)
        Position offset for the label in points (default: (0, 5)).
    fmt : function
        Function to format labels (default: str).
        Example: fmt=lambda v: f"{v:,}" for comma separators.
    """
    if ax is None:
        ax = plt.gca()

    for x, y in series.items():
        ax.annotate(
            fmt(y), (x, y), textcoords="offset points", xytext=offset, ha="center"
        )
