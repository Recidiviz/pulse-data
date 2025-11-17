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
"""Tools for cohort-based analysis."""

import datetime
import warnings
from typing import Iterable, List, Optional, Tuple, Union

import matplotlib.patches as mpatches
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from scipy.stats import norm

from recidiviz.common.date import calendar_unit_date_diff
from recidiviz.tools.analyst.plots import RECIDIVIZ_COLORS


# helper function to get difference between two dates in months
def get_month_delta(start_date: datetime.date, end_date: datetime.date) -> int:
    """
    Returns the number of months between two dates
    """
    return calendar_unit_date_diff(start_date, end_date, "months")


# helper function for dealing with full observability
# TODO(#32670): Warn less often once we're comfortable with this functionality
def _process_full_observability(
    df: pd.DataFrame,
    time_to_mature_field: str,
    time_index: Iterable[int],
    full_observability: bool,
    suppress_full_observability_warning: bool,
) -> pd.DataFrame:
    """
    Checks for full observability, optionally issues a warning if there are any cohorts without full observability, and
    drops those cohorts if full_observability is True.
    """
    # mark instances that don't have the full window of time_index available in which an event can occur
    df["incomplete_observability"] = df[time_to_mature_field] < max(time_index)
    if full_observability:
        if (
            df["incomplete_observability"].any() > 0
            and not suppress_full_observability_warning
        ):
            warnings.warn(
                "Warning: dropping cohorts that do not have full observability. To suppress this warning, "
                + "set suppress_full_observability_warning=True."
            )
        df = df[~df["incomplete_observability"]]
        if df.empty:
            raise ValueError(
                f"No cohort starts have full observability as of max(`time_index`) = {max(time_index)}."
            )
    elif (
        df["incomplete_observability"].any() > 0
        and not suppress_full_observability_warning
    ):
        warnings.warn(
            "Warning: some cohort starts do not have full observability. To automatically drop these cohorts from "
            + "the calculation, set full_observability=True. To suppress this warning, set "
            + "suppress_full_observability_warning=True."
        )

    return df


def add_time_unit_to_date(
    start_date: datetime.date, time_unit: str, num_time_unit: int
) -> datetime.date:
    """
    Adds a specified time unit to a datetime
    """
    if time_unit in ["years", "months", "days"]:
        date_output = start_date + relativedelta(
            dt1=None, dt2=None, **{time_unit: num_time_unit}
        )
    else:
        raise ValueError("`time_unit` must be one of 'years','months','days'")
    return date_output


# function to plot "seaweed" style survival curves using date cohorts on a timeline
def plot_timeline_cohort_survival_curves(
    *,
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

    if title_text:
        plt.title(title_text)
    if x_text:
        plt.xlabel(x_text)

    # Align x-axis date labels with date cohorts
    x_axis_labels = [
        np.datetime64(min_date + relativedelta(months=+x))
        for x in range(0, date_range + plot_timespan, cohort_spacing)
        if min_date + relativedelta(months=+x) <= datetime.date.today()
    ]
    plt.xticks(x_axis_labels, rotation=45)
    plt.xlim(min_date, max(x_axis_labels))

    if save_fig:
        plt.savefig(save_fig)
    plt.show()


def gen_cohort_status_df(
    *,
    cohort_df: pd.DataFrame,
    status_df: pd.DataFrame,
    cohort_date_field: str,
    status_start_date_field: str,
    status_end_date_field: str,
    join_field: Optional[Union[str, List[str]]] = "person_id",
    time_index: Iterable[int] = (0, 6, 12, 18, 24, 36),
    time_unit: str = "months",
    last_day_of_data: Optional[datetime.datetime] = None,
    cohort_attribute_col: Optional[Union[str, List[str]]] = None,
    status_attribute_col: Optional[Union[str, List[str]]] = None,
    full_observability: bool = False,
    suppress_full_observability_warning: bool = False,
) -> pd.DataFrame:
    # TODO(#23152): Consolidate cohort time at status and cohort event rate functions
    """
    Returns a dataframe indexed by the original columns with values for `days_at_status`
    calculated across time indices. When a `status_attribute_col` is specified, the dataframe
    also returns a column indicating the status at each evaluation date.
    Params:
    ------
    cohort_df: pandas DataFrame
        Dataframe of all cohort starts that will be evaluated as having subsequent days
        spent at a given status. Must have a field indicating the cohort start date as well
        as a id field that joins the individual to the events in `status_df`. This dataframe
        can have any other cohort attributes included as well, which will flow through to the
        output.

    status_df: pandas DataFrame
        Dataframe of all statuses that will be evaluated against cohort start. Must have a
        field indicating the status start date and status end date, as well as an id field
        that joins to the individual in `cohort_df`. This dataframe can have any other status
        attributes as well, which will flow through to the output.

    cohort_date_field: string
        Field name in `cohort_df` that indicates the cohort start date.

    status_start_date_field: string
        Field name in `status_df` that indicates the status start date.

    status_end_date_field: string
        Field name in `status_df` that indicates the status end date.

    join_field: Optional[Union[str, List[str]]]
        Field name (or list of field names) in both `cohort_df` and `status_df` joins
        these two tables together when dates are compared.

    time_index: list
        List of integers that specify the time units from cohort start that the days at
        status metric will be evaluated against. These can represent days, months or years.

    time_unit: str
        Unit of time that the `time_index` represents. Can be "years", "months", or
        "days"

    last_day_of_data: str
        Day representing the last day of available data. If none is provided, the max
        status start/end date is used. This is used to ensure that days at status calculations only use
        cohort starts that have had the full amount of time to mature.

    cohort_attribute_col: Optional[Union[str, List[str]]]
        Single field or list of fields in `cohort_df` that represent attributes of the cohort
        that the days at status metrics should be disaggregated by. If no list is specified, the
        metrics are calculated across all cohort starts.

    status_attribute_col: Optional[Union[str, List[str]]]
        Single field in `status_df` that represent attributes of the status
        that the days at status metrics should be disaggregated by. If no list is specified, the
        metrics are calculated across all statuses.

    full_observability: bool, default=False
        If True, only cohorts that have had the full amount of time to mature as of
        max(`time_index`) are included. If False, all cohorts are included, which
        may result in downward sloping event curves.

    suppress_full_observability_warning: bool, default=False
        If False, a warning will be issued if there are any cohorts that don't have
        full observability. If True, this warning won't be issued.
    """
    if isinstance(join_field, str):
        join_field_list = [join_field]
    elif isinstance(join_field, list):
        join_field_list = join_field
    else:
        raise ValueError("`join_field` not a string or list of strings.")

    # handle attribute columns which can be a list, a string, or None
    if not cohort_attribute_col:
        cohort_attribute_col_list = []
    elif isinstance(cohort_attribute_col, str):
        cohort_attribute_col_list = [cohort_attribute_col]
    else:
        cohort_attribute_col_list = cohort_attribute_col

    if not status_attribute_col:
        status_attribute_col_list = []
    elif isinstance(status_attribute_col, str):
        status_attribute_col_list = [status_attribute_col]
    else:
        status_attribute_col_list = status_attribute_col

    # Check that all necessary columns are present in cohort_df
    for var in [cohort_date_field] + join_field_list + cohort_attribute_col_list:
        if var not in cohort_df.columns:
            raise AttributeError(f"Column {var} can not be found in `cohort_df`.")

    # Check that all necessary columns are present in status_df
    for var in (
        [status_start_date_field, status_end_date_field]
        + join_field_list
        + status_attribute_col_list
    ):
        if var not in status_df.columns:
            raise AttributeError(f"Column {var} can not be found in `status_df`.")

    # Convert date fields
    cohort_df[cohort_date_field] = pd.to_datetime(
        cohort_df[cohort_date_field], errors="coerce"
    ).dt.date
    status_df[status_start_date_field] = pd.to_datetime(
        status_df[status_start_date_field], errors="coerce"
    ).dt.date
    status_df[status_end_date_field] = pd.to_datetime(
        status_df[status_end_date_field], errors="coerce"
    ).dt.date

    # Set the last day of data to be the max status date if not specified. Print the date
    # let user know, just to make sure this assumption seems correct. This is used to
    # determine which subset of the cohort starts are eligible for each calculation
    if not last_day_of_data:
        last_day_of_data = pd.to_datetime(
            status_df[[status_start_date_field, status_end_date_field]].max(1).max(0)
        )
        if not isinstance(last_day_of_data, datetime.datetime):
            raise ValueError(
                "Could not format `status_start_date_field` or `status_end_date_field` as a datetime object."
            )
        print(
            f"Last day of data not specified. Assuming to be {last_day_of_data.date()} "
            f"based on status dates"
        )

    # create a dataframe with the specified time units
    cohort_index_field = f"cohort_{time_unit}"
    time_df = pd.Series(time_index, name=cohort_index_field).to_frame()

    time_to_mature_field = f"{time_unit}_to_mature"

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
    cohort_df[time_to_mature_field] = cohort_df.apply(
        lambda x: calendar_unit_date_diff(
            x[cohort_date_field], last_day_of_data.strftime("%Y-%m-%d"), time_unit
        ),
        1,
    )

    cohort_df = _process_full_observability(
        cohort_df,
        time_to_mature_field,
        time_index,
        full_observability,
        suppress_full_observability_warning,
    )

    # Unnest the cohort_df by the time index series and then subset for those cohort
    # starts that have matured by the time index
    cohort_start_x_evaluation_months = pd.merge(cohort_df, time_df, how="cross")

    cohort_start_x_evaluation_months = cohort_start_x_evaluation_months[
        cohort_start_x_evaluation_months[time_to_mature_field]
        >= cohort_start_x_evaluation_months[cohort_index_field]
    ]

    # Create a field for the "cohort_evaluation_date" which is the cohort start date + the time index.
    # This represents the date at which we will look to see how many days have been at a given status
    # by that point
    cohort_start_x_evaluation_months[
        "cohort_eval_date"
    ] = cohort_start_x_evaluation_months.apply(
        lambda x: add_time_unit_to_date(
            x[cohort_date_field], time_unit, x[cohort_index_field]
        ),
        axis=1,
    )

    # Merge the unnested cohort starts to the statuses
    cohort_x_status = pd.merge(
        cohort_start_x_evaluation_months, status_df, on=join_field_list, how="left"
    )

    # In order to calculate the "days at status" for a given cohort start at a given time index, we need to calculate
    # the length of the intersection between the status span and the cohort start / cohort evaluation date span. This is
    # subtracting the max of the cohort date field and `status_start_date` from the min of the `cohort_eval_date` and
    # `status_end_date`. When there is no intersection, this value will be negative, and the values are therefore
    # clipped at 0 to represent 0 days of overlap.

    # Pandas 2.0: Convert date columns to datetime64 before arithmetic to enable .dt accessor
    cohort_x_status_dates = cohort_x_status[
        [
            "cohort_eval_date",
            status_end_date_field,
            cohort_date_field,
            status_start_date_field,
        ]
    ].apply(pd.to_datetime)

    cohort_x_status["days_at_status"] = (
        (
            (
                cohort_x_status_dates[["cohort_eval_date", status_end_date_field]]
                .fillna(pd.Timestamp.max)
                .min(1)
            )
            - (
                cohort_x_status_dates[[cohort_date_field, status_start_date_field]]
                .fillna(pd.Timestamp.max)
                .max(1)
            )
        ).dt.days
    ).clip(lower=0)
    # Aggregate across status spans and calculate the total number of days that a person / cohort start has a given
    # status at each time index
    cohort_days_at_status = cohort_x_status.groupby(
        (
            join_field_list
            + [cohort_date_field, cohort_index_field, "cohort_eval_date"]
            + cohort_attribute_col_list
            + status_attribute_col_list
        )
    )["days_at_status"].sum()
    # If there is a status attribute column, we also pull in a field indicating which of those statuses was relevant
    # at each time indexed cohort evaluation date
    if len(status_attribute_col_list) > 0:
        # Subset the cohort x status dataframe for only those rows where the evaluation date is between the status
        # start and end dates. This row contains the information indicating the relevant status at each date.
        in_range = (
            cohort_x_status["cohort_eval_date"]
            >= cohort_x_status[status_start_date_field]
        ) & (
            cohort_x_status["cohort_eval_date"]
            < cohort_x_status[status_end_date_field].fillna(pd.Timestamp.now().date())
        )

        status_at_eval_date = cohort_x_status[in_range].set_index(
            join_field_list
            + [cohort_date_field, cohort_index_field, "cohort_eval_date"]
            + cohort_attribute_col_list
        )[status_attribute_col_list]

        cohort_days_at_status = (
            cohort_days_at_status.unstack(status_attribute_col_list)
            .fillna(0)
            .astype(int)
        )
        # Since the output df will have both `days_at_status` and `status_at_eval_date` columns, make the columns
        # multi-index so we can differentiate them and easily subset for one or the other
        status_at_eval_date.columns = pd.MultiIndex.from_product(
            [["status_at_eval_date"], status_at_eval_date.columns]
        )

        if isinstance(cohort_days_at_status.columns, pd.MultiIndex):
            flattened_columns = cohort_days_at_status.columns.tolist()
        else:
            flattened_columns = cohort_days_at_status.columns

        cohort_days_at_status.columns = pd.MultiIndex.from_product(
            [["days_at_status"], flattened_columns]
        )

        cohort_days_at_status = cohort_days_at_status.join(status_at_eval_date)

    return cohort_days_at_status


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


def gen_aggregated_cohort_event_df(
    df: pd.DataFrame,
    *,
    cohort_date_field: str,
    event_date_field: str,
    time_index: Iterable[int] = (0, 6, 12, 18, 24, 36),
    time_unit: str = "months",
    last_day_of_data: Optional[datetime.datetime] = None,
    cohort_attribute_col: Optional[Union[str, List[str]]] = None,
    event_attribute_col: Optional[Union[str, List[str]]] = None,
    full_observability: bool = True,
    suppress_full_observability_warning: bool = False,
) -> pd.DataFrame:
    """
    Returns a dataframe aggregated by cohort time index as well as any cohort or event
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

    full_observability: bool, default=True
        If True, only cohorts that have had the full amount of time to mature as of
        max(`time_index`) are included. If False, all cohorts are included, which
        may result in downward sloping event curves.

    suppress_full_observability_warning: bool, default=False
        If False, a warning will be issued if there are any cohorts that don't have
        full observability. If True, this warning won't be issued.
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

    df = _process_full_observability(
        df,
        time_to_mature_field,
        time_index,
        full_observability,
        suppress_full_observability_warning,
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
    *,
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
) -> Tuple[Figure, Axes]:
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

    fig, ax = plt.subplots()
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
    return fig, ax
