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
"""Tools for conducting power calculations via simulation"""

import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from linearmodels.panel.results import PanelEffectsResults
from matplotlib import pyplot as plt
from tqdm import tqdm

from recidiviz.tools.analyst.estimate_effects import (
    est_did_effect,
    est_es_effect,
    validate_df,
)


def detect_date_granularity(
    df: pd.DataFrame,
    unit_of_analysis_column: str,
    date_column: str = "start_date",
) -> Tuple[str, int]:
    """
    Determines the granularity of dates in `df` by calculating the average number of
    days between observations for each unit of analysis.
    """

    granularity = None
    # sort by date column to get positive diffs
    df = df.sort_values([unit_of_analysis_column, date_column]).copy()
    # Convert to pandas datetime to ensure diff() returns proper timedelta (Pandas 2.0 compatibility)
    df[date_column] = pd.to_datetime(df[date_column])
    days_between_periods = int(
        np.floor(df.groupby([unit_of_analysis_column])[date_column].diff().mean().days)
    )
    if days_between_periods == 7:
        granularity = "week"
    elif 28 <= days_between_periods <= 31:
        granularity = "month"
    elif 88 <= days_between_periods <= 92:
        granularity = "quarter"
    elif 365 <= days_between_periods <= 366:
        granularity = "year"
    else:
        granularity = f"{days_between_periods} day period"
    return granularity, days_between_periods


def get_date_sequence(
    reference_period: datetime.datetime,
    granularity: Optional[str] = None,
    days_between_periods: Optional[int] = None,
    periods: int = 3,
) -> List[datetime.datetime]:
    """
    Returns a date sequence at known granularity (week, month, quarter, year) starting
    at a period after `reference_period` and continuing for `periods` periods.

    If `granularity` is not a standard value, a user can supply `days_between_periods`
    to generate the list of dates.
    """

    if granularity == "week":
        future_date_list = [
            reference_period + relativedelta(weeks=i) for i in range(1, periods + 1)
        ]
    elif granularity == "month":
        future_date_list = [
            reference_period + relativedelta(months=i) for i in range(1, periods + 1)
        ]
    elif granularity == "quarter":
        future_date_list = [
            reference_period + relativedelta(months=i * 3)
            for i in range(1, periods + 1)
        ]
    elif granularity == "year":
        future_date_list = [
            reference_period + relativedelta(years=i) for i in range(1, periods + 1)
        ]
    else:
        if not days_between_periods:
            raise ValueError(
                "Must provide `days_between_periods` if `granularity` is not a "
                "known value."
            )
        future_date_list = [
            reference_period + relativedelta(days=i * days_between_periods)
            for i in range(1, periods + 1)
        ]
    return future_date_list


# helper function for returning regression coefficient + confidence interval
def get_coefficient_and_ci(
    res: PanelEffectsResults,
    variable: str,
    confidence_level: float = 0.95,
) -> Tuple[float, float, float]:
    """
    Takes a PanelEffectsResults from a regression and returns the coefficient and
    bounds of the confidence interval for `variable`. The confidence level is set by
    `confidence_level` with default of 95%.
    """
    # get coefficient
    coefficient = res.params[variable]

    # get confidence interval
    ci = res.conf_int(level=confidence_level)
    lower = ci.loc[variable].lower
    upper = ci.loc[variable].upper

    return coefficient, lower, upper


def simulate_and_estimate(
    *,
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    treatment_column: str = "treated",
    date_column: str = "start_date",
    future_date_list: Optional[List[datetime.datetime]] = None,
    weight_column: Optional[str] = None,
    simulated_effect: Optional[float] = None,
    event_study: bool = False,
    clustered: bool = False,
    control_columns: Optional[List[str]] = None,
) -> Tuple[float, float, float]:
    """
    Function for simulating and estimating, returns tuple of results:
    treatment effect, lower bound, upper bound
    """

    # simulate treatment effect
    dfsim = simulate_rollout(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        treatment_column=treatment_column,
        date_column=date_column,
        control_columns=control_columns,
        post_rollout_dates=future_date_list,
        weight_column=weight_column,
        simulated_effect=simulated_effect,
    )

    # get treatment effect results
    if event_study:
        res = est_es_effect(
            df=dfsim,
            outcome_column=outcome_column,
            treated_column="treat_post",
            unit_of_analysis_column=unit_of_analysis_column,
            unit_of_treatment_column=unit_of_treatment_column,
            date_column=date_column,
            weight_column=weight_column,
            cluster_column=unit_of_treatment_column if clustered else None,
            control_columns=control_columns,
        )
    # diff-in-diff
    else:
        res = est_did_effect(
            df=dfsim,
            outcome_column=outcome_column,
            interaction_column="treat_post",
            unit_of_analysis_column=unit_of_analysis_column,
            unit_of_treatment_column=unit_of_treatment_column,
            date_column=date_column,
            weight_column=weight_column,
            cluster_column=unit_of_treatment_column if clustered else None,
            control_columns=control_columns,
        )
    # get coefficient and confidence interval
    coefficient, lower, upper = get_coefficient_and_ci(res, "treat_post")
    return coefficient, lower, upper


def simulate_rollout(
    *,
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    treatment_column: str = "treated",
    date_column: str = "start_date",
    post_rollout_periods: Optional[int] = 3,
    post_rollout_dates: Optional[List[datetime.datetime]] = None,
    weight_column: Optional[str] = None,
    control_columns: Optional[List[str]] = None,
    simulated_effect: Optional[float] = None,
    show_plot: bool = False,
    plot_title: str = "Simulated outcomes",
) -> pd.DataFrame:
    """
    Takes pre-rollout `df` and returns additional `post_rollout_periods` periods
    observations of `outcome` with `effect` added. Simulated post-rollout observations
    take samples from pre-rollout observations within each `unit_of_treatment_column`.

    Params
    ------
    df : pd.DataFrame
        Dataframe with columns as described below

    outcome_column : str
        Column name of numeric outcome variable

    unit_of_analysis_column: str
        Column with unit of analysis (e.g. district or officer)

    unit_of_treatment_column: str
        Column name of categorical column with units of treatment (e.g. districts
        or officers). Note that this can be the same or different than
        `unit_of_analysis_column`.

    treatment_column : str, default = "treated"
        Column name of boolean column = True in units that should be considered
        treated in the simulated periods

    date_column : str, default = "start_date"
        Column name of datetime column with dates of observations

    post_rollout_periods : Optinonal[int], default = 3
        Number of periods of simulated post-rollout observations to add

    post_rollout_dates : Optional[List[datetime.datetime]], default = None
        Instead of post_rollout_periods, a list of dates to use for the simulated
        periods.

    weight_column: Optional[str], default = None
        Column name of numeric column with sample weights (i.e. populations). If None,
        all units are assumed to have equal weight. These weights only affect the
        weighted averages in the plot in this function.

    simulated_effect : Optional[float], default = None
        Effect size to add to treatment group in the simulated periods. If None, no
        effect is added.

    show_plot : bool, default = False
        If True, show plot of simulated outcomes

    plot_title : str, default = "Simulated outcomes"
        Title of plot, if shown

    Returns
    -------
    pd.DataFrame

    Example Usage
    -------------
    >>> df = pd.DataFrame(
    ...     {
    ...         "district": ["A", "A", "B", "B"],
    ...         "officer": ["C", "C", "D", "D"],
    ...         "treated": [True, True, False, False,]
    ...         "start_date": [
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...         ],
    ...         "outcome": [1, 3, 2, 2],
    ...     }
    ... )
    >>> simulate_rollout(
    ...     df,
    ...     outcome_column="outcome",
    ...     unit_of_analysis_column="officer",
    ...     unit_of_treatment_column="district",
    ...     treatment_column="treated",
    ...     date_column="start_date",
    ...     post_rollout_periods=3,
    ...     simulated_effect=1,
    ...     show_plot=True,
    ... )
    """

    other_columns = [treatment_column]
    if control_columns:
        other_columns.extend(control_columns)

    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    )

    # get last period
    last_period = df[date_column].max()

    # determine post_rollout_dates if not supplied
    if not post_rollout_dates:
        if post_rollout_periods is None:
            raise ValueError(
                "Must provide either `post_rollout_periods` or `post_rollout_dates`"
            )

        if post_rollout_periods == 0:
            post_rollout_dates = []
        else:
            # get days between periods
            _, days_between_periods = detect_date_granularity(
                df, unit_of_analysis_column, date_column
            )

            # get post_rollout_dates
            post_rollout_dates = get_date_sequence(
                reference_period=last_period,
                granularity="day",
                days_between_periods=days_between_periods,
                periods=post_rollout_periods,
            )

    # at this point post_rollout_dates must be defined
    # check if post_rollout_periods is missing or disagrees with post_rollout_dates
    # and give priority to post_rollout_dates
    if post_rollout_periods is None or len(post_rollout_dates) != post_rollout_periods:
        post_rollout_periods = len(post_rollout_dates)

    # helper function for getting future values of a given variable using
    # pre-period values
    def get_future_values(
        df: pd.DataFrame,
        sampled_column: str,
        future_dates: List[datetime.datetime],
    ) -> pd.DataFrame:
        """
        Takes dataframe `df` and returns additional periods of sampled
        values from `sampled_column`.

        `future_dates` is a list of dates to use for the future periods.

        `df` must have columns:
        - unit_of_analysis_column
        - unit_of_treatment_column
        - `sampled_column`
        - weight_column,
        - treatment_column,

        The returned dataframe has the same columns.
        """

        # determine number of units
        # set unit as the distinct combination of unit_of_analysis_column and
        # unit_of_treatment_column
        unit = list(set([unit_of_analysis_column, unit_of_treatment_column]))

        # sample from pre-rollout periods, with replacement
        # include control_columns if applicable
        sampled_columns = [
            *unit,
            sampled_column,
            weight_column,
            treatment_column,
        ]
        if control_columns:
            sampled_columns.extend(control_columns)

        future = df[sampled_columns].copy()
        # sample with replacement within unit, dropping date column and `column`
        future = future.groupby(unit).sample(
            n=len(future_dates),
            replace=True,
        )

        # get number of units
        number_of_units = len(future[unit].drop_duplicates())

        future = pd.concat(
            [
                future.reset_index(drop=True),
                pd.Series(
                    future_dates * number_of_units,
                    name=date_column,
                    dtype="datetime64[ns]",
                ),
            ],
            axis=1,
        )

        # return dataframe
        return future

    # at this point, post_rollout_dates must be defined, or post_rollout_periods = 0
    # if > 0, get future values
    if post_rollout_periods > 0:
        # mypy
        if not post_rollout_dates:
            raise ValueError("post_rollout_dates must be defined")
        # get future values
        future = get_future_values(
            df=df,
            sampled_column=outcome_column,
            future_dates=post_rollout_dates,
        )

        # add treatment effect to treatment group only
        if simulated_effect is not None:
            future[outcome_column] += simulated_effect * future[
                treatment_column
            ].astype(int)

        # append to original df
        df_all = (
            pd.concat([df[future.columns], future], axis=0)
            .sort_values([unit_of_analysis_column, date_column])
            .reset_index(drop=True)
        )
    else:
        df_all = df.copy()

    # new treatment indicator for treatment * post rollout
    df_all["post"] = df_all[date_column] > last_period
    df_all["treat_post"] = df_all[treatment_column] & df_all.post

    if show_plot:
        df_agg = (
            df_all.groupby([date_column, treatment_column, "post"])[
                [outcome_column, weight_column]
            ]
            .apply(
                lambda x: pd.Series(
                    {
                        outcome_column: np.average(
                            x[outcome_column], weights=x[weight_column]
                        )
                    }
                )
            )
            .reset_index()
        )
        df_agg.set_index(date_column, inplace=True)
        # iterate over boolean treatment column, may be only True for event studies
        for t in df_agg[treatment_column].unique():
            dfsub = df_agg.loc[(df_agg[treatment_column] == t)]
            label = "Treated" if t else "Control"
            dfsub[outcome_column].plot(label=label)
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.title(plot_title)
        plt.ylim(0)

        # add vertical line halfway between last_period and rollout date
        plt.axvline(
            last_period + relativedelta(days=15),
            color="black",
            linestyle="dashed",
            alpha=0.5,
        )

    return df_all


def get_simulated_power_curve(
    *,
    iters: int,
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    treatment_column: str = "treated",
    date_column: str = "start_date",
    post_rollout_periods: Optional[List[int]] = None,
    weight_column: Optional[str] = None,
    control_columns: Optional[List[str]] = None,
    compliance: float = 1.0,
    plot: bool = True,
    power_level: float = 0.8,
    verbose: bool = True,
) -> pd.DataFrame:
    """

    Generates a power calculation curve by simulating multiple difference-in-differences
    or event study estimates. This shows the effect sizes our data can detect based on
    the chosen post-rollout period (the wait time before launching the tool statewide).

    WARNING: this can take a while.
    Complexity ~ len(df) ** 2 * iters * len(post_rollout_periods)

    Params
    ------
    iters: int
        Number of simulation iterations to run. Should be >1000 for reliable results.
    df: pd.DataFrame
        Dataframe with columns as described below
    outcome_column : str
        Column name of numeric outcome variable (e.g. primary metric)
    unit_of_analysis_column: str
        Column with unit of analysis (e.g. district or officer). This refers to the
        level at which outcomes are observed in our data.
    unit_of_treatment_column: str
        Column name of categorical column with units of treatment (e.g. districts
        or officers). This refers to the level at which treatment was assigned.
    treatment_column : str, default = "treated"
        Column name of boolean column = True in units that should be considered
        treated in the simulated periods. This column should be True for all units
        if the simulation is an event study.
    date_column : str, default = "start_date"
        Column name of datetime column with dates of observations
    post_rollout_periods : List[int], default = [1, 2, 3]
        Number of periods (e.g. weeks, months) of simulated post-rollout observations to add.
        If multiple values are provided, the simulation will be run for each value.
    weight_column: Optional[str], default = None
        Column name of numeric column with sample weights (i.e. populations). If None,
        all units are assumed to have equal weight.
    control_columns: Optional[List[str]], default = None
        List of column names of numeric control variables. If None, no control
        variables are used. This could helb reduce our variance and gain statistical power.
    compliance: float, default = 1.0
        Proportion of units that are treated AND respond to treatment. This parameter is
        useful if we are concerned a nontrivial proportion of units will ignore the
        rollout.
    power_level: float, default = 0.8
        Desired power level for the simulation. This is the probability of detecting an
        effect if it exists. The default is 0.8, which is a common threshold in social
        science.
    verbose: bool, default = True
        If True, print simulation parameters and progress
    plot: bool, default = True
        If True, plot the power curve

    Returns
    -------
    pd.DataFrame

    Example Usage
    -------------
    df = pd.DataFrame(
        {
            "district": ["A", "A", "B", "B"],
            "treated": [True, True, False, False],
            "start_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-02-01"),
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-02-01"),
            ],
            "outcome": [1, 300, 2, 2],
        }
    )
    power_df = get_simulated_power_curve(
        iters=100,
        df=df,
        outcome_column="outcome",
        unit_of_analysis_column="district",
        unit_of_treatment_column="district",
        treatment_column="treated",
        date_column="start_date",
        post_rollout_periods=[1, 2, 3],
        power_level = 0.8,
        verbose = False,
        plot=True,
    )
    """

    other_columns = [treatment_column]
    if control_columns:
        other_columns.extend(control_columns)
    if unit_of_treatment_column != unit_of_analysis_column:
        other_columns.append(unit_of_treatment_column)
    if weight_column is None:
        weight_column = "weight_column"
        df[weight_column] = 1

    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    )

    # detect if clustered treatment
    clustered = unit_of_treatment_column != unit_of_analysis_column

    # detect if DiD or Event Study
    event_study = df[treatment_column].nunique() == 1

    # determine number of units
    # set unit as the distinct combination of unit_of_analysis_column and
    # unit_of_treatment_column
    unit = list(set([unit_of_analysis_column, unit_of_treatment_column]))
    number_of_units = len(df[unit].drop_duplicates())
    cluster = list(set([unit_of_treatment_column]))
    number_of_clusters = len(df[cluster].drop_duplicates())

    # determine granularity of time in df
    granularity, days_between_periods = detect_date_granularity(
        df, unit_of_analysis_column, date_column
    )

    # get first and last period in df
    first_period = df[date_column].min()
    last_period = df[date_column].max()

    # print weighted baseline mean in treatment group
    dfsub = df.loc[df[treatment_column] & (df[date_column] == df[date_column].max())]
    baseline_mean = np.average(dfsub[outcome_column], weights=dfsub[weight_column])

    # Total simulation rounds = iters * len(post_rollout_periods)
    if not post_rollout_periods:
        post_rollout_periods = [1, 2, 3]

    if verbose:
        print(
            f"DATA PARAMETERS:\n"
            f"Baseline (weighted) mean in treatment group: {baseline_mean:0.3g}\n"
            f"First pre-treatment {granularity}: {first_period}\n"
            f"Last pre-treatment {granularity}: {last_period}\n"
            f"Number of units: {number_of_units}\n"
            f"Pre-treatment {granularity}s: {df[date_column].nunique()}\n"
            f"Days between periods: {days_between_periods}\n"
            f"Outcomes observed at {unit_of_analysis_column}-level and {granularity}-granularity\n"
            "------------------------------------\n"
            "SIMULATION ASSUMPTIONS:\n"
            f"Analysis strategy: {'Diff-in-Diff' if not event_study else 'Event Study'}\n"
            f"{'Treatment clustered at ' + unit_of_treatment_column + '-level' if clustered else 'Treatment not clustered'}\n"
            f"Treatment {granularity}s: {post_rollout_periods}\n"
            f"Compliance rate: {compliance}\n"
            f"Starting simulation. Total simulation rounds: {iters * len(post_rollout_periods)}\n"
            "------------------------------------"
        )

    # DF where we will store our final results
    outcome_columns = [
        "outcome",
        "no_post_rollout_periods",
        "granularity",
        "days_between_periods",
        "baseline_mean",
        "compliance_rate",
        "no_units",
        "no_clusters",
        "first_period",
        "last_period",
        "power_level",
        "MDE",
        "MDE_percent",
    ]
    outcome_df = pd.DataFrame(index=post_rollout_periods, columns=outcome_columns)
    outcome_df["outcome"] = outcome_column
    outcome_df["no_post_rollout_periods"] = post_rollout_periods
    outcome_df["granularity"] = granularity
    outcome_df["days_between_periods"] = days_between_periods
    outcome_df["baseline_mean"] = baseline_mean
    outcome_df["compliance_rate"] = compliance
    outcome_df["no_units"] = number_of_units
    outcome_df["no_clusters"] = number_of_clusters
    outcome_df["first_period"] = first_period
    outcome_df["last_period"] = last_period
    outcome_df["power_level"] = power_level

    # loop over post_rollout_periods
    for period_count in post_rollout_periods:
        print(f"Starting simulation for {period_count} post-rollout {granularity}s")

        # get future dates to use for simulated periods
        future_date_list = get_date_sequence(
            reference_period=last_period,
            granularity=granularity,
            days_between_periods=days_between_periods,
            periods=period_count,
        )

        # get dfs for storing results
        dfpow = pd.DataFrame()
        dfres = pd.DataFrame(index=range(iters))

        # loop over iters
        for idx in tqdm(range(iters)):
            try:
                coefficient_estimate, lb, ub = simulate_and_estimate(
                    df=df,
                    outcome_column=outcome_column,
                    unit_of_analysis_column=unit_of_analysis_column,
                    unit_of_treatment_column=unit_of_treatment_column,
                    treatment_column=treatment_column,
                    date_column=date_column,
                    future_date_list=future_date_list,
                    weight_column=weight_column,
                    simulated_effect=0,
                    event_study=event_study,
                    clustered=clustered,
                    control_columns=control_columns,
                )
            # if failed, retry
            # this can occur if the random future draws result in multicoliniarity,
            # which is exceptionally rare but possible with small datasets
            except ValueError:
                coefficient_estimate, lb, ub = simulate_and_estimate(
                    df=df,
                    outcome_column=outcome_column,
                    unit_of_analysis_column=unit_of_analysis_column,
                    unit_of_treatment_column=unit_of_treatment_column,
                    treatment_column=treatment_column,
                    date_column=date_column,
                    future_date_list=future_date_list,
                    weight_column=weight_column,
                    simulated_effect=0,
                    event_study=event_study,
                    clustered=clustered,
                    control_columns=control_columns,
                )

            # Collect the results
            dfres.loc[idx, "meaneffect"] = coefficient_estimate
            dfres.loc[idx, "lb"] = lb
            dfres.loc[idx, "ub"] = ub

        # figure out when power = 0 and = 1
        # lb will (usually) be negative, ub will be (usually) be positive
        #  because simulated effect is centered at zero.
        maxeffect = max(abs(dfres.lb.min()), dfres.ub.max())
        mineffect = -1 * maxeffect

        # get dfpow for mineffect to maxeffect
        dfres_og = dfres.copy()
        for effect in np.linspace(mineffect, maxeffect, 200):
            # scale by `effect`
            for c in dfres_og.columns:
                dfres[c] = dfres_og[c] + effect

            # intuition: add effect to bounds, see if both bounds
            #  positive or negative. If so, effect is "found".
            dfres["effect_found"] = np.array(
                dfres.apply(lambda x: (x.ub * x.lb) > 0, 1), dtype="int"
            )

            # calc power
            dfpow.loc[effect, "power"] = dfres.effect_found.mean()
            dfpow.loc[effect, "effect_plow"] = dfres.meaneffect.quantile(0.025)
            dfpow.loc[effect, "effect_phigh"] = dfres.meaneffect.quantile(0.975)
            dfpow.loc[effect, "first_effect"] = abs(
                dfres.loc[dfres.effect_found == 1].meaneffect
            ).min()

        # dfpow now saturated
        # print MDE @ {power_level}% power
        mde = (
            dfpow.loc[(dfpow.index > 0) & (dfpow.power >= power_level)].head(1).index[0]
        )
        if verbose:
            print(
                f"Min. detectable effect @ {power_level*100}% power: {mde:0.3g}\n"
                f"Min. detectable effect as % of baseline mean: {100 * mde / baseline_mean:0.3g}%"
            )

        outcome_df.loc[period_count, "MDE_percent"] = 100 * mde / baseline_mean
        outcome_df.loc[period_count, "MDE"] = mde
        # plots, if option selected
        if plot:
            # power curve
            dfpowsub = dfpow.loc[dfpow.index >= 0]
            plt.plot(
                dfpowsub.index, dfpowsub.power, label=f"{period_count} {granularity}s"
            )

    # add labels to plot, if necessary
    if plot:
        plt.title(f"Power curve for {outcome_column} using simulation")
        plt.xlabel("True effect")
        plt.ylabel("Power")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5), title="Power Curves")
        plt.ylim(0, 1.1)

    return outcome_df
