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

from typing import List, Optional

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from tqdm import tqdm

from recidiviz.tools.analyst.estimate_effects import (
    est_did_effect,
    est_es_effect,
    validate_df,
)


def simulate_rollout(
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    treatment_column: str = "treated",
    date_column: str = "start_date",
    post_rollout_periods: int = 3,
    weight_column: Optional[str] = None,
    simulated_effect: Optional[float] = None,
    show_plot: bool = False,
    plot_title: str = "Simulated outcomes",
) -> pd.DataFrame:
    """
    Takes pre-rollout `df` and returns additional `post_rollout_periods` months
    observations of `outcome` with `effect` added. Simulated post-rollout observations
    take samples from pre-rollout observations within each `unit_of_treatment_column`.

    Currently this function assumes monthly date granularity but can be generalized.

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

    post_rollout_periods : int, default = 3
        Number of months of simulated post-rollout observations to add

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
    ...     unit_of_analysis_column="district",
    ...     unit_of_treatment_column="district",
    ...     treatment_column="treated",
    ...     date_column="start_date",
    ...     post_rollout_periods=3,
    ...     simulated_effect=1,
    ...     show_plot=True,
    ... )
    """

    other_columns = [treatment_column]
    if unit_of_treatment_column != unit_of_analysis_column:
        other_columns.append(unit_of_treatment_column)
    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    )

    # TODO(#21614): generalize to other date granularities
    # for now, verify that date_column is monthly
    if any(df[date_column].dt.day != 1):
        raise ValueError(
            f"Date column {date_column} is not monthly. "
            "Currently only monthly date granularity is supported."
        )

    # get last period
    last_period = df[date_column].max()

    # helper function for getting future values of a given variable using
    # pre-period values
    def get_future_values(
        df: pd.DataFrame,
        sampled_column: str,
        periods: int,
    ) -> pd.DataFrame:
        """
        Takes dataframe `df` and returns additional `periods` periods of sampled
        values from `sampled_column`.

        `df` must have columns:
        - unit_of_analysis_column
        - `sampled_column`
        - weight_column,
        - treatment_column,

        The returned dataframe has the same columns.
        """

        # sample from pre-rollout periods, with replacement
        future = df[
            [
                unit_of_analysis_column,
                sampled_column,
                weight_column,
                treatment_column,
            ]
        ].copy()
        # sample with replacement within unit, dropping date column and `column`
        future = future.groupby(unit_of_analysis_column).sample(
            n=periods,
            replace=True,
        )

        # at this point we have n * K samples for n periods and K units.
        # add date column with future dates
        # TODO(#21614): generalize to other date granularities
        future_dates = [
            last_period + relativedelta(months=i) for i in range(1, periods + 1)
        ] * df[unit_of_analysis_column].nunique()
        future = pd.concat(
            [
                future.reset_index(drop=True),
                pd.Series(future_dates, name=date_column, dtype="datetime64[ns]"),
            ],
            axis=1,
        )

        # return dataframe
        return future

    # get future outcomes and weights, if necessary
    if post_rollout_periods > 0:
        future = get_future_values(df, outcome_column, post_rollout_periods)

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
        # iterate over boolean treatment column, may be only True for event studies
        for t in df_agg[treatment_column].unique():
            dfsub = df_agg.loc[(df_agg[treatment_column] == t)]
            label = "Treated" if t else "Control"
            plt.plot(dfsub[date_column], dfsub[outcome_column], label=label)
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
    assumptions: bool = True,
    plot: bool = True,
) -> pd.DataFrame:
    """
    Runs a Monte Carlo simulation of many diff-in-diff estimates to produce a power
    curve.

    WARNING: this can take a while.
    Complexity ~ len(df) ** 2 * iters * len(post_rollout_periods)

    Params
    ------
    iters: int
        Number of simulation iterations to run. Should be >1000 for reliable results.

    df: pd.DataFrame
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

    post_rollout_periods : List[int], default = [1, 2, 3]
        Number of months of simulated post-rollout observations to add. If multiple
        values are provided, the simulation will be run for each value and a line
        for each duration will be plotted.

    weight_column: Optional[str], default = None
        Column name of numeric column with sample weights (i.e. populations). If None,
        all units are assumed to have equal weight. These weights affect the DiD
        estimates by upweighting units with larger weights.

    control_columns: Optional[List[str]], default = None
        List of column names of numeric control variables. If None, no control
        variables are used.

    compliance: float, default = 1.0
        Proportion of units that are treated AND respond to treatment. This parameter is
        useful if we are concerned a nontrivial proportion of units will ignore the
        rollout.

    assumptions: bool, default = True
        If True, print simulation assumptions

    plot: bool, default = True
        If True, plot the power curve

    Returns
    -------
    pd.DataFrame

    Example Usage
    -------------
    >>> df = pd.DataFrame(
    ...     {
    ...         "district": ["A", "A", "B", "B"],
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
    >>> df = get_simulated_did_power_curve(
    ...     iters=1000,
    ...     df=df,
    ...     outcome="outcome",
    ...     unit_of_analysis_column="district",
    ...     unit_of_treatment_column="district",
    ...     treatment_column="treated",
    ...     date_column="date",
    ...     post_rollout_periods=[1, 2, 3],
    ...     assumptions=True,
    ...     plot=True,
    ... )
    """

    other_columns = [treatment_column]
    if control_columns:
        other_columns.extend(control_columns)
    if unit_of_treatment_column != unit_of_analysis_column:
        other_columns.append(unit_of_treatment_column)
    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    )

    # detect if clustered treatment
    clustered = unit_of_treatment_column != unit_of_analysis_column

    # detect if DiD or Event Study
    event_study = df[treatment_column].nunique() == 1

    # print assumptions if True
    if assumptions:
        print("Assumptions:")
        if clustered:
            print(f"Treatment clustered at {unit_of_treatment_column}-level")
        else:
            print("Treatment not clustered")
        print(f"Outcomes observed at {unit_of_analysis_column}-level")
        print(f"Last control month: {df[date_column].max()}")
        print(f"Pre-treatment months: {df[date_column].nunique()}")
        print(f"Treatment months: {post_rollout_periods}")
        print(f"Compliance rate: {compliance}")

    # print total simulation rounds
    if not post_rollout_periods:
        post_rollout_periods = [1, 2, 3]
    print(
        f"Starting simulation. Total simulation rounds: {iters * len(post_rollout_periods)}"
    )

    # loop over post_rollout_periods
    for period_count in post_rollout_periods:
        print(f"Starting simulation for {period_count} post-rollout periods")

        # get dfs for storing results
        dfpow = pd.DataFrame()
        dfres = pd.DataFrame(index=range(iters))

        for i in tqdm(range(iters)):
            # simulate treatment effect
            dfsim = simulate_rollout(
                df=df,
                outcome_column=outcome_column,
                unit_of_analysis_column=unit_of_analysis_column,
                unit_of_treatment_column=unit_of_treatment_column,
                treatment_column=treatment_column,
                date_column=date_column,
                post_rollout_periods=period_count,
                weight_column=weight_column,
                simulated_effect=0,
            )

            # get treatment effect results
            if event_study:
                res = est_es_effect(
                    df=dfsim,
                    outcome_column=outcome_column,
                    treated_column="treat_post",
                    unit_of_analysis_column=unit_of_analysis_column,
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
                    date_column=date_column,
                    weight_column=weight_column,
                    cluster_column=unit_of_treatment_column if clustered else None,
                    control_columns=control_columns,
                )

            dfres.loc[i, "meaneffect"] = res.params["treat_post"]
            dfres.loc[i, "lb"] = res.conf_int().loc["treat_post"].lower
            dfres.loc[i, "ub"] = res.conf_int().loc["treat_post"].upper

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
        # print MDE @ 80% power
        mde = dfpow.loc[(dfpow.index > 0) & (dfpow.power >= 0.8)].head(1).index[0]
        print(f"Min. detectable effect @ 80% power: {mde:0.3g}")

        # plots, if option selected
        if plot:
            # power curve
            dfpowsub = dfpow.loc[dfpow.index >= 0]
            plt.plot(dfpowsub.index, dfpowsub.power, label=f"{period_count} periods")

    # add labels to plot, if necessary
    if plot:
        plt.title(f"Power curve for {outcome_column} using simulation")
        plt.xlabel("True effect")
        plt.ylabel("Power")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5), title="Power Curves")
        plt.ylim(0, 1.1)

    return dfpow.rename_axis(index="effect_size")
