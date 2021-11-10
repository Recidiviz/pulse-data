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
"""Tools for conducting power calculations"""

from logging import warning
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from scipy.stats import t
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm


class PowerCalc:
    """Encapsulate all information necessary to perform a power calculation."""

    def __init__(
        self,
        mde: Optional[float] = None,
        n_clusters: Optional[int] = None,
        n_per_cluster: int = 1,
        icc: Optional[float] = 0.0,
        sigma2: Optional[float] = None,
        tau2: Optional[float] = None,
        alpha: float = 0.05,
        power: float = 0.8,
        percent_treated: float = 0.5,
        pre_periods: Optional[int] = None,
        post_periods: Optional[int] = None,
        compliance_rate: float = 1.0,
    ) -> None:
        """
        Params
        ------
        mde : Optional[float], default None
            Minimum detectable effect

        n_clusters : Optional[int], default None
            Number of clusters (all, including treated and control)

        n_per_cluster : int, default 1
            Number of units per cluster. Set to 1 for person-randomized trials.

        icc : Optional[float], default 0.0
            Intracluster correlation. Set this (or tau2) to 0 for person-randomized
            trials.

        sigma2 : Optional[float], default None
            Between-cluster variance (model variance).

        tau2 : Optional[float], default None
            Within-cluster variance. Set this (or icc) to 0 for person-randomized
            trials. If icc not defined, then sigma2 and tau2 must be defined.

        alpha : float, default 0.05
            Probability of Type I error (i.e. 1 - confidence level)

        power : float, default 0.8
            1 - P(Type II error), i.e. probability of correctly
            rejecting null hypothesis when false

        percent_treated : float, default 0.5
            Proportion of units treated

        pre_periods : Optional[int], default None
            Number of periods pre-treatment, if using panel data

        post_periods : Optional[int], default None
            Number of periods post-treatment, if using panel data

        compliance_rate : float, default 1.0
            Rate of compliance in (0, 1]. Will scale the MDE
            by 1/compliance_rate.
        """

        # assign each param to self
        self.mde = mde
        self.n_clusters = n_clusters
        self.n_per_cluster = n_per_cluster
        self.icc = icc
        self.sigma2 = sigma2
        self.tau2 = tau2
        self.alpha = alpha
        self.power = power
        self.percent_treated = percent_treated
        self.pre_periods = pre_periods
        self.post_periods = post_periods
        self.compliance_rate = compliance_rate

    def checks(self) -> None:
        """Checks that parameters meet needs for power calculations."""
        # check icc OR (sigma2 and tau2) defined
        if self.icc is None and (self.sigma2 is None or self.tau2 is None):
            raise ValueError("Must define `icc` OR (`sigma2` and `tau2`).")

        # calc icc if not defined (which means tau2 and sigma2 must be defined)
        # For person-randomized trials, icc is zero
        if self.icc is None and self.sigma2 is not None and self.tau2 is not None:
            self.icc = self.tau2 / (self.sigma2 + self.tau2)

        # make sure icc and tau2 agree, if both (and sigma2) exist
        if (
            self.icc is not None
            and self.icc < 1
            and self.tau2 is not None
            and self.sigma2 is not None
        ):
            if self.tau2 != self.icc * self.sigma2 / (1 - self.icc):
                raise ValueError(
                    "icc and tau2 do not agree. Try again with tau2 = None."
                )

        # check pre and post periods >= 1
        if self.pre_periods or self.post_periods:
            if not (self.pre_periods and self.post_periods):
                raise ValueError(
                    "Define both pre_periods and post_periods, not only one."
                )
        if self.pre_periods and self.post_periods:
            if not (self.pre_periods >= 1) and (self.post_periods >= 1):
                raise ValueError(
                    f"Please set pre_periods and post_periods >= 1. Current values are {self.pre_periods} and {self.post_periods}."
                )

        # throw warning if icc is zero but there are multiple units per cluster
        if (self.icc == 0) and (self.n_per_cluster > 1):
            warning("`icc` == 0 but `n_per_cluster` > 1, which is unlikely.")

        # check params between correct bounds
        for param in (self.mde, self.pre_periods, self.post_periods):
            if param is not None and param <= 0:
                raise ValueError("mde, pre_periods, and post_periods must be > 0.")
        for param in (self.sigma2, self.tau2):
            if param is not None and param < 0:
                raise ValueError("sigma2 and tau2 must be >= 0.")
        for param in (self.alpha, self.power, self.percent_treated):
            if not 0 < param < 1:
                raise ValueError(
                    "alpha, power, and percent_treated must be between 0 and 1 (exclusive)."
                )
        if self.icc and not 0 <= self.icc <= 1:
            raise ValueError("icc must be between 0 and 1 (inclusive).")
        if self.compliance_rate and not 0 < self.compliance_rate <= 1:
            raise ValueError("compliance rate must be > 0 and <= 1.")

    def adjust_mde(self) -> None:
        """Adjusts MDE for compliance rate and panel data, if applicable."""
        # abort if mde not defined
        if not self.mde:
            raise ValueError("self.mde not defined.")

        # adjust for compliance rate
        self.mde /= self.compliance_rate

        # adjust for panel data, if applicable
        if self.pre_periods and self.post_periods:
            self.mde *= np.sqrt(
                (self.pre_periods + self.post_periods)
                / (self.pre_periods * self.post_periods)
            )

    def get_mde(self) -> float:
        """
        Returns the minimum detectable effect for cluster and
        non-cluster randomized designs using formula-derived power calculations.

        If sigma2 not defined, returns MDE in standard deviations.
        If sigma2 defined, returns MDE in levels (sqrt(sigma2) units).

        For non-clustered designs, set n_clusters=N, n_per_cluster=1, and icc=0.

        For approximate mde accounting for longitudinal data, set pre_periods and
        post_periods >= 1.
        """
        # check assertions and calc icc
        self.checks()

        # make sure necessary params defined
        if self.n_clusters is None:
            raise ValueError("`n_clusters' must be defined.")

        # (unnecessary) param checks for mypy
        if self.icc is None:
            raise ValueError("`icc' must be defined.")

        # check sample size params correctly defined
        if (self.n_clusters <= 1) or not isinstance(self.n_clusters, int):
            raise ValueError("`n_clusters` must be an integer > 1.")
        if (self.n_per_cluster < 1) or not isinstance(self.n_per_cluster, int):
            raise ValueError("`n_per_cluster` must be an integer >= 1.")

        # calc sample size for simplicity
        sample_size = self.n_clusters * self.n_per_cluster

        # Now calculate MDE
        # For simplicity, break up the math into three pieces:
        # c: critical values effect
        # d: design effect
        # s: sample size effect

        # critical values effect
        c = t.ppf(1 - self.alpha / 2, sample_size) + t.ppf(self.power, sample_size)

        # design effect (how much n_per_cluster shrinks due to icc)
        d = 1 + (self.n_per_cluster - 1) * self.icc

        # sample size effect
        s = 1 / (self.percent_treated * (1 - self.percent_treated) * sample_size)

        # mde in standard deviations
        self.mde = c * np.sqrt(d * s)

        # adjust mde for compliance rate or panel data, if applicable:
        self.adjust_mde()

        # units in terms of standard deviations or levels
        if self.sigma2:
            self.mde *= np.sqrt(self.sigma2)  # levels

        if not self.mde:  # for mypy
            raise ValueError("Something went wrong...mde not defined.")

        return self.mde

    def plot_mde_vs(self, x_range: Sequence[float], param: str = "n_clusters") -> None:
        """
        Plots minimum detectable effect (MDE) vs `param`, which can be one of:
        - n_clusters

        Params
        ------

        x_range : Iterable[float]
            The range along the x-axis to plot MDE. Contains two values, the lower and
            upper bounds.

        param : str
            The parameter in the power calculation to use for the x-axis. Acceptable
            values for `param`:
            - n_clusters
        """

        # check that x_range contains two values
        if len(x_range) != 2:
            raise ValueError(
                "`x_range` must contain TWO values: lower and upper bounds."
            )

        # check that x_range elements are strictly increasing
        if x_range[0] >= x_range[1]:
            raise ValueError("`x_range` values must be strictly increasing.")

        # check that `param` is something we have a method for
        acceptable_params = ["n_clusters"]
        if param not in acceptable_params:
            raise ValueError(
                f"`param` must be in {acceptable_params}, which does not include {param}"
            )

        # initialize list of calculated MDEs and x values
        mde_all = []

        # calc for n_clusters
        if param == "n_clusters":

            # check that values in x_range for `n_clusters` param are integers.
            if not all(isinstance(x, int) for x in x_range):
                raise ValueError(
                    "`x_range` values for param `n_clusters` must be integers."
                )

            # check that lower bound of x_range for `n_clusters` is greater than 1.
            if x_range[0] <= 1:
                raise ValueError(
                    "`x_range` values for param `n_clusters` must be greater than 1."
                )

            # store existing value
            temp_val = self.n_clusters

            # generate an array of x values
            step_size = max(np.floor((x_range[1] - x_range[0]) / 100), 1)
            x_vals = np.arange(x_range[0], x_range[1], step_size)

            # loop through x_range getting MDE each round
            for x in x_vals:
                self.n_clusters = x
                mde_all.append(self.get_mde())

            # restore value
            self.n_clusters = temp_val

        # plot
        plt.plot(x_vals, mde_all)
        plt.xlabel(param)
        plt.ylabel("MDE")
        plt.title(f"Minimum detectable effect vs {param}")


def get_icc(
    data: pd.DataFrame, outcome_var: str, group_var: str, print_shrink: bool = False
) -> float:
    """
    Returns intracluster correlation, that is ratio of variance within
    `group_var` to overall model error.

    Formula: ICC = (BMS - WMS) / (BMS + (k - 1) * WMS)
    BMS: between mean squared errors
    WMS: within mean squared errors

    Params
    ------
    data : pd.DataFrame
        DataFrame containing `outcome_var` and `group_var`.

    outcome_var : str
        Column name in `df` containing the measure of interest

    group_var : str
        Column name in `df` containing the cluster grouping.

    print_shrink : bool, default = False
        If true, prints the effective sample size shrink factor. The shrink factor
        tells how much smaller an equally powered cluster-randomized experiment would be
        under zero intracluster correlation.

    """

    # fit linear model containing entity fixed effects
    mod = ols(f"{outcome_var} ~ 1 + C({group_var})", data=data).fit()

    # ANOVA of the linear model
    a = anova_lm(mod)

    # get 'between' and 'within' squared errors
    bms = a["mean_sq"][0]
    wms = a["mean_sq"][1]

    # get average number of units per group
    k = len(data) / len(data[group_var].unique())

    # intracluster-correlation equation
    icc = (bms - wms) / (bms + (k - 1) * wms)

    # print effective sample size
    if print_shrink:
        print(f"Effective sample size shrink factor: {1/(1 + icc * (k - 1))}%")
    return icc
