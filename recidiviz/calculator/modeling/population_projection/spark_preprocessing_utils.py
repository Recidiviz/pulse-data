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

"""
This file contains a set of utilities that will be useful when constructing the
three dataframes required to run Spark population projections.
"""

# import dependencies
from typing import Optional, Sequence
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import lognorm
import matplotlib.pyplot as plt
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import (
    SIG_FIGS,
)


def transitions_uniform(
    c_from: str,
    c_to: str,
    mean_los: int,
    prob: float,
    round_digits: int = SIG_FIGS,
    max_periods: int = 119,
    disagg_label: str = "x",
) -> pd.DataFrame:
    """
    Creates dataframe with transitions table probabilities
    distributed uniformly between (1, 2 * `mean_los` - 1). See example
    below.

    Params
    ------
    c_from          compartment transitioning from

    c_to            compartment transitioning to

    mean_los        mean duration of event (e.g., in months)

    prob            prob of event. Splits this evenly across the uniform events.

    round_digits    Number of places after decimal to round transition probabilities

    max_periods     Max number of periods to include. Only binds if `max_periods` <
                    0.5 * `mean_los`. Must be odd (why? Because with uniform dist,
                    only possible to maintain expected LOS with odd max_periods).

    disagg_label    String label for disaggregation axis, default is 'x'.

    Example
    -------
    Average time from parole to release is 12 months.
    Probability of release (vs all other events from parole) is 0.8.
    transitions_uniform('parole', 'release',
                        12, 0.8, 5, 119, 'x') produces a dataframe with:

    compartment, outflow_to, crime_type, compartment_duration, total_population
    parole,release,x,1,0.03478
    parole,release,x,2,0.03478
    ...
    parole,release,x,22,0.03478
    parole,release,x,23,0.03478

    Why 23? We maintain the mean duration at 12 months by splitting the interval evenly
     between 1 and 23 months.
    """

    # -- assertions --
    # check types
    for s in [c_from, c_to, disagg_label]:
        if not isinstance(s, str):
            raise ValueError(f"{s} not type str.")
    for i in [mean_los, round_digits, max_periods]:
        if not isinstance(i, int):
            raise ValueError(f"{i} not type int.")
    for f in [prob]:
        if not isinstance(f, float):
            raise ValueError(f"{f} not type float.")

    # check > 0
    if any((x <= 0 for x in [mean_los, prob, max_periods])):
        raise ValueError("mean_los, prob, and max_periods cannot be <= 0.")

    # check max_periods even
    if max_periods % 2 != 1:
        raise ValueError("max_periods must be odd.")
    # -- passed assertions --

    # number of periods in months or years
    # adjust if max periods binds
    n = min(mean_los * 2 - 1, max_periods)
    n = int((n - 1) / 2)  # periods +/- mean LOS

    # get time periods (e.g. months)
    months = range(mean_los - n, mean_los + n + 1)

    # uniform probabilities, check not too small
    prob_u = round(prob / len(months), round_digits)
    if prob_u == 0:
        raise ValueError("Increase round_digits, rounded probabilities too small")

    # create dataframe
    df = pd.DataFrame(
        {
            "compartment": c_from,
            "outflow_to": c_to,
            "crime_type": disagg_label,
            "compartment_duration": months,
            "total_population": [prob_u] * len(months),
        },
        index=range(len(months)),
    )

    return df


def get_lognorm_params(
    xlst: list,
    dlst: list,
    meanbounds: Sequence[int] = (1, 5),
    sdbounds: Sequence[float] = (5, 50),
    splits: int = 10,
    weights: Optional[list] = None,
    print_errs: bool = False,
    grid_search_only: bool = False,
) -> list:
    """
    Uses grid search and scipy.optimize to pick optimal mean and stddev
    parameters for lognorm dist.

    Params
    ------
    xlst              Known x-axis quantities (e.g., months), list of ints

    dlst              Known densities corresponding with xlst, list of floats. Should
                      sum to 1, even if censored or truncated...if not the program will
                      normalize it for you.

    meanbounds        Bounds between which to look for best fit mean parameter

    sdbounds          Bounds between which to look for best fit standard
                      deviation parameter

    splits            Splits used in grid search. Grid will be sized splits ** 2

    weights           If defined, place more weight on corresponding points in xlst

    print_errs        If True, prints errors on each round of optimization

    grid_search_only  If True, ignore the last step of running optimize.minimize
                      over the parameter search

    Example
    -------
    You have recidivism counts for each month for the first 36 months after release.

    months = list(range(1, 37))  # 1, .., 36 months
    recid = [1267, 1613, ..., 572, 542]  # monthly recidivism counts for a state
    densities = [x / sum(recid) for x in recid]  # densities that sum to 1
    meanbounds = [1,5]  # start wide, consider moving narrower. Center on log(mean LOS)
    sdbounds = [10,65]  # start wide, move narrower
    weights = 1 * len(months)  # uniform weights
    weights = list(range(1, 37))  # alternately, more weight on tail

    mean, std = get_lognorm_params(months, dlst, meanbounds, sdbounds,
                50, weights, False, False)
    """

    # -- assertions --
    # check types
    for i in [splits]:
        if not isinstance(i, int):
            raise ValueError(f"{i} not type int.")
    for lvar in [xlst, dlst]:
        if not isinstance(lvar, list):
            raise ValueError(f"{lvar} not type list.")
    for t in [meanbounds, sdbounds]:
        if not isinstance(t, (tuple, list)):
            raise ValueError(f"{t} not type tuple or list.")
    for b in [print_errs, grid_search_only]:
        if not isinstance(b, bool):
            raise ValueError(f"{b} not type bool.")

    # weights, if found
    if weights is not None:
        if not isinstance(weights, list):
            raise ValueError("weights not type list.")
    else:
        weights = [1] * len(xlst)

    # check lists same length
    if len(xlst) != len(dlst) or len(xlst) != len(weights):
        raise ValueError("xlst, dlst, and weights must be the same length.")

    # check elements inside lists
    if not all((isinstance(x, int) for x in xlst)):
        raise ValueError("xlst contains non int elements.")
    for t in [xlst, dlst, meanbounds, sdbounds, weights]:
        if not all((isinstance(x, (int, float)) for x in t)):
            raise ValueError(f"{t} contains non numeric elements.")
        if not all((x > 0 for x in t)):
            raise ValueError(f"{t} contains numbers <= 0, but all must be positive.")

    # check meanbounds and sdbounds > 0
    for v in [meanbounds, sdbounds]:
        if any((x < 0 for x in v)):
            raise ValueError(f"{v} contains negative elements.")
        if v[0] >= v[1]:
            raise ValueError("Set bounds such that right bound > left bound.")
        if len(meanbounds) != 2 or len(sdbounds) != 2:
            raise ValueError("meanbounds and sdbounds must be length 2.")
    # -- passed assertions --

    # get month range
    x_diff = max(xlst) - min(xlst)

    # make sure dlst sums to 1
    if abs(sum(dlst) - 1) > 0.01:
        dlst = [x / sum(dlst) for x in dlst]

    # define loss function
    def loss_function(params):
        """
        Returns weighted squared error.

        pdfx: x values for which pdf is calculated
        pdf: distribution given params
        pdfr: relevant pdf values used to calculate error
        """

        # two params
        mu, sigma = params

        # require both params > 0
        if (mu < 0) | (sigma < 0):
            return 9999

        # get pdf
        # months to estimate pdfs
        pdfx = np.linspace(min(xlst), max(xlst), (x_diff + 1))

        # fit probability density function
        pdf = lognorm.pdf(pdfx, s=mu, loc=0, scale=sigma)

        # scale pdf
        # This is so every element becomes a (sorta) 'monthly' density and sum to 1
        pdf = [j * len(pdf) / sum(pdf) / (x_diff + 1) for j in pdf]

        # get pdf values where we will estimate error
        idx = [j for j, x in enumerate(pdfx) if x in xlst]
        pdfr = [pdf[j] for j in idx]

        # get squared error
        errs = [(x1 - x2) ** 2 * w for x1, x2, w in zip(pdfr, dlst, weights)]
        if print_errs:
            print(errs)

        return sum(errs)

    # optimize over loss function.
    # Basically a grid search, then dive into the best match.
    bestmean = 0
    bestsd = 0
    besterror = 9999
    for mean in np.linspace(meanbounds[0], meanbounds[1], splits):
        for sd in np.linspace(sdbounds[0], sdbounds[1], splits):
            error = loss_function([mean, sd])
            if error < besterror:
                bestmean, bestsd, besterror = mean, sd, error

    if grid_search_only:
        print(f"Mean: {bestmean}, Std: {bestsd}")
        return [bestmean, bestsd]
    # Optimize.minimize finds local minima - be careful!
    x0 = np.array([bestmean, bestsd])  # initial guess
    result = minimize(loss_function, x0)
    print(f'Mean: {result["x"][0]}, Std: {result["x"][1]}')
    return result["x"]


def transitions_lognorm(
    c_from: str,
    c_to: str,
    mean: float,
    std: float,
    x_months: int,
    p_x_months: float,
    last_month: int = 120,
    round_digits: int = SIG_FIGS,
    disagg_label: str = "x",
    plot: bool = False,
) -> pd.DataFrame:
    """
    Creates dataframe with transitions table probabilities
    distributed log-normally with parameters `mean` and `std`.

    It is recommended that these parameters be chosen using the function
    get_lognorm_params.

    Params
    ------
    c_from        compartment transitioning from

    c_to          compartment transitioning to

    mean          mean param for the log-normal distribution (already logged)

    std           standard deviation paramter for the log-normal distribution

    x_months      number of months at benchmark probability `p_x_months`

    p_x_months    benchmark transition probability at `x_months`

    last_month    last month to include in transition probabilities. Default is
                  10 years (120 months).

    round_digits  Number of places after decimal to round transition probabilities

    disagg_label  String label for disaggregation axis, default is 'x'.

    plot          If True, plots transition probabilities over time

    Example
    -------
    We know P(recidivism within 36 months) = 0.33.

    x_months = 36
    p_x_months = 0.33
    mean, std = get_lognorm_params() # 1.5, 20.0

    transitions_lognorm('parole', 'release', mean, std, x_months, p_x_months,
                        120, 5, 'x', False) produces a dataframe with:

    compartment, outflow_to, crime_type, compartment_duration, total_population
    parole,release,x,1,0.01838
    parole,release,x,2,0.02078
    ...
    parole,release,x,119,0.00056
    parole,release,x,120,0.00055
    """

    # -- assertions --
    # check types
    for s in [c_from, c_to, disagg_label]:
        if not isinstance(s, str):
            raise ValueError(f"{s} not type str.")
    for i in [x_months, last_month, round_digits]:
        if not isinstance(i, int):
            raise ValueError(f"{i} not type int.")
    for f in [mean, std, p_x_months]:
        if not isinstance(f, float):
            raise ValueError(f"{f} not type float.")
    for b in [plot]:
        if not isinstance(b, bool):
            raise ValueError(f"{b} not type bool.")

    # check > 0
    for x in [x_months, last_month, mean, std, p_x_months]:
        if x <= 0:
            raise ValueError(f"{x} must be > 0.")

    # check last_month >= x_months
    if last_month < x_months:
        raise ValueError("last_month must be >= x_months")
    # -- passed assertions --

    # Get log-normal pdf from params
    months = np.linspace(1, last_month, last_month, dtype=int)
    pdf = lognorm.pdf(months, s=mean, loc=0, scale=std)

    # scale probabilities to equal one
    # This is only necessary if last_month is sufficiently small
    #  and thereby trims the tail.
    pdf = [x / sum(pdf) for x in pdf]

    # scale probabilities so sum is = P(transition by x_months)
    pdf = [x * p_x_months / sum(pdf[:x_months]) for x in pdf]

    # check smallest probability nonzero after rounding
    pdf = [round(x, round_digits) for x in pdf]
    if pdf[-1] == 0:
        raise ValueError("Increase round_digits, rounded probabilities too small")

    # create dataframe
    df = pd.DataFrame(
        {
            "compartment": c_from,
            "outflow_to": c_to,
            "crime_type": disagg_label,
            "compartment_duration": months,
            "total_population": pdf,
        },
        index=range(len(months)),
    )

    # create plot if option chosen
    if plot:
        # print feedback for checking
        print(
            f"Actual and predicted P(Transition by {x_months} months): "
            f"{sum(pdf[:x_months])}"
        )
        print(f"Predicted P(Transition by {last_month} months): {sum(pdf)}")

        # plot full PDF
        plt.plot(months, pdf, label="Log Normal PDF")
        plt.title("Transition probabilities")
        plt.ylabel("Monthly P(transition)")
        plt.xlabel("Months")

    return df
