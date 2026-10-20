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
"""Tools for estimating the effects of experiments, both randomized and natural."""

import math
import warnings
from enum import Enum
from itertools import combinations
from typing import Callable, Iterator, List, NamedTuple, Optional, Union

import attr
import numpy as np
import numpy.typing as npt
import pandas as pd
import statsmodels.formula.api as smf
from linearmodels import PanelOLS
from linearmodels.panel.results import PanelEffectsResults
from scipy import sparse

from recidiviz.common import attr_validators

# Below this many treated clusters, cluster-robust standard errors are biased
# downward and their p-values are too small (MacKinnon and Webb, 2017).
MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE = 10

# With this many clusters or fewer, Rademacher weights allow too few distinct
# bootstrap draws (2 ** n_clusters), so the bootstrap uses Webb's six-point
# weights instead (Webb, 2023).
_MAX_CLUSTERS_FOR_WEBB_WEIGHTS = 11
_WEBB_WEIGHTS = np.array(
    [-np.sqrt(1.5), -1.0, -np.sqrt(0.5), np.sqrt(0.5), 1.0, np.sqrt(1.5)]
)
_RADEMACHER_WEIGHTS = np.array([-1.0, 1.0])

_FIXED_EFFECTS_RELATIVE_TOLERANCE = 1e-10
_FIXED_EFFECTS_MAX_ITERATIONS = 10_000

# A treatment column whose weighted sum of squares shrinks below this fraction
# after residualizing is collinear with the fixed effects and controls.
_ABSORBED_TREATMENT_RELATIVE_TOLERANCE = 1e-10

# The randomization distribution recomputes the observed assignment's t-statistic,
# which can differ from the observed one in the last few bits.
_T_STATISTIC_TIE_RELATIVE_TOLERANCE = 1e-9

_CONFIDENCE_INTERVAL_SEARCH_ITERATIONS = 60
_RANDOMIZATION_ASSIGNMENT_CHUNK_SIZE = 256
_NEVER_TREATED = np.iinfo(np.int64).max

# Width of the PanelOLS summary tables, so DidEffectResult.summary lines up.
_SUMMARY_WIDTH = 78


# function for calculating partial regression residuals of X & Y variable against a set
# of controls
def partial_regression(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
    control_columns: Optional[Union[str, list]] = "1",
    return_append_string: Optional[str] = "_resid",
) -> pd.DataFrame:
    """
    Returns pandas DataFrame consisting of X and Y series residualized on a set of
    control variables via ordinary least squares regression.

    Params:
    ------
    data: pandas DataFrame
    x_column : str
        Column name in `data` with the endogenous variable of interest.

    y_column : str
        Column name in `data` with the exogenous variable of interest.

    control_columns : str or list, default '1'
        List of string column names in `data` associated with other exogenous variables,
        or a string of the right side of the OLS formula consisting of other exogenous
        variables. The effect of these variables will be removed by OLS regression.
        Default ('1') performs no residualization and returns df['x_column'] and
        df['y_column'].

    return_append_string (Optional) : str, default '_resid'
        String to append to residualized `x_column` and `y_column` column names in returned
        dataframe. If no value is provided, append default '_resid'.

    """

    # Checks input type of control variables, and converts `control_columns` object to
    # string formula if input is a list
    if isinstance(control_columns, list):
        control_formula = " + ".join(control_columns)
    elif isinstance(control_columns, str):
        control_formula = control_columns
    else:
        raise ValueError(
            "`control_columns` must be either a list of exogenous variables or a string of"
            " the right side OLS formula"
        )

    # Regress exogenous variable on control variables, and calculate residual errors
    res_model_x = smf.ols(f"{x_column} ~ {control_formula}", data=data).fit()
    # actual = predicted + error
    # error = actual - predicted
    x_error = data[x_column] - res_model_x.predict(data)

    # Regress endogenous variable on control variables, and calculate residual errors
    res_model_y = smf.ols(f"{y_column} ~ {control_formula}", data=data).fit()
    y_error = data[y_column] - res_model_y.predict(data)

    residualized_data = pd.DataFrame(
        {
            f"{x_column}{return_append_string}": x_error,
            f"{y_column}{return_append_string}": y_error,
        }
    )

    return residualized_data


def validate_df(
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    date_column: str,
    weight_column: Optional[str] = None,
    other_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Runs checks on a dataframe to ensure it is suitable for functions in this toolkit
    and subsets to only necessary columns (i.e. those specified in the function
    arguments).

    Also adds weights if not provided.

    Params
    ------
    df : pd.DataFrame
        Dataframe with columns `columns`

    outcome_column : str
        Column name of numeric outcome variable

    unit_of_analysis_column : str
        Column name of categorical column with units of analysis (e.g. districts
        or officers)

    unit_of_treatment_column : str
        Column name of categorical column with units of treatment (e.g. districts
        or officers)

    date_column : str
        Column name of datetime column with dates of observations

    weight_column : Optional[str]
        Column name of numeric column with sample weights (i.e. populations)

    other_columns : Optional[List[str]]
        List of column names of other columns that should be in the dataframe

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
    ...         "date": [
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...         ],
    ...         "post_treat": [False, True, False, False],
    ...         "outcome": [1, 3, 2, 2],
    ...     }
    ... )
    >>> validate_df(df, "outcome", "district", "officer", "treated", "date", "weights", ["post_treat"])
    """

    # add weights if not provided
    if weight_column is None:
        df["weights"] = 1
        weight_column = "weights"

    # ensure all column names in df unique
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("Column names in df must be unique")

    # ensure df has necessary columns
    all_columns = [
        outcome_column,
        unit_of_analysis_column,
        unit_of_treatment_column,
        date_column,
        weight_column,
    ]
    if other_columns:
        all_columns.extend(other_columns)
    for col in all_columns:
        if col not in df.columns:
            raise ValueError(f"Column {col} not in df")

    # check types of columns with known types
    # first, numeric types
    for var in [outcome_column, weight_column]:
        if df[var].dtype not in [int, float]:
            raise TypeError(f"Column `{var}` must be numeric")
    # check unit of analysis and unit of treatment are strings
    for var in [unit_of_analysis_column, unit_of_treatment_column]:
        if df[var].dtype != "object":
            raise TypeError(f"Column `{var}` must be string")
    # date column
    if df[date_column].dtype != "datetime64[ns]":
        raise TypeError(f"Column `{date_column}` must be datetime")

    # subset to necessary columns, max one copy of each
    return_cols = []
    for col in all_columns:
        if col not in return_cols:
            return_cols.append(col)
    df = df[return_cols]

    # ensure no missing values
    if df.isnull().sum().sum() > 0:
        raise ValueError("Missing values found in df")

    df[
        [unit_of_analysis_column, unit_of_treatment_column, date_column]
    ].drop_duplicates()

    return df


# PanelOLS function shared across DiD and Event Study methods
def get_panel_ols_result(
    reg_formula: str,
    df: pd.DataFrame,
    weight_column: str,
    cluster_column: Optional[str] = None,
) -> PanelEffectsResults:
    """
    Returns a PanelEffectsResults object from a regression formula, dataframe,
    weights, and optional cluster column.
    """
    # if cluster_column, estimate clustered standard errors
    if cluster_column:
        clustering_param = {"clusters": df[cluster_column]}
    # otherwise, cluster standard errors on the entity
    else:
        clustering_param = {"cluster_entity": True}

    res = PanelOLS.from_formula(
        reg_formula,
        df,
        drop_absorbed=True,
        # Leave check_rank on: with check_rank=False, linearmodels 7.0 uses the
        # number of dates as the rank of the regressors, mistakes a regressor for a
        # constant when there is one more date than regressors or fewer dates than
        # regressors, and returns wrong coefficients.
        weights=df[weight_column],
    ).fit(cov_type="clustered", **clustering_param)

    return res


def est_did_effect(
    df: pd.DataFrame,
    outcome_column: str,
    interaction_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    date_column: str,
    weight_column: Optional[str] = None,
    cluster_column: Optional[str] = None,
    control_columns: Optional[List[str]] = None,
) -> PanelEffectsResults:
    """
    Estimates the effect of treatment on `outcome` via a
    difference-in-differences framework.

    Params
    ------
    df : pd.DataFrame
        Dataframe with columns as described below

    outcome_column : str
        Column name of numeric outcome variable

    interaction_column : str
        Column name of boolean column = True in post-rollout for treated units

    unit_of_analysis_column : str
        Column name of categorical column with units of analysis (e.g. districts
        or officers)

    date_column : str
        Column name of datetime column with dates of observations

    weight_column: Optional[str], default None
        Column name of numeric column with sample weights (i.e. populations)

    cluster_column : Optional[str], default None
        Column name of categorical column with clusters for clustered standard errors,
        which should be used if the level of treatment is more granular than the
        unit of analysis (e.g. officers within districts)

    control_columns : Optional[List[str]], default None
        List of column names of numeric control variables. These can be included to
        control for variation in the outcome that is not due to the treatment. These
        control variables must vary over time at the unit of analysis level, otherwise
        they will be dropped from the model.

    Returns
    -------
    PanelEffectsResults

    Example Usage
    -------------
    >>> df = pd.DataFrame(
    ...     {
    ...         "district": ["A", "A", "B", "B"],
    ...         "officer": ["C", "C", "D", "D"],
    ...         "treated": [True, True, False, False,]
    ...         "date": [
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...         ],
    ...         "post_treat": [False, True, False, False],
    ...         "outcome": [1, 3, 2, 2],
    ...     }
    ... )
    >>> est_did_effect(
    ...     df,
    ...     outcome_column="outcome",
    ...     interaction_column="post_treat",
    ...     unit_of_analysis_column="district",
    ...     unit_of_treatment_column="officer",
    ...     date_column="date",
    ... )

    """
    res = _fit_did_panel_ols(
        df=df,
        outcome_column=outcome_column,
        interaction_column=interaction_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        cluster_column=cluster_column,
        control_columns=control_columns,
    )
    n_treated_clusters = df.loc[
        df[interaction_column].astype(bool), unit_of_treatment_column
    ].nunique()
    if n_treated_clusters < MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE:
        warnings.warn(
            f"Only [{n_treated_clusters}] units of treatment are treated, fewer than "
            f"[{MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE}], so the clustered standard "
            f"errors and p-values from est_did_effect are too small. Use "
            f"est_did_effect_with_inference for wild cluster bootstrap and "
            f"randomization inference p-values.",
            stacklevel=2,
        )
    return res


def _fit_did_panel_ols(
    *,
    df: pd.DataFrame,
    outcome_column: str,
    interaction_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    date_column: str,
    weight_column: str | None,
    cluster_column: str | None,
    control_columns: list[str] | None,
) -> PanelEffectsResults:
    """Returns the PanelOLS fit described in est_did_effect, which documents the
    arguments."""
    other_columns = [interaction_column]
    if control_columns:
        other_columns.extend(control_columns)
    if cluster_column:
        other_columns.append(cluster_column)
    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    ).copy()
    # impute name of weight_column from validate_df()
    if not weight_column:
        weight_column = "weights"

    # coerce interaction_column to int
    df[interaction_column] = df[interaction_column].astype(int)

    # set entity and time indices
    df = df.set_index([unit_of_analysis_column, date_column])

    # fit model
    reg_formula = (
        f"{outcome_column} ~ -1 + {interaction_column} + EntityEffects + TimeEffects"
    )

    # if control_columniables, add to model
    if control_columns:
        reg_formula += " + " + " + ".join(control_columns)

    # get PanelOLS result
    res = get_panel_ols_result(
        reg_formula=reg_formula,
        df=df,
        weight_column=weight_column,
        cluster_column=cluster_column
        if cluster_column and (cluster_column != unit_of_analysis_column)
        else None,
    )

    return res


class WildBootstrapWeightDistribution(Enum):
    """Distribution of the per-cluster weights that a wild cluster bootstrap
    multiplies residuals by."""

    RADEMACHER = "RADEMACHER"
    """Equal chance of -1 and 1."""

    WEBB = "WEBB"
    """Equal chance of each of -sqrt(3/2), -1, -sqrt(1/2), sqrt(1/2), 1, and
    sqrt(3/2)."""


@attr.define(frozen=True, kw_only=True)
class WildClusterBootstrapResult:
    """Wild cluster restricted bootstrap p-value and confidence interval for a
    difference-in-differences coefficient."""

    p_value: float = attr.ib(validator=attr_validators.is_float_between_zero_and_one)
    """Share of bootstrap draws, generated with the coefficient fixed at zero, whose
    clustered t-statistic is at least as large in absolute value as the observed
    one."""

    confidence_interval_lower: float = attr.ib(validator=attr_validators.is_float)
    """Smallest coefficient the bootstrap test does not reject, or -inf when it
    rejects no value below the estimate."""

    confidence_interval_upper: float = attr.ib(validator=attr_validators.is_float)
    """Largest coefficient the bootstrap test does not reject, or inf when it
    rejects no value above the estimate."""

    confidence_level: float = attr.ib(
        validator=attr_validators.is_float_between_zero_and_one
    )
    """Confidence level of the interval, such as 0.95."""

    weight_distribution: WildBootstrapWeightDistribution = attr.ib(
        validator=attr.validators.in_(WildBootstrapWeightDistribution)
    )
    """Distribution the per-cluster bootstrap weights were drawn from."""

    n_draws: int = attr.ib(validator=attr_validators.is_positive_int)
    """Number of bootstrap draws."""

    is_exhaustive: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the draws are every possible Rademacher weight vector rather than a
    random sample."""


@attr.define(frozen=True, kw_only=True)
class RandomizationInferenceResult:
    """Randomization inference p-value for a difference-in-differences coefficient,
    from refitting the model with the observed adoption dates reassigned across
    units of treatment."""

    p_value: float = attr.ib(validator=attr_validators.is_float_between_zero_and_one)
    """Share of assignments whose clustered t-statistic is at least as large in
    absolute value as the observed one. Sampled assignments count the observed
    assignment once more, so the p-value is never zero."""

    observed_t_statistic: float = attr.ib(validator=attr_validators.is_float)
    """Clustered t-statistic of the coefficient under the real assignment, equal to
    the one in the PanelOLS fit."""

    coefficients: npt.NDArray[np.float64] = attr.ib(
        validator=attr.validators.instance_of(np.ndarray), eq=False, repr=False
    )
    """Coefficient under each assignment, in the same order as t_statistics."""

    t_statistics: npt.NDArray[np.float64] = attr.ib(
        validator=attr.validators.instance_of(np.ndarray), eq=False, repr=False
    )
    """Clustered t-statistic under each assignment. When is_exhaustive is True,
    this includes the real assignment."""

    is_exhaustive: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether the assignments are every distinct reassignment of adoption dates
    rather than a random sample."""

    @property
    def n_assignments(self) -> int:
        return len(self.t_statistics)


@attr.define(frozen=True, kw_only=True)
class DidEffectResult:
    """Difference-in-differences estimate with p-values that stay valid when few
    units of treatment are treated."""

    interaction_column: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the treatment-by-post column, which keys the coefficient in
    panel_ols_result."""

    panel_ols_result: PanelEffectsResults = attr.ib(
        validator=attr.validators.instance_of(PanelEffectsResults)
    )
    """PanelOLS fit with standard errors clustered on the unit of treatment."""

    n_treated_clusters: int = attr.ib(validator=attr_validators.is_positive_int)
    """Number of units of treatment that are ever treated."""

    n_clusters: int = attr.ib(validator=attr_validators.is_positive_int)
    """Number of units of treatment, treated or not."""

    wild_cluster_bootstrap: WildClusterBootstrapResult = attr.ib(
        validator=attr.validators.instance_of(WildClusterBootstrapResult)
    )
    """Wild cluster restricted bootstrap p-value and confidence interval."""

    randomization_inference: RandomizationInferenceResult = attr.ib(
        validator=attr.validators.instance_of(RandomizationInferenceResult)
    )
    """Randomization inference p-value and the distribution behind it."""

    @property
    def coefficient(self) -> float:
        return float(self.panel_ols_result.params[self.interaction_column])

    @property
    def inference_summary(self) -> str:
        """Returns a text table of the few-cluster p-values, formatted to sit under
        the PanelOLS summary."""
        bootstrap = self.wild_cluster_bootstrap
        randomization = self.randomization_inference
        bootstrap_draws = (
            f"all {bootstrap.n_draws}"
            if bootstrap.is_exhaustive
            else f"{bootstrap.n_draws} random"
        )
        assignments = (
            f"all {randomization.n_assignments}"
            if randomization.is_exhaustive
            else f"{randomization.n_assignments} sampled"
        )
        confidence_percent = round(bootstrap.confidence_level * 100)
        rows = [
            (
                "Treated units of treatment",
                f"{self.n_treated_clusters} of {self.n_clusters}",
            ),
            ("Coefficient", f"{self.coefficient:.4f}"),
            (
                "Clustered p-value",
                f"{self.panel_ols_result.pvalues[self.interaction_column]:.4f}",
            ),
            (
                "Wild cluster bootstrap p-value",
                f"{bootstrap.p_value:.4f} ({bootstrap_draws} "
                f"{bootstrap.weight_distribution.value.title()} draws)",
            ),
            (
                f"Wild cluster bootstrap {confidence_percent}% CI",
                f"{bootstrap.confidence_interval_lower:.4f} to "
                f"{bootstrap.confidence_interval_upper:.4f}",
            ),
            (
                "Randomization inference p-value",
                f"{randomization.p_value:.4f} ({assignments} assignments)",
            ),
        ]
        rule = "=" * _SUMMARY_WIDTH
        title = f"Few-Cluster Inference for {self.interaction_column}"
        return "\n".join(
            [
                title.center(_SUMMARY_WIDTH).rstrip(),
                rule,
                *(f"{label + ':':<36}{value}" for label, value in rows),
                rule,
            ]
        )

    @property
    def summary(self) -> str:
        """Returns the PanelOLS summary followed by inference_summary."""
        return f"{self.panel_ols_result.summary}\n\n{self.inference_summary}"

    def __repr__(self) -> str:
        return self.summary


def est_did_effect_with_inference(
    df: pd.DataFrame,
    *,
    outcome_column: str,
    interaction_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    date_column: str,
    weight_column: str | None,
    control_columns: list[str] | None,
    n_bootstrap_draws: int = 9999,
    max_randomization_assignments: int = 10_000,
    confidence_level: float = 0.95,
    seed: int = 0,
) -> DidEffectResult:
    """Estimates a difference-in-differences effect with the same model as
    est_did_effect, and adds two p-values that stay valid when few units of
    treatment are treated.

    Clustered standard errors are too small when fewer than
    MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE units of treatment are treated, which is
    common for facility- or district-level launches. This function clusters on the
    unit of treatment and adds:
    - a wild cluster restricted bootstrap p-value and confidence interval
      (MacKinnon and Webb, 2017), and
    - a randomization inference p-value, which refits the model with the observed
      adoption dates reassigned across units of treatment and ranks the real
      clustered t-statistic among the refits.

    For example, if 2 of 13 facilities launched on the same date, randomization
    inference refits the model for all 78 ways to pick two facilities to treat, and
    its p-value is the share of those 78 fits whose t-statistic is at least as
    extreme as the real one.

    With one to three treated units of treatment, the bootstrap p-value tends to be
    too large, so report the randomization inference p-value. Randomization
    inference treats every unit of treatment as equally likely to have been
    treated, so it is least reliable when the treated units are much larger or
    smaller than the rest.

    Args:
        df: Panel with one row per unit of analysis and date.
        outcome_column: Numeric outcome column.
        interaction_column: Boolean column that is True on and after a unit of
            treatment's adoption date, and False otherwise. Each unit of treatment
            switches from False to True at most once.
        unit_of_analysis_column: String column that gets entity fixed effects.
        unit_of_treatment_column: String column naming the unit treatment was
            assigned to. Standard errors are clustered on it, and randomization
            inference reassigns adoption dates across its values.
        date_column: Datetime column that gets time fixed effects.
        weight_column: Positive numeric column of regression weights, or None to
            weight rows equally. PanelOLS rejects zero or negative weights.
        control_columns: Numeric control columns, or None for no controls.
        n_bootstrap_draws: Number of wild bootstrap draws. When 2 to the power of
            the number of clusters is no larger, the bootstrap uses every Rademacher
            weight vector instead.
        max_randomization_assignments: Largest number of treatment assignments to
            enumerate. When more are possible, this many are sampled at random.
        confidence_level: Confidence level of the bootstrap confidence interval.
        seed: Seed for the random draws.
    """
    if not 0 < confidence_level < 1:
        raise ValueError(
            f"confidence_level must be between 0 and 1, found [{confidence_level}]"
        )
    panel_ols_result = _fit_did_panel_ols(
        df=df,
        outcome_column=outcome_column,
        interaction_column=interaction_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        cluster_column=unit_of_treatment_column,
        control_columns=control_columns,
    )
    panel = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=[interaction_column, *(control_columns or [])],
    )
    # validate_df adds a column of equal weights named "weights" when no weight
    # column is given.
    weights = panel[weight_column or "weights"].to_numpy(dtype=float)

    treatment_unit_codes, treatment_unit_names = pd.factorize(
        panel[unit_of_treatment_column]
    )
    n_clusters = len(treatment_unit_names)
    if n_clusters < 2:
        raise ValueError(
            f"Found [{n_clusters}] units of treatment in column "
            f"[{unit_of_treatment_column}]; clustered inference needs at least 2."
        )
    treatment = panel[interaction_column].astype(bool).to_numpy()
    date_values = panel[date_column].to_numpy(dtype="datetime64[ns]").astype(np.int64)
    adoption_values = _adoption_values_by_treatment_unit(
        treatment=treatment,
        date_values=date_values,
        treatment_unit_codes=treatment_unit_codes,
        treatment_unit_names=treatment_unit_names,
    )

    residualizer = _FixedEffectsResidualizer(
        entity_codes=pd.factorize(panel[unit_of_analysis_column])[0],
        time_codes=pd.factorize(panel[date_column])[0],
        weights=weights,
        controls=(
            panel[control_columns].to_numpy(dtype=float) if control_columns else None
        ),
    )
    cluster_indicators = _indicator_matrix(treatment_unit_codes, n_columns=n_clusters)
    residualized_outcome = residualizer.residualize(
        panel[outcome_column].to_numpy(dtype=float)[:, None]
    )[:, 0]
    observed_treatment = treatment.astype(float)[:, None]
    residualized_treatment = residualizer.residualize(observed_treatment)
    observed_fit = _fit_residualized_treatments(
        raw_treatments=observed_treatment,
        residualized_treatments=residualized_treatment,
        residualized_outcome=residualized_outcome,
        weights=weights,
        cluster_indicators=cluster_indicators,
    )
    if not observed_fit.is_identified[0]:
        raise ValueError(
            f"Column [{interaction_column}] is collinear with the fixed effects and "
            f"controls, so its effect can't be estimated. Check that treated units of "
            f"treatment have rows both before and after their adoption date."
        )
    coefficient = float(observed_fit.coefficients[0])
    panel_ols_coefficient = float(panel_ols_result.params[interaction_column])
    # The bootstrap and randomization inference test this module's own fit, so it
    # must agree with the PanelOLS fit reported alongside them.
    if not math.isclose(coefficient, panel_ols_coefficient, rel_tol=1e-6, abs_tol=1e-9):
        raise ValueError(
            f"PanelOLS estimated [{panel_ols_coefficient}] for [{interaction_column}], "
            f"but the weighted regression on fixed effects estimated [{coefficient}]."
        )
    standard_error = float(observed_fit.standard_errors[0])
    if standard_error == 0:
        raise ValueError(
            f"The clustered standard error of [{interaction_column}] is zero, which "
            f"means the model fits [{outcome_column}] exactly."
        )

    observed_t_statistic = float(panel_ols_result.tstats[interaction_column])
    rng = np.random.default_rng(seed)
    return DidEffectResult(
        interaction_column=interaction_column,
        panel_ols_result=panel_ols_result,
        n_treated_clusters=int((adoption_values != _NEVER_TREATED).sum()),
        n_clusters=n_clusters,
        wild_cluster_bootstrap=_wild_cluster_bootstrap(
            residualizer=residualizer,
            residualized_treatment=residualized_treatment[:, 0],
            residualized_outcome=residualized_outcome,
            weights=weights,
            cluster_indicators=cluster_indicators,
            coefficient=coefficient,
            standard_error=standard_error,
            n_draws=n_bootstrap_draws,
            confidence_level=confidence_level,
            rng=rng,
        ),
        randomization_inference=_randomization_inference(
            residualizer=residualizer,
            residualized_outcome=residualized_outcome,
            weights=weights,
            cluster_indicators=cluster_indicators,
            treatment_unit_codes=treatment_unit_codes,
            treatment_unit_names=treatment_unit_names,
            date_values=date_values,
            adoption_values=adoption_values,
            observed_t_statistic=observed_t_statistic,
            t_statistic_scale=observed_t_statistic / (coefficient / standard_error),
            max_assignments=max_randomization_assignments,
            rng=rng,
        ),
    )


class _FixedEffectsResidualizer:
    """Residualizes columns on entity fixed effects, time fixed effects, and
    controls by weighted least squares."""

    def __init__(
        self,
        *,
        entity_codes: npt.NDArray[np.intp],
        time_codes: npt.NDArray[np.intp],
        weights: npt.NDArray[np.float64],
        controls: npt.NDArray[np.float64] | None,
    ) -> None:
        self._weights = weights
        self._entity_indicators = _indicator_matrix(
            entity_codes, n_columns=int(entity_codes.max()) + 1
        )
        self._time_indicators = _indicator_matrix(
            time_codes, n_columns=int(time_codes.max()) + 1
        )
        self._entity_weight_totals = self._entity_indicators.T @ weights
        self._time_weight_totals = self._time_indicators.T @ weights
        self._residualized_controls = (
            None if controls is None else self._residualize_on_fixed_effects(controls)
        )

    def residualize(self, matrix: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Returns the residuals from regressing each column of matrix on the fixed
        effects and controls."""
        residuals = self._residualize_on_fixed_effects(matrix)
        if self._residualized_controls is None:
            return residuals
        root_weights = np.sqrt(self._weights)[:, None]
        control_coefficients = np.linalg.lstsq(
            self._residualized_controls * root_weights,
            residuals * root_weights,
            rcond=None,
        )[0]
        return residuals - self._residualized_controls @ control_coefficients

    def _residualize_on_fixed_effects(
        self, matrix: npt.NDArray[np.float64]
    ) -> npt.NDArray[np.float64]:
        """Returns matrix with its weighted entity and time means removed, by
        alternating the two demeaning steps until both sets of means are zero."""
        residuals = matrix.astype(float, copy=True)
        tolerance = _FIXED_EFFECTS_RELATIVE_TOLERANCE * max(
            1.0, float(np.abs(residuals).max(initial=0.0))
        )
        for _ in range(_FIXED_EFFECTS_MAX_ITERATIONS):
            residuals -= self._entity_indicators @ self._weighted_group_means(
                self._entity_indicators, self._entity_weight_totals, residuals
            )
            time_means = self._weighted_group_means(
                self._time_indicators, self._time_weight_totals, residuals
            )
            if np.abs(time_means).max(initial=0.0) < tolerance:
                return residuals
            residuals -= self._time_indicators @ time_means
        raise ValueError(
            f"Removing fixed effects did not converge within "
            f"[{_FIXED_EFFECTS_MAX_ITERATIONS}] iterations."
        )

    def _weighted_group_means(
        self,
        indicators: sparse.csr_matrix,
        weight_totals: npt.NDArray[np.float64],
        matrix: npt.NDArray[np.float64],
    ) -> npt.NDArray[np.float64]:
        return (indicators.T @ (self._weights[:, None] * matrix)) / weight_totals[
            :, None
        ]


class _TreatmentFits(NamedTuple):
    coefficients: npt.NDArray[np.float64]
    standard_errors: npt.NDArray[np.float64]
    is_identified: npt.NDArray[np.bool_]


def _indicator_matrix(
    codes: npt.NDArray[np.intp], *, n_columns: int
) -> sparse.csr_matrix:
    """Returns a sparse matrix with a 1 in row i, column codes[i], and 0 elsewhere."""
    return sparse.csr_matrix(
        (np.ones(len(codes)), (np.arange(len(codes)), codes)),
        shape=(len(codes), n_columns),
    )


def _fit_residualized_treatments(
    *,
    raw_treatments: npt.NDArray[np.float64],
    residualized_treatments: npt.NDArray[np.float64],
    residualized_outcome: npt.NDArray[np.float64],
    weights: npt.NDArray[np.float64],
    cluster_indicators: sparse.csr_matrix,
) -> _TreatmentFits:
    """Returns the coefficient and clustered standard error from regressing the
    residualized outcome on each column of residualized_treatments separately.

    The standard errors leave out small-sample scaling. That scaling is the same
    for every column, so it cancels out of any comparison between columns.

    A column is unidentified, and gets NaN for both, when residualizing removed
    nearly all of the variation in the matching raw_treatments column.
    """
    weighted_treatments = weights[:, None] * residualized_treatments
    sums_of_squares = (weighted_treatments * residualized_treatments).sum(axis=0)
    raw_sums_of_squares = (weights[:, None] * raw_treatments**2).sum(axis=0)
    is_identified = (
        sums_of_squares > _ABSORBED_TREATMENT_RELATIVE_TOLERANCE * raw_sums_of_squares
    )
    coefficients = np.divide(
        (weighted_treatments * residualized_outcome[:, None]).sum(axis=0),
        sums_of_squares,
        out=np.full(sums_of_squares.shape, np.nan),
        where=is_identified,
    )
    residuals = residualized_outcome[:, None] - residualized_treatments * np.nan_to_num(
        coefficients
    )
    cluster_scores = cluster_indicators.T @ (weighted_treatments * residuals)
    standard_errors = np.divide(
        np.sqrt((cluster_scores**2).sum(axis=0)),
        sums_of_squares,
        out=np.full(sums_of_squares.shape, np.nan),
        where=is_identified,
    )
    return _TreatmentFits(
        coefficients=coefficients,
        standard_errors=standard_errors,
        is_identified=is_identified,
    )


def _adoption_values_by_treatment_unit(
    *,
    treatment: npt.NDArray[np.bool_],
    date_values: npt.NDArray[np.int64],
    treatment_unit_codes: npt.NDArray[np.intp],
    treatment_unit_names: pd.Index,
) -> npt.NDArray[np.int64]:
    """Returns each unit of treatment's adoption date in int64 nanoseconds, which is
    the first date its interaction column is True, or _NEVER_TREATED when it never
    is."""
    adoption_values = np.full(len(treatment_unit_names), _NEVER_TREATED, dtype=np.int64)
    np.minimum.at(
        adoption_values, treatment_unit_codes[treatment], date_values[treatment]
    )
    is_on_or_after_adoption = date_values >= adoption_values[treatment_unit_codes]
    mismatched_unit_codes = np.unique(
        treatment_unit_codes[is_on_or_after_adoption != treatment]
    )
    if len(mismatched_unit_codes):
        mismatched_units = ", ".join(
            sorted(str(name) for name in treatment_unit_names[mismatched_unit_codes])
        )
        raise ValueError(
            f"Units of treatment [{mismatched_units}] have an interaction column that "
            f"turns False again after first turning True. Make the interaction column "
            f"True on every date from each unit of treatment's adoption date onward."
        )
    return adoption_values


def _draw_wild_bootstrap_weights(
    *, n_clusters: int, n_draws: int, rng: np.random.Generator
) -> tuple[npt.NDArray[np.float64], WildBootstrapWeightDistribution, bool]:
    """Returns an n_clusters x draws matrix of bootstrap weights, the distribution
    they come from, and whether they are every possible Rademacher draw."""
    if n_clusters <= _MAX_CLUSTERS_FOR_WEBB_WEIGHTS:
        return (
            rng.choice(_WEBB_WEIGHTS, size=(n_clusters, n_draws)),
            WildBootstrapWeightDistribution.WEBB,
            False,
        )
    if 2**n_clusters <= n_draws:
        draw_numbers = np.arange(2**n_clusters)
        draw_bits = (draw_numbers[None, :] >> np.arange(n_clusters)[:, None]) & 1
        return (
            (2 * draw_bits - 1).astype(float),
            WildBootstrapWeightDistribution.RADEMACHER,
            True,
        )
    return (
        rng.choice(_RADEMACHER_WEIGHTS, size=(n_clusters, n_draws)),
        WildBootstrapWeightDistribution.RADEMACHER,
        False,
    )


def _wild_cluster_bootstrap(
    *,
    residualizer: _FixedEffectsResidualizer,
    residualized_treatment: npt.NDArray[np.float64],
    residualized_outcome: npt.NDArray[np.float64],
    weights: npt.NDArray[np.float64],
    cluster_indicators: sparse.csr_matrix,
    coefficient: float,
    standard_error: float,
    n_draws: int,
    confidence_level: float,
    rng: np.random.Generator,
) -> WildClusterBootstrapResult:
    """Returns the wild cluster restricted bootstrap p-value for a zero
    coefficient, and the confidence interval from inverting the same test.

    Each draw rebuilds the outcome from the model fit with the coefficient fixed at
    a null value, plus the restricted residuals with every cluster's residuals
    multiplied by one bootstrap weight. Every quantity a draw needs is linear in
    the bootstrap weights and in the null value, so this precomputes a few matrices
    from the per-cluster pieces of the residualized outcome and treatment, and then
    tests any null value with matrix arithmetic instead of refitting the model.
    """
    n_clusters = cluster_indicators.shape[1]
    # Column g of each block holds cluster g's rows and zeros elsewhere, before
    # being residualized as a whole.
    residualized_pieces = residualizer.residualize(
        np.hstack(
            [
                cluster_indicators.multiply(residualized_outcome[:, None]).toarray(),
                cluster_indicators.multiply(residualized_treatment[:, None]).toarray(),
            ]
        )
    )
    outcome_pieces = residualized_pieces[:, :n_clusters]
    treatment_pieces = residualized_pieces[:, n_clusters:]
    weighted_treatment = weights * residualized_treatment
    cluster_sums_of_squares = cluster_indicators.T @ (
        weighted_treatment * residualized_treatment
    )
    sum_of_squares = float(cluster_sums_of_squares.sum())

    (
        bootstrap_weights,
        weight_distribution,
        is_exhaustive,
    ) = _draw_wild_bootstrap_weights(n_clusters=n_clusters, n_draws=n_draws, rng=rng)
    outcome_cross_products = (weighted_treatment @ outcome_pieces) @ bootstrap_weights
    treatment_cross_products = (
        weighted_treatment @ treatment_pieces
    ) @ bootstrap_weights
    outcome_cluster_scores = (
        cluster_indicators.T @ (weighted_treatment[:, None] * outcome_pieces)
    ) @ bootstrap_weights
    treatment_cluster_scores = (
        cluster_indicators.T @ (weighted_treatment[:, None] * treatment_pieces)
    ) @ bootstrap_weights

    def p_value_at(null_coefficient: float) -> float:
        coefficient_deviations = (
            outcome_cross_products - null_coefficient * treatment_cross_products
        ) / sum_of_squares
        cluster_scores = (
            outcome_cluster_scores
            - null_coefficient * treatment_cluster_scores
            - np.outer(cluster_sums_of_squares, coefficient_deviations)
        )
        bootstrap_standard_errors = (
            np.sqrt((cluster_scores**2).sum(axis=0)) / sum_of_squares
        )
        bootstrap_t_statistics = coefficient_deviations / bootstrap_standard_errors
        observed_t_statistic = (coefficient - null_coefficient) / standard_error
        return float(
            np.mean(np.abs(bootstrap_t_statistics) >= abs(observed_t_statistic))
        )

    rejection_level = 1 - confidence_level
    return WildClusterBootstrapResult(
        p_value=p_value_at(0.0),
        confidence_interval_lower=_find_confidence_interval_bound(
            p_value_at=p_value_at,
            estimate=coefficient,
            initial_step=-standard_error,
            rejection_level=rejection_level,
        ),
        confidence_interval_upper=_find_confidence_interval_bound(
            p_value_at=p_value_at,
            estimate=coefficient,
            initial_step=standard_error,
            rejection_level=rejection_level,
        ),
        confidence_level=confidence_level,
        weight_distribution=weight_distribution,
        n_draws=bootstrap_weights.shape[1],
        is_exhaustive=is_exhaustive,
    )


def _find_confidence_interval_bound(
    *,
    p_value_at: Callable[[float], float],
    estimate: float,
    initial_step: float,
    rejection_level: float,
) -> float:
    """Returns the null value, on the side of estimate that initial_step points to,
    where p_value_at crosses rejection_level. Returns an infinite bound when no
    value on that side is rejected."""
    step = initial_step
    not_rejected = estimate
    for _ in range(_CONFIDENCE_INTERVAL_SEARCH_ITERATIONS):
        rejected = estimate + step
        if p_value_at(rejected) < rejection_level:
            break
        not_rejected = rejected
        step *= 2
    else:
        return math.copysign(math.inf, initial_step)
    for _ in range(_CONFIDENCE_INTERVAL_SEARCH_ITERATIONS):
        midpoint = (not_rejected + rejected) / 2
        if p_value_at(midpoint) < rejection_level:
            rejected = midpoint
        else:
            not_rejected = midpoint
    return (not_rejected + rejected) / 2


def _count_distinct_assignments(adoption_values: npt.NDArray[np.int64]) -> int:
    """Returns the number of distinct ways to reassign adoption_values across units
    of treatment."""
    _, counts = np.unique(adoption_values, return_counts=True)
    return math.factorial(len(adoption_values)) // math.prod(
        math.factorial(int(count)) for count in counts
    )


def _distinct_assignments(
    adoption_values: npt.NDArray[np.int64],
) -> Iterator[npt.NDArray[np.int64]]:
    """Yields every distinct reordering of adoption_values, so that each unit of
    treatment gets one adoption value and each value is used as often as it occurs
    in adoption_values."""
    values, counts = np.unique(adoption_values, return_counts=True)

    def assign_from(
        value_index: int,
        open_positions: tuple[int, ...],
        assignment: npt.NDArray[np.int64],
    ) -> Iterator[npt.NDArray[np.int64]]:
        if value_index == len(values):
            yield assignment.copy()
            return
        for chosen_positions in combinations(open_positions, int(counts[value_index])):
            assignment[list(chosen_positions)] = values[value_index]
            yield from assign_from(
                value_index + 1,
                tuple(p for p in open_positions if p not in chosen_positions),
                assignment,
            )

    yield from assign_from(
        0,
        tuple(range(len(adoption_values))),
        np.empty(len(adoption_values), dtype=np.int64),
    )


def _randomization_inference(
    *,
    residualizer: _FixedEffectsResidualizer,
    residualized_outcome: npt.NDArray[np.float64],
    weights: npt.NDArray[np.float64],
    cluster_indicators: sparse.csr_matrix,
    treatment_unit_codes: npt.NDArray[np.intp],
    treatment_unit_names: pd.Index,
    date_values: npt.NDArray[np.int64],
    adoption_values: npt.NDArray[np.int64],
    observed_t_statistic: float,
    t_statistic_scale: float,
    max_assignments: int,
    rng: np.random.Generator,
) -> RandomizationInferenceResult:
    """Returns the randomization inference p-value from refitting the model under
    every distinct reassignment of adoption dates across units of treatment, or
    under max_assignments random reassignments when there are more.

    Multiplying each refit's t-statistic by t_statistic_scale puts it on the same
    scale as observed_t_statistic, which comes from PanelOLS.
    """
    is_exhaustive = _count_distinct_assignments(adoption_values) <= max_assignments
    assignments = (
        np.stack(list(_distinct_assignments(adoption_values)))
        if is_exhaustive
        else np.stack(
            [rng.permutation(adoption_values) for _ in range(max_assignments)]
        )
    )

    coefficient_chunks = []
    t_statistic_chunks = []
    for chunk_start in range(0, len(assignments), _RANDOMIZATION_ASSIGNMENT_CHUNK_SIZE):
        assignment_chunk = assignments[
            chunk_start : chunk_start + _RANDOMIZATION_ASSIGNMENT_CHUNK_SIZE
        ]
        treatments = (
            date_values[:, None] >= assignment_chunk[:, treatment_unit_codes].T
        ).astype(float)
        fits = _fit_residualized_treatments(
            raw_treatments=treatments,
            residualized_treatments=residualizer.residualize(treatments),
            residualized_outcome=residualized_outcome,
            weights=weights,
            cluster_indicators=cluster_indicators,
        )
        if not fits.is_identified.all():
            unidentified_assignment = assignment_chunk[
                int(np.argmin(fits.is_identified))
            ]
            treated_units = ", ".join(
                sorted(
                    str(name)
                    for name in treatment_unit_names[
                        unidentified_assignment != _NEVER_TREATED
                    ]
                )
            )
            raise ValueError(
                f"Reassigning treatment to units of treatment [{treated_units}] makes "
                f"the interaction column collinear with the fixed effects and "
                f"controls. This happens when a unit of treatment has no rows before "
                f"or after an adoption date it can be assigned. Restrict the panel to "
                f"units of treatment with rows on both sides of every adoption date."
            )
        coefficient_chunks.append(fits.coefficients)
        t_statistic_chunks.append(
            t_statistic_scale * fits.coefficients / fits.standard_errors
        )

    t_statistics = np.concatenate(t_statistic_chunks)
    n_as_extreme = int(
        (
            np.abs(t_statistics)
            >= abs(observed_t_statistic) * (1 - _T_STATISTIC_TIE_RELATIVE_TOLERANCE)
        ).sum()
    )
    return RandomizationInferenceResult(
        p_value=(
            n_as_extreme / len(t_statistics)
            if is_exhaustive
            else (n_as_extreme + 1) / (len(t_statistics) + 1)
        ),
        observed_t_statistic=observed_t_statistic,
        coefficients=np.concatenate(coefficient_chunks),
        t_statistics=t_statistics,
        is_exhaustive=is_exhaustive,
    )


def est_es_effect(
    df: pd.DataFrame,
    outcome_column: str,
    treated_column: str,
    unit_of_analysis_column: str,
    unit_of_treatment_column: str,
    date_column: str,
    weight_column: Optional[str] = None,
    cluster_column: Optional[str] = None,
    control_columns: Optional[List[str]] = None,
) -> PanelEffectsResults:
    """
    Estimates the effect of treatment on `outcome` via an event study framework.

    Params
    ------
    df : pd.DataFrame
        Dataframe with columns as described below

    outcome_column : str
        Column name of numeric outcome variable

    treated_column : str
        Column name of boolean column = True in post-rollout periods

    unit_of_analysis_column : str
        Column name of categorical column with units of analysis (e.g. districts
        or officers)

    date_column : str
        Column name of datetime column with dates of observations

    weight_column: Optional[str], default None
        Column name of numeric column with sample weights (i.e. populations)

    cluster_column : Optional[str], default None
        Column name of categorical column with clusters for clustered standard errors,
        which should be used if the level of treatment is more granular than the
        unit of analysis (e.g. officers within districts)

    control_columns : Optional[List[str]], default None
        List of column names of numeric control variables. These can be included to
        control for variation in the outcome that is not due to the treatment. These
        control variables must vary over time at the unit of analysis level, otherwise
        they will be dropped from the model.

    Returns
    -------
    PanelEffectsResults

    Example Usage
    -------------
    >>> df = pd.DataFrame(
    ...     {
    ...         "district": ["A", "A", "A", "A"],
    ...         "officer": ["B", "B", "B", "B"],
    ...         "treated": [False, False, True, True,]
    ...         "date": [
    ...             pd.to_datetime("2020-01-01"),
    ...             pd.to_datetime("2020-02-01"),
    ...             pd.to_datetime("2020-03-01"),
    ...             pd.to_datetime("2020-04-01"),
    ...         ],
    ...         "outcome": [1, 1, 2, 2],
    ...     }
    ... )
    >>> est_es_effect(
    ...     df,
    ...     outcome_column="outcome",
    ...     treated_column="post_treat",
    ...     unit_of_analysis_column="district",
    ...     unit_of_treatment_column="officer",
    ...     date_column="date",
    ... )

    """

    other_columns = [treated_column]
    if control_columns:
        other_columns.extend(control_columns)
    if cluster_column:
        other_columns.append(cluster_column)
    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        unit_of_treatment_column=unit_of_treatment_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    ).copy()
    # impute name of weight_column from validate_df()
    if not weight_column:
        weight_column = "weights"

    # coerce treated_column to int
    df[treated_column] = df[treated_column].astype(int)

    # set entity and time indices
    df = df.set_index([unit_of_analysis_column, date_column])

    # fit model
    reg_formula = f"{outcome_column} ~ -1 + {treated_column} + EntityEffects"

    # if control_columniables, add to model
    if control_columns:
        reg_formula += " + " + " + ".join(control_columns)

    # get PanelOLS result
    res = get_panel_ols_result(
        reg_formula=reg_formula,
        df=df,
        weight_column=weight_column,
        cluster_column=cluster_column
        if cluster_column and (cluster_column != unit_of_analysis_column)
        else None,
    )

    return res


def pairwise_stratify(
    df: pd.DataFrame,
    unit_of_treatment_column: str,
    categorical_columns: Optional[List[str]] = None,
    continuous_column: Optional[str] = None,
    seed: Optional[int] = 42,
    keep_block_and_pair_ids: bool = False,
) -> pd.DataFrame:
    """
    Takes a dataframe `df` with unit of treatment as indicated by
    `unit_of_treatment_column` and a column or set of columns designated by
    `categorical_columns` and `continuous_column` and returns the dataframe with a new
    column "treated" that is a binary indicator for whether the unit is
    treated.

    This function assigns treatment via pairwise stratification where units are
    first grouped by `categorical_columns` and then sorted by `continuous_column` and
    paired. Units are then randomly assigned treatment in pairs.

    By construction, probability of treatment for any unit is 0.5. It is possible to get
    overall imbalanced treatment (P(treated) != 0.5) if there are an odd number of
    units.

    `seed` is used to get deterministic treatment assignments.

    `keep_block_and_pair_ids` returns the dataframe with two additional columns:
    - block_id: the id of the block of units that were grouped together (i.e. using
     `categorical_columns`))
    - pair_id: the id of the pair of units that were paired together (within blocks
     using `continuous_column`)
    """
    # verify that categorical_columns or continuous_column is not None
    if categorical_columns is None and continuous_column is None:
        raise ValueError("Must specify either categorical_columns or continuous_column")

    # verify that included columns are in df
    included_columns = [unit_of_treatment_column]
    if categorical_columns:
        included_columns.extend(categorical_columns)
    if continuous_column:
        included_columns.append(continuous_column)
    for col in included_columns:
        if col not in df.columns:
            raise ValueError(f"Column {col} not in df")

    # keep only relevant vars
    dftmp = df.loc[:, included_columns].copy()

    # get block id
    if categorical_columns:
        # Use ngroup() instead of deprecated grouper.group_info (Pandas 2.0)
        dftmp["group_id"] = dftmp.groupby(categorical_columns).ngroup()
    else:
        dftmp["group_id"] = 1

    # handle no cont_var
    if continuous_column is None:
        dftmp["continuous_column"] = 1
        continuous_column = "continuous_column"

    # get pairs along cont_var within group_id
    # first random number handles ties
    np.random.seed(seed)

    dftmp["randn"] = np.random.uniform(0, 1, len(dftmp))
    dftmp.sort_values(
        ["group_id", continuous_column, "randn"], ascending=False, inplace=True
    )
    dftmp["pair_id"] = np.floor(dftmp.groupby("group_id").cumcount() / 2).astype(int)

    # now randomize order within pairs
    # new random number

    dftmp["randn"] = np.random.uniform(0, 1, len(dftmp))
    dftmp.sort_values(["group_id", "pair_id", "randn"], ascending=False, inplace=True)
    dftmp["treated"] = dftmp.groupby("group_id").cumcount() % 2

    # handle odd numbers within group_id
    for g in dftmp.group_id.unique():
        dfsub = dftmp.loc[dftmp.group_id == g]
        # check if odd number of entries
        if len(dfsub) % 2 == 1:
            # get index of first observation (sorted descending, so first ob is odd)
            idx = dfsub.head(1).index[0]
            # randomly assign treatment to the person
            dftmp.loc[idx, "treated"] = int(np.random.uniform(0, 1) < 0.5)

    # merge `treated` using index and return
    keep_cols = ["treated"]
    if keep_block_and_pair_ids:
        keep_cols.extend(["group_id", "pair_id"])
    df = df.join(dftmp[keep_cols], how="outer")
    return df
