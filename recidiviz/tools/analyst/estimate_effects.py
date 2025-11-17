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

from typing import List, Optional, Union

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from linearmodels import PanelOLS
from linearmodels.panel.results import PanelEffectsResults


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
    # otherwise, estimate heteroskedasticity-robust standard errors
    else:
        clustering_param = {"cluster_entity": True}

    res = PanelOLS.from_formula(
        reg_formula,
        df,
        drop_absorbed=True,
        check_rank=False,
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
