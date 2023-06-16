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

from typing import List, Optional

import pandas as pd
from linearmodels import PanelOLS
from linearmodels.panel.results import PanelEffectsResults


def validate_df(
    df: pd.DataFrame,
    outcome_column: str,
    unit_of_analysis_column: str,
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
    >>> validate_df(df, "outcome", "district", "date", "treated", ["post_treat"])
    """

    # add weights if not provided
    if weight_column is None:
        df["weights"] = 1
        weight_column = "weights"

    # ensure df has necessary columns
    all_columns = [outcome_column, unit_of_analysis_column, date_column, weight_column]
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
    # check unit of analysis is string
    if df[unit_of_analysis_column].dtype != "object":
        raise TypeError(f"Column `{unit_of_analysis_column}` must be string")
    # date column
    if df[date_column].dtype != "datetime64[ns]":
        raise TypeError(f"Column `{date_column}` must be datetime")

    # subset to necessary columns
    df = df[all_columns]

    # ensure no missing values
    if df.isnull().sum().sum() > 0:
        raise ValueError("Missing values found in df")

    # ensure df is unique on unit of analysis and date
    if len(df) != len(df[[unit_of_analysis_column, date_column]].drop_duplicates()):
        raise ValueError(
            f"Dataframe is not unique on {unit_of_analysis_column} and {date_column}"
        )

    return df


def est_did_effect(
    df: pd.DataFrame,
    outcome_column: str,
    interaction_column: str,
    unit_of_analysis_column: str,
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
    ...     date_column="date",
    ... )

    """

    other_columns = [interaction_column]
    if control_columns:
        other_columns.extend(control_columns)
    df = validate_df(
        df=df,
        outcome_column=outcome_column,
        unit_of_analysis_column=unit_of_analysis_column,
        date_column=date_column,
        weight_column=weight_column,
        other_columns=other_columns,
    )

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

    # if cluster_column, estimate clustered standard errors
    if cluster_column is not None and cluster_column != unit_of_analysis_column:
        res = PanelOLS.from_formula(
            reg_formula,
            df,
            drop_absorbed=True,
            check_rank=False,
            weights=df[weight_column],
        ).fit(cov_type="clustered", clusters=df[cluster_column])

    # otherwise, estimate heteroskedasticity-robust standard errors
    else:
        res = PanelOLS.from_formula(
            reg_formula,
            df,
            drop_absorbed=True,
            check_rank=False,
            weights=df[weight_column],
        ).fit(cov_type="clustered", cluster_entity=True)

    return res
