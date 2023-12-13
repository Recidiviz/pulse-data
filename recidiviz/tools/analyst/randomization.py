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
"""Tools for conducting and testing randomization"""

import numpy as np
import pandas as pd
from scipy import stats


def stratified_randomization(
    df: pd.DataFrame,
    stratas_columns: list,
    share_to_treatment: float = 0.5,
    random_seed: int = 0,
) -> pd.DataFrame:
    """
    Perform stratified randomization on a dataframe. It will assign a treatment to each
    row of the dataframe. The treatment will be assigned randomly, but it will be
    balanced within each strata.

    Args:
        df: dataframe to randomize
        stratas_columns: list of columns to stratify on.
        share_to_treatment: share of units to assign to treatment. Default is 0.5 (half).
    """
    df_copy = df.copy()

    # Create a random uniform value for each row
    rng = np.random.default_rng(random_seed)
    df_copy["random_uniform"] = rng.uniform(0, 1, size=len(df_copy))
    # Sort by blocking columns and random uniform
    df_copy = df_copy.sort_values(stratas_columns + ["random_uniform"])
    # Create a count per block and total per block
    df_copy["count_per_block"] = df_copy.groupby(stratas_columns).cumcount() + 1
    df_copy["total_per_block"] = df_copy.groupby(stratas_columns)[
        stratas_columns[0]
    ].transform("count")
    # For cases where the total_per_block is odd, make some of the counts even randomly.
    #    That way the number of units in both group will be the same on average
    if round(1 / share_to_treatment, 1).is_integer():
        df_copy["total_per_block"] = df_copy["total_per_block"] + (
            (df_copy["total_per_block"] % (1 / share_to_treatment))
            * rng.choice([0, 1], size=len(df_copy))
        )
    # Assign treatment based to the first share_to_treatment of each block
    df_copy["treatment"] = (
        df_copy["count_per_block"] <= (share_to_treatment * df_copy["total_per_block"])
    ).astype(int)
    # Drop helper columns
    df_copy = df_copy.drop(
        ["random_uniform", "count_per_block", "total_per_block"], axis=1
    )

    return df_copy


def orthogonality_table(
    df: pd.DataFrame, columns_to_test: list, treatment_col: str = "treatment"
) -> pd.DataFrame:
    """
    Returns a table of t-statistics and p-values for the difference in means between
    the treatment and control groups. The null hypothesis is that the difference in means
    is zero.

    Args:
        df: A pandas dataframe with the treatment column and columns to test
        columns_to_test: List of column names to test
        treatment_col: Name of the treatment column
    """
    results = []

    for column in columns_to_test:
        group1 = df[df[treatment_col] == 1][column]
        group2 = df[df[treatment_col] == 0][column]

        t_stat, p_value = stats.ttest_ind(
            group1, group2, equal_var=False, nan_policy="omit"
        )

        mean1 = group1.mean()
        mean2 = group2.mean()
        mean_diff = mean1 - mean2

        results.append([column, mean1, mean2, mean_diff, t_stat, p_value])

    results_df = pd.DataFrame(
        results,
        columns=[
            "Column",
            "Treatment",
            "Control",
            "Difference",
            "T-Statistic",
            "P-Value",
        ],
    )
    results_df.loc[len(results_df)] = [
        "Number of units",
        len(df[df["treatment"] == 1]),
        len(df[df["treatment"] == 0]),
    ] + [None] * (len(results_df.columns) - 3)

    return results_df.round(2)
