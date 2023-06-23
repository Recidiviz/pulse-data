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
"""Tools for joining data using fuzzy text matching."""

from typing import List, Optional

import pandas as pd
from fuzzywuzzy import fuzz, process


def fuzzy_merge(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    exact_on: Optional[List[str]] = None,
    fuzzy_on: Optional[List[str]] = None,
    match_score_vars: Optional[List[str]] = None,
    match_threshold: int = 80,
) -> pd.DataFrame:
    """
    Function that performs a left join of one dataframe to another dataframe using a
    combination of strict and fuzzy text-matched keys. Returns dataframe containing a
    merged dataframes which may contain some fuzzy-matched row values.

    Params:
    -------
    left_df : pd.DataFrame
        Dataframe on the left of the merge, containing potentially erroneous keys that
        may require fuzzy matching.

    right_df : pd.DataFrame
        Dataframe on the right of the merge, containing reference keys.

    exact_on : list[str]
        List of column names in `df1` and `df2` to use for an exact merge between
        dataframes. If None, perform fuzzy matching on the entire dataframe.

    fuzzy_on : list[str]
        List of column names in `df1` and `df2` with which to perform a fuzzy merge
        between dataframes. If None, use the set of all column names that are shared
        across both dataframes.

    match_score_vars : list[str]
        List of column names in `df1` and `df2` to use for calculating match ratio score
        of the merge, calculated as the average of the match ratio score for each
        column. If None, use the set of all column names that are shared across both
        dataframes.

    match_threshold : int
        Number between 0 and 100 indicating the minimum acceptable match ratio for the
        concatenated strings from `match_score_vars` columns across df1 and df2, as
        determined by the fuzzywuzzy package. Default = 80.

    """

    df1 = left_df.copy()
    df2 = right_df.copy()

    common_cols = list(set(df1.columns) & set(df2.columns))
    df1_only_cols = [col for col in df1.columns if col not in common_cols]
    df2_only_cols = [col for col in df2.columns if col not in common_cols]

    # check for valid match score threshold
    if (match_threshold <= 0) or (match_threshold > 100):
        raise ValueError(f"match_threshold {match_threshold} is not in range (0, 100].")

    # if no variables are specified for calculating fuzzy matching score, use all shared
    # columns
    if not match_score_vars:
        match_score_vars = common_cols

    # if no variables are specified for performing fuzzy merge, use all shared columns
    if not fuzzy_on:
        fuzzy_on = common_cols
        print(f"Fuzzy matching on {', '.join(match_score_vars)}")

    # create a fuzzy match key in the reference dataset
    df2["fuzzy_match_key"] = df2[fuzzy_on].fillna("").agg(" ".join, axis=1)

    # create fuzzy match keys for scoring match quality
    for df in [df1, df2]:
        df["match_score_key"] = df[match_score_vars].fillna("").agg(" ".join, axis=1)

    # if no variables are specified for exact merge, perform fuzzy matching on entire
    # dataframe `df1`.
    if exact_on is None:
        merged_df = df1
        merged_df["match_ratio"] = 0
        exact_matches = pd.DataFrame(columns=["match_ratio"])
        exact_on = []
    else:
        # merge dataframes using exact key match
        merged_df = df1.merge(df2, how="left", on=exact_on)[
            exact_on
            + df1_only_cols
            + [f"{var}_x" for var in common_cols if var not in exact_on]
            + df2_only_cols
            + ["match_score_key_x", "match_score_key_y"]
        ]

        # calculate match score of merged dataframe based on string concatenation of
        # all match_score_vars
        merged_df["match_ratio"] = merged_df.apply(
            lambda x: fuzz.token_sort_ratio(
                x["match_score_key_x"], x["match_score_key_y"]
            ),
            axis=1,
        )
        # clean column names
        merged_df.columns = [
            c.replace("_x", "") if c.endswith("_x") else c for c in merged_df.columns
        ]

        # identify rows with match score that passes specified threshold
        exact_matches = merged_df[merged_df.match_ratio >= match_threshold].copy()

    # identify rows with insufficient match score
    bad_matches = merged_df[merged_df.match_ratio < match_threshold].copy()[
        list(set(exact_on + df1_only_cols + common_cols + ["match_score_key"]))
    ]
    print(f"Fuzzy matching {len(bad_matches)} rows...")

    # perform fuzzy text match to get the closest text matches
    bad_matches["fuzzy_match_key"] = pd.DataFrame(
        bad_matches[fuzzy_on]
        .fillna("")
        .agg(" ".join, axis=1)
        .map(
            lambda x: process.extractOne(
                x, df2.fuzzy_match_key, scorer=fuzz.token_sort_ratio
            )[0]
        )
        .tolist(),
        index=bad_matches.index,
    )

    fuzzy_matches = bad_matches.merge(df2, how="left", on="fuzzy_match_key")[
        df1_only_cols
        # for fuzzy join, use values from right side of join
        + [f"{var}_y" for var in common_cols]
        + df2_only_cols
        + ["match_score_key_x", "match_score_key_y"]
    ]
    fuzzy_matches["match_ratio_fuzzy"] = fuzzy_matches.apply(
        lambda x: fuzz.token_sort_ratio(x["match_score_key_x"], x["match_score_key_y"]),
        axis=1,
    )
    # clean column names
    fuzzy_matches.columns = [
        c.replace("_y", "") if c.endswith("_y") else c for c in fuzzy_matches.columns
    ]
    matched_df = pd.concat(
        [
            exact_matches.assign(is_fuzzy_match=False),
            fuzzy_matches.assign(is_fuzzy_match=True),
        ]
    )

    matched_df["match_ratio_fuzzy"] = matched_df.match_ratio_fuzzy.combine_first(
        matched_df.match_ratio
    )
    print(
        f"Num. fuzzy merged rows with match score < {match_threshold} "
        f"= {len(matched_df[matched_df.match_ratio_fuzzy < match_threshold])}"
    )

    return matched_df[
        df1_only_cols
        + df2_only_cols
        + common_cols
        + ["match_ratio_fuzzy", "is_fuzzy_match"]
    ]
