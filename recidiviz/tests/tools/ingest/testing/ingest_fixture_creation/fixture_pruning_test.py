# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests fixture_pruning.py"""
from typing import Any

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.ingest.direct.types.direct_ingest_constants import (
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.fixture_pruning import (
    prune_fixture_data,
)


def _df_from_rows(
    rows: list[list[Any]],
    cols: list[str],
    primary_key_cols: list[str],
) -> pd.DataFrame:
    return (
        pd.DataFrame(rows, columns=cols)
        .sort_values([UPDATE_DATETIME_COL_NAME] + primary_key_cols)
        .reset_index(drop=True)
    )


def test_prune_fixture_data() -> None:

    all_cols = [
        "id",
        "fake_name",
        "value_of_interest",
        UPDATE_DATETIME_COL_NAME,
        IS_DELETED_COL_NAME,
    ]
    primary_key_cols = ["id", "fake_name"]

    # Only two rows per PK, should be the same
    df = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 30, pd.Timestamp("2023-01-01"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    pruned_df = prune_fixture_data(df, primary_key_cols)
    assert_frame_equal(df, pruned_df)

    # Up to two rows per PK, should be the same
    df = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    pruned_df = prune_fixture_data(df, primary_key_cols)
    assert_frame_equal(df, pruned_df)

    # More than two rows per PK, should keep the first and last
    df = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 25, pd.Timestamp("2023-02-01"), False],
            [1, "Alice", 26, pd.Timestamp("2023-08-01"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    expected = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    pruned_df = prune_fixture_data(df, primary_key_cols)
    assert_frame_equal(expected, pruned_df)

    # Every column is a primary key, We expect the second row to drop
    primary_key_cols = ["id", "fake_name", "value_of_interest"]
    df = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 25, pd.Timestamp("2023-02-01"), False],
            [1, "Alice", 25, pd.Timestamp("2023-08-02"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    expected = _df_from_rows(
        [
            [1, "Alice", 25, pd.Timestamp("2023-01-01"), False],
            [1, "Alice", 25, pd.Timestamp("2023-08-02"), False],
            [1, "Alice", 26, pd.Timestamp("2024-01-02"), False],
            [2, "Bob", 31, pd.Timestamp("2024-01-02"), False],
        ],
        all_cols,
        primary_key_cols,
    )
    pruned_df = prune_fixture_data(df, primary_key_cols)
    assert_frame_equal(expected, pruned_df)
