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
"""This module has functionality to prune fixture data to avoid BIG ol' files."""
import pandas as pd

from recidiviz.ingest.direct.types.direct_ingest_constants import (
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)


def prune_fixture_data(df: pd.DataFrame, primary_key_cols: list[str]) -> pd.DataFrame:
    """
    Keeps the earliest and latest rows for each primary key in the fixture data.
    Does not apply to code files

    Args:
        fixture_data: The fixture data to prune.

    Returns:
        The pruned fixture data.
    """
    # Drop deleted rows (pylint said we can't do == False)
    df = df.loc[~df[IS_DELETED_COL_NAME]]
    # If the dependency has no valid primary keys and we pass in all non-metadata columns,
    # then having dropna=False is super essential. Otherwise pain and suffering will follow
    groups = df.sort_values([UPDATE_DATETIME_COL_NAME]).groupby(
        primary_key_cols, dropna=False
    )
    # Get the first and last rows for each group and drop duplicates
    # if the first and last rows are the same (there's only one row in the group)
    # We use head and tail, rather than fist and last because they return a frame
    # without the grouped index
    return (
        pd.concat([groups.head(1), groups.tail(1)], axis=0)
        .drop_duplicates()
        .sort_values([UPDATE_DATETIME_COL_NAME] + primary_key_cols)
        .reset_index(drop=True)
    )
