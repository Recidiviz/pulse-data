# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""BigQuery Methods for both Spark and Ignite population projection simulations"""
from typing import List, Dict

import pandas as pd


def store_simulation_results(
    project_id: str,
    dataset: str,
    table_name: str,
    table_schema: List[Dict[str, str]],
    data: pd.DataFrame,
) -> None:
    """Append the new results to the BigQuery table in the spark output dataset"""

    # Reorder the columns to match the schema ordering
    column_order = [column["name"] for column in table_schema]
    data = data[column_order]

    # Append the results in BigQuery
    data.to_gbq(
        destination_table=f"{dataset}.{table_name}",
        project_id=project_id,
        if_exists="append",
        chunksize=100000,
        table_schema=table_schema,
    )


def add_simulation_date_column(df: pd.DataFrame) -> pd.DataFrame:
    # Convert the fractional year column into the integer year and month columns
    df["year"] = round(df["year"], 5)
    df["month"] = (12 * (df["year"] % 1)).round(0).astype(int) + 1
    df["year"] = df["year"].astype(int)
    df["day"] = 1

    df["simulation_date"] = pd.to_datetime(df[["year", "month", "day"]]).dt.date

    df = df.drop(["year", "month", "day"], axis=1)
    return df
