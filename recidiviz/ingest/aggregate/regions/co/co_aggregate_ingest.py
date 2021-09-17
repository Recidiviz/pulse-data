# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Download and parse (turn the data into wide format consistent with other
state aggregate datasets) the Colorado HB19-1297 Jail Data csv.

Author: Albert Sun"""

from typing import Dict

import numpy as np
import pandas as pd
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import fips
from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.persistence.database.schema.aggregate.schema import CoFacilityAggregate


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    table = table.rename(columns={"date_collected": "report_date"})
    table["aggregation_window"] = enum_strings.daily_granularity
    table["report_frequency"] = enum_strings.quarterly_granularity

    return {CoFacilityAggregate: table}


def _parse_table(filename: str) -> pd.DataFrame:
    """
    Parse the Colorado jail data csv by turning the data into wide format
    consistent with other aggregate data integration efforts.

    :return: parsed colorado df
    """

    data = pd.read_csv(filename, encoding="cp1252")
    data = data[data["County"] != "Statewide"]
    county_names = data.County
    data = fips.add_column_to_df(data, county_names, us.states.CO)  # type: ignore

    def label_date(row):
        """Impute date_collected based on collection quarter"""
        md = {1: "1/1", 2: "4/1", 3: "7/1", 4: "10/1"}[row["Qtr"]]
        return f"{md}/{row['QtrYear']}"

    data["date_collected"] = data.apply(label_date, axis=1)
    data["date_collected"] = pd.to_datetime(data["date_collected"])
    data = data.assign(
        datecounty=data["date_collected"].dt.strftime("%Y-%m-%d") + data["County"]
    )

    def move_column_inplace(df, col, pos):
        """Move newly created columns to the front of the dataset"""
        col = df.pop(col)
        df.insert(pos, col.name, col)

    move_column_inplace(data, "date_collected", 0)
    move_column_inplace(data, "fips", 0)

    # We use the unique county jail and date collected as a unique identifier
    # for each column in the final dataset.
    data = data.set_index(["date_collected", "County"])

    # create head, a df that keeps all columns independent from the "measures"
    # used to categorize the data, such as the first 11 columns, i.e. beds,
    # deaths) for each unique county jail at a specific date. we will
    # concatenate all of the columns that are different for different measures
    # to the end of the head df's
    head = data.iloc[:, :9].drop_duplicates()

    # Create a tail df for each type of measure. Concatenate each of the
    # tail df's for each measure to the head df:
    for measure in data.Measure.unique():
        num_inmates_df = data[data.Measure == measure]
        tail = num_inmates_df.iloc[:, 11:21].add_suffix("_" + measure).drop_duplicates()
        head = pd.concat([head, tail], axis=1, join="outer")

    # add a na_message column, which accumulates all na messages for any measure
    # in the CO dataset per county.
    data = data.assign(
        na_message=np.where(
            data["Not Available"].isnull(),
            np.NaN,
            "**" + data.Measure + "**: " + data["Not Available"] + ", ",
        )
    )
    na_mappings = (
        data.fillna("")
        .groupby(["date_collected", "County"])
        .agg({"na_message": "".join})
    )
    final = pd.concat([head, na_mappings], axis=1, join="inner")

    # turn off multi-indexing
    final = final.reset_index()

    # clean column names
    final.columns = final.columns.str.replace(" - ", "_")
    final.columns = final.columns.str.replace(" ", "_")
    final.columns = final.columns.str.replace("-", "_")
    final.columns = final.columns.str.lower()

    return final
