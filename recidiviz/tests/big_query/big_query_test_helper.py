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
"""Defines a base interface shared by the postgres and bigquery test implementations."""

import abc
import logging
from typing import Dict, List, Type, Union

import pandas as pd


# TODO(#15020): Get rid of this interface once the postgres implementation is deleted.
class BigQueryTestHelper:
    """Interface for functions needed by tests that use BigQuery."""

    @abc.abstractmethod
    def query(self, query: str) -> pd.DataFrame:
        """Returns results from the given query"""

    @staticmethod
    def apply_types_and_sort(
        df: pd.DataFrame,
        data_types: Dict[str, Union[Type, str]],
        sort_dimensions: List[str],
    ) -> pd.DataFrame:
        if not data_types:
            raise ValueError("Found empty data_types")

        if not sort_dimensions:
            raise ValueError("Found empty dimensions")

        # Convert values in dataframe to specified types.
        df = df.astype(data_types)

        # Sets the dimension columns as index columns so we can sort by the values
        # in those columns. This REMOVES the columns from the dataframe data.
        df = df.set_index(sort_dimensions)
        df = df.sort_index()

        # Remove the index columns, which adds those columns back into the Dataframe
        # data.
        df = df.reset_index()

        return df


def query_view(
    helper: BigQueryTestHelper, view_name: str, view_query: str
) -> pd.DataFrame:
    """Returns results from view based on the given query"""
    results = helper.query(view_query)
    # Log results to debug log level, to see them pass --log-level DEBUG to pytest
    logging.debug("Results for `%s`:\n%s", view_name, results.to_string())
    return results
