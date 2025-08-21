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
"""Utility methods for working with fixture files specific to direct ingest"""
import datetime
import os

import numpy as np
import pandas as pd

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
)
from recidiviz.tests.ingest.constants import DEFAULT_UPDATE_DATETIME
from recidiviz.tests.ingest.direct import direct_ingest_fixtures

FIXTURE_ENCODING = "utf-8"
DIRECT_INGEST_FIXTURES_ROOT = os.path.dirname(direct_ingest_fixtures.__file__)
ENUM_PARSING_FIXTURE_SUBDIR = "__enum_parsing_test_fixtures__"
INGEST_MAPPING_OUTPUT_SUBDIR = "__ingest_mapping_output_fixtures__"


def enum_parsing_fixture_path(state_code: StateCode, file_tag: str) -> str:
    return os.path.join(
        DIRECT_INGEST_FIXTURES_ROOT,
        state_code.value.lower(),
        ENUM_PARSING_FIXTURE_SUBDIR,
        f"{file_tag}.csv",
    )


def ingest_mapping_output_fixture_path(
    state_code: StateCode, ingest_view_name: str, characteristic: str
) -> str:
    return os.path.join(
        DIRECT_INGEST_FIXTURES_ROOT,
        state_code.value.lower(),
        INGEST_MAPPING_OUTPUT_SUBDIR,
        ingest_view_name,
        f"{characteristic}.txt",
    )


def fixture_path_for_address(
    state_code: StateCode, address: BigQueryAddress, file_name: str
) -> str:
    """
    Returns the fixture file path for the given BigQueryAddress and fixute file name.
    The file name is either the test characteristic or raw data persona, depending
    on the context in which this is called.
    """
    if state_code != address.state_code_for_address():
        raise ValueError(
            f"Invalid [{state_code.value}] address [{address.to_str()}]. "
            "Ingest fixtures for a given state must correspond to a valid "
            "state-specific dataset for that state (e.g. us_xx_state)."
        )
    return os.path.join(
        DIRECT_INGEST_FIXTURES_ROOT,
        state_code.value.lower(),
        address.dataset_id,
        address.table_id,
        f"{file_name}.csv",
    )


def fixture_path_for_raw_data_dependency(
    state_code: StateCode,
    raw_data_dependency: DirectIngestViewRawFileDependency,
    file_name: str,
) -> str:
    if raw_data_dependency.raw_file_config.is_code_file and state_code not in {
        # TODO(#40113) Update code file fixtures and remove this exemption
        StateCode.US_IX,
        # TODO(#40114) Update code file fixtures and remove this exemption
        StateCode.US_PA,
    }:
        file_name = "__SHARED_CODE_FILE__"
    return os.path.join(
        DIRECT_INGEST_FIXTURES_ROOT,
        state_code.value.lower(),
        f"{state_code.value.lower()}_raw_data",
        raw_data_dependency.file_tag,
        f"{file_name}.csv",
    )


def ingest_view_results_directory(state_code: StateCode) -> str:
    return os.path.join(
        DIRECT_INGEST_FIXTURES_ROOT,
        state_code.value.lower(),
        f"{state_code.value.lower()}_ingest_view_results",
    )


def read_ingest_view_results_fixture(
    state_code: StateCode,
    ingest_view_name: str,
    file_name_w_suffix: str,
    generate_metadata: bool = True,
) -> pd.DataFrame:
    """
    Reads an ingest_view_result fixture and adds default values to metadata
    as if it were created in Dataflow. This allows us to use the fixture
    file in downstream ingest pipeline steps.
    """
    path = os.path.join(
        ingest_view_results_directory(state_code),
        ingest_view_name,
        file_name_w_suffix,
    )
    df = load_dataframe_from_path(path, None)
    # Use space sep in isoformat to match beam test outputs
    if generate_metadata:
        df[MATERIALIZATION_TIME_COL_NAME] = datetime.datetime.now().isoformat(sep=" ")
        df[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat(sep=" ")
    return df


def load_dataframe_from_path(
    raw_fixture_path: str,
    fixture_columns: list[str] | None,
    allow_comments: bool = True,
    encoding: str = FIXTURE_ENCODING,
    separator: str = ",",
) -> pd.DataFrame:
    """Given a raw fixture path and a list of fixture columns, load the raw data into a dataframe."""
    df = pd.read_csv(
        raw_fixture_path,
        usecols=fixture_columns,
        dtype=str,
        # If the first column is an ordinal Pandas will try to use it as the index and
        # shift all of the columns by one unless we tell it not to.
        index_col=False,
        keep_default_na=False,
        na_values=[""],
        # The c engine doesn't like multi-byte comment characters so use the python engine instead.
        engine="python" if allow_comments else "c",
        comment="💬" if allow_comments else None,
        encoding=encoding,
        delimiter=separator,
    )
    df.replace([np.nan], [None], inplace=True)
    return df
