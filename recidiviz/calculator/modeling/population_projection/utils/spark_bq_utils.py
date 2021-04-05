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
"""BigQuery Methods for the Spark population projection simulation"""

import datetime
from typing import List, Dict, Any

import pandas as pd
import pandas_gbq

from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.calculator.modeling.population_projection.utils.bq_utils import (
    store_simulation_results,
)


# Constants for the Spark input data
SPARK_INPUT_PROJECT_ID = "recidiviz-staging"
SPARK_INPUT_DATASET = "spark_public_input_data"

OUTFLOWS_DATA_TABLE_NAME = "outflows_data_raw"
OUTFLOWS_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "time_step", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "outflow_to", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "crime", "type": "STRING", "mode": "NULLABLE"},
    {"name": "crime_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "age", "type": "STRING", "mode": "NULLABLE"},
    {"name": "race", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


TRANSITIONS_DATA_TABLE_NAME = "transitions_data_raw"
TRANSITIONS_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment_duration", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "outflow_to", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "crime", "type": "STRING", "mode": "NULLABLE"},
    {"name": "crime_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "age", "type": "STRING", "mode": "NULLABLE"},
    {"name": "race", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

TOTAL_POPULATION_DATA_TABLE_NAME = "total_population_data_raw"
TOTAL_POPULATION_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "time_step", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "crime", "type": "STRING", "mode": "NULLABLE"},
    {"name": "crime_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "age", "type": "STRING", "mode": "NULLABLE"},
    {"name": "race", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


def _validate_schema(
    schema: List[Dict[str, str]], dataframe: pd.DataFrame, title: str
) -> None:
    "Validate that dataframes match schemas"

    required_columns = {item["name"] for item in schema if item["mode"] == "REQUIRED"}
    allowed_columns = {item["name"] for item in schema}

    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(f"Table '{title}' missing required columns {missing_columns}")

    extra_columns = set(dataframe.columns).difference(allowed_columns)
    if extra_columns:
        raise ValueError(f"Table '{title}' contains unexpected columns {extra_columns}")

    dtype_translation = {
        "STRING": "object",
        "INTEGER": "int64",
        "FLOAT": "float64",
        "TIMESTAMP": "datetime64[ns]",
    }
    for item in schema:
        if dataframe[item["name"]].empty:
            continue
        expected_type = dtype_translation[item["type"]]
        gotten_type = dataframe[item["name"]].dtypes
        if dataframe[item["name"]].dtypes != dtype_translation[item["type"]]:
            raise ValueError(
                f"Table '{title}' has wrong type for column '{item['name']}'. Type '{gotten_type}' should be '{expected_type}'"
            )


def _validate_data(project_id: str, uploads: List[Dict[str, Any]]) -> None:
    "Basic validation of data in dataframes"

    if project_id not in ["recidiviz-staging", "recidiviz-123"]:
        raise ValueError(f"{project_id} is not a supported gcloud BigQuery project")

    # Must include one of the valid disaggregation axes; must not contain null values
    missing_dissagregation_axis = []
    for params in uploads:
        table_name = params["table"][:-4]

        # check for null items in dataframe
        if params["data_df"].isnull().values.any():
            raise ValueError(f"Table '{table_name}' must not contain null values")

        # check that dataframe contains at least one of the required disaggregation axes
        missing_columns = {"crime", "crime_type", "age", "race"}.difference(
            params["data_df"].columns
        )
        if len(missing_columns) >= 4:
            missing_dissagregation_axis.append(params["table"][:-4])

    if len(missing_dissagregation_axis) != 0:
        raise ValueError(
            f"Tables {missing_dissagregation_axis} must have dissaggregation axis of 'crime', 'crime_type', 'age', or 'race'"
        )


def _validate_yaml(yaml_path: str, uploads: List[Dict[str, Any]]) -> None:
    "Validate the contents of the relevant yaml file"

    yaml_dict = YAMLDict.from_path(yaml_path)

    # Check for all required and no extra inputs
    required_inputs = {
        "user_inputs",
        "compartments_architecture",
        "reference_date",
        "time_step",
        "data_inputs",
        "disaggregation_axes",
        "per_year_costs",
    }
    given_inputs = set(yaml_dict.keys())

    missing_inputs = required_inputs.difference(given_inputs)
    if len(missing_inputs) > 0:
        raise ValueError(f"Missing yaml inputs: {missing_inputs}")

    unexpected_inputs = given_inputs.difference(required_inputs)
    if len(unexpected_inputs) > 0:
        raise ValueError(f"Unexpected yaml inputs: {unexpected_inputs}")

    # Check that all disaggregation axes are in all the dataframes
    disaggregation_axes = yaml_dict.pop("disaggregation_axes", list)

    for axis in disaggregation_axes:
        for upload in uploads:
            df = upload["data_df"]
            if axis not in df.columns:
                raise ValueError(
                    f"All disagregation axes must be included in the input dataframe columns\n"
                    f"Expected: {disaggregation_axes}, Actual: {df.columns}"
                )


def upload_spark_model_inputs(
    project_id: str,
    simulation_tag: str,
    outflows_data_df: pd.DataFrame,
    transitions_data_df: pd.DataFrame,
    total_population_data_df: pd.DataFrame,
    yaml_path: str,
) -> None:
    """Reformat the preprocessed model input data to match the table schema and upload them to BigQuery"""
    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()

    uploads = [
        {
            "table": OUTFLOWS_DATA_TABLE_NAME,
            "schema": OUTFLOWS_SCHEMA,
            "data_df": outflows_data_df,
        },
        {
            "table": TRANSITIONS_DATA_TABLE_NAME,
            "schema": TRANSITIONS_SCHEMA,
            "data_df": transitions_data_df,
        },
        {
            "table": TOTAL_POPULATION_DATA_TABLE_NAME,
            "schema": TOTAL_POPULATION_SCHEMA,
            "data_df": total_population_data_df,
        },
    ]
    # Validate that the dataframe data won't cause errors when running simulation
    _validate_data(project_id, uploads)
    # Validate the relevant yaml file
    if yaml_path is not None:
        _validate_yaml(yaml_path, uploads)

    # Process dataframe and check against schema
    for params in uploads:
        if params["data_df"].empty:
            continue
        params["data_df"]["simulation_tag"] = simulation_tag
        params["data_df"]["date_created"] = upload_time
        for disaggregation_axis in ["crime", "crime_type", "age", "race"]:
            if disaggregation_axis not in params["data_df"].columns:
                params["data_df"][disaggregation_axis] = None

        table_name = params["table"][: (len(params["table"]) - 4)]
        _validate_schema(params["schema"], params["data_df"], table_name)

    for params in uploads:
        if params["data_df"].empty:
            continue

        store_simulation_results(
            project_id,
            SPARK_INPUT_DATASET,
            params["table"],
            params["schema"],
            params["data_df"],
        )


def load_spark_table_from_big_query(
    table_name: str, simulation_tag: str
) -> pd.DataFrame:
    """Pull all data from a table for a specific state and run date"""

    query = f"""
        WITH latest_runs AS
        (
        SELECT simulation_tag, MAX(date_created) as latest_run
        FROM {SPARK_INPUT_DATASET}.{table_name}
        GROUP BY simulation_tag
        )
        SELECT data.*
        FROM {SPARK_INPUT_DATASET}.{table_name} data
        LEFT JOIN latest_runs ON data.simulation_tag = latest_runs.simulation_tag
        WHERE data.simulation_tag = '{simulation_tag}'
            AND date_created = latest_runs.latest_run
        """

    table_results = pandas_gbq.read_gbq(query, project_id=SPARK_INPUT_PROJECT_ID)
    return table_results
