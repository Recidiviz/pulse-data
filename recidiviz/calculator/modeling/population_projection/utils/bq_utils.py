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
import datetime
from typing import Dict, List

import pandas as pd
from pytz import timezone

# Constants for the Policy Simulation Output data
SPARK_OUTPUT_DATASET = "spark_public_output_data"

COST_AVOIDANCE_TABLE_NAME = "cost_avoidance_estimate_raw"
COST_AVOIDANCE_NON_CUMULATIVE_TABLE_NAME = "cost_avoidance_non_cumulative_estimate_raw"
COST_AVOIDANCE_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_cost", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


LIFE_YEARS_TABLE_NAME = "life_years_estimate_raw"
LIFE_YEARS_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "life_years", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

POPULATION_TABLE_NAME = "population_estimate_raw"
POPULATION_SCHEMA = [
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "scenario", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]


# Constants for the baseline simulation data
BASELINE_PROJECTED_POPULATION_TABLE_NAME = "microsim_projection_raw"
BASELINE_PROJECTED_POPULATION_SCHEMA = [
    # TODO(#9719): migrate `simulation_tag` to `state_code`
    {"name": "simulation_tag", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "compartment_population_min", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "compartment_population_max", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

BASELINE_PROJECTED_OUTFLOWS_TABLE_NAME = "microsim_projected_outflows_raw"
BASELINE_PROJECTED_OUTFLOWS_SCHEMA = [
    {"name": "state_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "outflow_to", "type": "STRING", "mode": "REQUIRED"},
    {"name": "cohort_population", "type": "INT64", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

VALIDATIONS_DATA_TABLE_NAME = "microsim_validation_data_raw"
VALIDATION_SCHEMA = [
    {"name": "state_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "simulation_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "simulation_group", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment", "type": "STRING", "mode": "REQUIRED"},
    {"name": "compartment_population", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "date_created", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "run_date", "type": "DATE", "mode": "REQUIRED"},
]


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


def upload_policy_simulation_results(
    project_id: str,
    simulation_tag: str,
    cost_avoidance_df: pd.DataFrame,
    life_years_df: pd.DataFrame,
    population_change_df: pd.DataFrame,
    cost_avoidance_non_cumulative_df: pd.DataFrame,
) -> None:
    """Reformat the simulation results to match the table schema and upload them to BigQuery"""

    # Set the upload timestamp for all tables
    upload_time = datetime.datetime.now()

    cost_avoidance_table = _format_policy_simulation_results(
        cost_avoidance_df,
        "total_cost",
        simulation_tag=simulation_tag,
        upload_time=upload_time,
    )
    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        COST_AVOIDANCE_TABLE_NAME,
        COST_AVOIDANCE_SCHEMA,
        cost_avoidance_table,
    )

    cost_avoidance_non_cumulative_table = _format_policy_simulation_results(
        cost_avoidance_non_cumulative_df,
        "total_cost",
        simulation_tag=simulation_tag,
        upload_time=upload_time,
    )
    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        COST_AVOIDANCE_NON_CUMULATIVE_TABLE_NAME,
        COST_AVOIDANCE_SCHEMA,
        cost_avoidance_non_cumulative_table,
    )

    life_years_table = _format_policy_simulation_results(
        life_years_df,
        "life_years",
        simulation_tag=simulation_tag,
        upload_time=upload_time,
    )
    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        LIFE_YEARS_TABLE_NAME,
        LIFE_YEARS_SCHEMA,
        life_years_table,
    )

    population_table = _format_policy_simulation_results(
        population_change_df,
        "population",
        simulation_tag=simulation_tag,
        upload_time=upload_time,
    )
    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        POPULATION_TABLE_NAME,
        POPULATION_SCHEMA,
        population_table,
    )


def _format_policy_simulation_results(
    df: pd.DataFrame,
    value_name: str,
    simulation_tag: str,
    upload_time: datetime.datetime,
) -> pd.DataFrame:
    """Change the format of the results to match the table schema with 1 row per compartment, year, and month
    `df` the pandas DataFrame to update
    `value_name` the column name to use for the values in the original results table
    `simulation_tag` the simulation name string to use for the results (typically the state and policy name)
    `upload_time` the timestamp to use for the `date_created` column in the BQ table
    """

    # If the table has 1 row per compartment and 1 column per scenario, then transform to 1 row per compartment/scenario
    if "compartment" in df.columns:
        df = df.melt(
            id_vars=["year", "compartment"], var_name="scenario", value_name=value_name
        )
    # Otherwise, if the table has 1 column per compartment, transform to 1 row per compartment
    else:
        df = df.reset_index().melt(
            id_vars="year", var_name="compartment", value_name=value_name
        )

    # Convert the fractional year column into the integer year and month columns
    df = add_simulation_date_column(df)

    # Add metadata columns to the output table
    df["simulation_tag"] = simulation_tag
    df["date_created"] = upload_time
    return df


def upload_baseline_simulation_results(
    project_id: str,
    microsim_population_df: pd.DataFrame,
    microsim_outflows_df: pd.DataFrame,
    state_code: str,
) -> None:
    """Reformat the simulation results to match the table schema and upload them to BigQuery"""

    # Set the upload timestamp for the population output to the current time in PST
    upload_time = datetime.datetime.now(tz=timezone("US/Pacific"))

    # Add metadata columns to the output tables
    microsim_population_df = add_simulation_date_column(microsim_population_df)
    # TODO(#9719): migrate `simulation_tag` to `state_code`
    microsim_population_df["simulation_tag"] = state_code
    microsim_population_df["date_created"] = upload_time

    microsim_outflows_df = add_simulation_date_column(microsim_outflows_df)
    microsim_outflows_df["state_code"] = state_code
    microsim_outflows_df["date_created"] = upload_time

    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        BASELINE_PROJECTED_POPULATION_TABLE_NAME,
        BASELINE_PROJECTED_POPULATION_SCHEMA,
        microsim_population_df,
    )

    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        BASELINE_PROJECTED_OUTFLOWS_TABLE_NAME,
        BASELINE_PROJECTED_OUTFLOWS_SCHEMA,
        microsim_outflows_df,
    )


def upload_validation_projection_results(
    project_id: str,
    validation_projections_df: pd.DataFrame,
    state_code: str,
) -> None:

    # Set the upload timestamp for the population output to the current time in PST
    upload_time = datetime.datetime.now(tz=timezone("US/Pacific"))

    validation_projections_df = add_simulation_date_column(validation_projections_df)
    validation_projections_df["state_code"] = state_code
    validation_projections_df["date_created"] = upload_time

    store_simulation_results(
        project_id,
        SPARK_OUTPUT_DATASET,
        VALIDATIONS_DATA_TABLE_NAME,
        VALIDATION_SCHEMA,
        validation_projections_df,
    )
