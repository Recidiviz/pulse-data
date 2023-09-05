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
"Task Eligibility Spans (TES) methods for analysis"

import datetime
import json
from typing import List

import numpy as np
import pandas as pd


def get_eligible_clients_over_time(
    task_name: str,
    state_code: str,
    project_id: str = "recidiviz-staging",
    date_part_interval: str = "MONTH",
    table_starts_at: str = "2015-01-01",
) -> pd.DataFrame:
    """
    Gets the number of eligible clients over time for a given task from the
    'task_eligibility.all_tasks_materialized' table in BigQuery

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.
        date_part_interval (str): The date part interval. Defaults to 'MONTH'.
        table_starts_at (str): The table starts at. Defaults to "2015-01-01".

    Returns:
        DataFrame: A DataFrame with columns 'date' and 'eligible_folks', representing
            the number of eligible people in prison for each {date_part_interval} (e.g. 'MONTH').
    """
    # Define the SQL query
    query = f"""
    SELECT 
        date,
        COUNT(*) AS eligible_folks,
    FROM `{project_id}.task_eligibility.all_tasks_materialized` s,
    UNNEST(GENERATE_DATE_ARRAY( CAST('{table_starts_at}' AS DATE), 
           CURRENT_DATE('US/Eastern'), 
           INTERVAL 1 {date_part_interval})) AS date
    WHERE date BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE('US/Eastern'))
        AND is_eligible
        AND state_code = '{state_code}'
        AND task_name = '{task_name}'
    GROUP BY date
    ORDER BY 1
    """

    # Read data from BigQuery into a DataFrame
    df = pd.read_gbq(
        query,
        project_id=project_id,
        use_bqstorage_api=True,
    )

    # Convert 'date' column to datetime and set it as the index
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df


def get_task_query(
    task_name: str, state_code: str, project_id: str = "recidiviz-staging"
) -> pd.DataFrame:
    """
    Gets the data for a given task from the 'task_eligibility.all_tasks_materialized'
    table in BigQuery. It also converts the 'start_date' and 'end_date' columns to
    datetime and sets NULL end_dates to the current date.

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.

    Returns:
        pd.DataFrame: DataFrame containing modified task eligibility data.
    """
    # Define the SQL query
    current_query = f"""
    SELECT 
        * EXCEPT (end_date, end_reason, task_name, task_eligibility_span_id),
        IFNULL(end_date, CURRENT_DATE) AS end_date,
    FROM `{project_id}.task_eligibility.all_tasks_materialized` s
    WHERE state_code = '{state_code}'
        AND task_name = '{task_name}'
    ORDER BY 1,2,3
    """

    # Read data from BigQuery into a DataFrame
    df = pd.read_gbq(
        current_query,
        project_id=project_id,
        use_bqstorage_api=True,
    )

    # Convert 'start_date' and 'end_date' columns to datetime
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    return df


def get_criteria_names_from_tes_reasons(df: pd.DataFrame) -> List[str]:
    """
    Takes a TES Task Query data frame and returns a list of criteria names.

    Args:
        df (pd.DataFrame): TES Task Query data frame
    """

    # Retrieve all criteria_names from reason blob
    first_reason_blob = json.loads(df.reasons[0])  # we only need the first one

    return [item["criteria_name"] for item in first_reason_blob]


def gen_tes_spans_by_removing_each_criteria_once(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes as input a data frame with the current eligible population over
    time and removes each criteria to understand how many people would be eligible if
    one individual criteria was removed.

    Takes a TES Task Query data frame and returns some additional columns:
        - remove_{criteria}: 1 if by removing the first criteria, that person becomes
            eligible, 0 if that person would remain ineligible
        - remove_{criteria}: 1 if by removing the second criteria, that person becomes
            eligible, 0 if that person would remain ineligible
        - ...

    Args:
        df (pd.DataFrame): TES Task Query data frame
    """

    # Retrieve all criteria_names from reason blob
    criteria_names = get_criteria_names_from_tes_reasons(df)

    # Number of rows in dataframe
    n_rows = df.shape[0]

    for criteria in criteria_names:
        df["remove_" + criteria] = df["is_eligible"]

        for row in range(n_rows):
            # Check if <criteria> is present in all elements of the "ineligible_criteria"
            #   column of the DataFrame
            if (np.all(np.isin(df["ineligible_criteria"][row], criteria))) & (
                # Only loop over folks who are currently ineligible.
                df["remove_" + criteria][row]
                == np.bool_(False)
            ):
                # Set column associated with that criteria to True
                df.loc[row, "remove_" + criteria] = True

    # Relevant columns for the eligibility graph
    relevant_columns = [
        "state_code",
        "person_id",
        "start_date",
        "end_date",
        "is_eligible",
    ] + [col for col in df.columns if col.startswith("remove_")]

    return df[relevant_columns]


def gen_tes_spans_by_adding_each_criteria_one_by_one(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    This function takes as input a data frame with the candidate population and adds
    each criteria one by one to understand how eligibility changes after each addition.

    I.e. takes a TES Task Query data frame and returns additional columns:
        - everyone_is_eligible: 1 for every span, 0 otherwise
        - add_{criteria}: 1 if the first criteria is met, 0 otherwise
        - add_{criteria}: 1 if the first and second criteria are met, 0 otherwise
        - ...
    The last of these columns should have the same values as is_eligible.

    Args:
        df (pd.DataFrame): TES Task Query data frame
    """

    # Retrieve all criteria_names from reason blob
    criteria_names = get_criteria_names_from_tes_reasons(df)

    # Number of rows in dataframe
    n_rows = df.shape[0]

    # Add criteria one by one and remove folks that become ineligible after each added criteria
    df["everyone_is_eligible"] = 1
    for i, criteria in enumerate(criteria_names):
        # If first criteria, start assuming everyone is eligible
        if i == 0:
            df["add_" + criteria] = 1
        # Otherwise, start with the eligible population left by the previous criteria
        else:
            df["add_" + criteria] = df["add_" + criteria_names[i - 1]]

        for row in range(n_rows):
            # If criteria is in ineligible_criteria
            if (criteria in df["ineligible_criteria"][row]) & (
                df["add_" + criteria][row] == 1
            ):
                # Set column associated with that criteria to 0
                df.loc[row, "add_" + criteria] = 0

    # Relevant columns for the eligibility graph
    relevant_columns = [
        "state_code",
        "person_id",
        "start_date",
        "end_date",
        "everyone_is_eligible",
    ] + [col for col in df.columns if col.startswith("add_")]

    return df[relevant_columns]


def count_people_per_month(
    df: pd.DataFrame, start_date: str = "2015-01-01", frequency: str = "M"
) -> pd.DataFrame:
    """
    Count the number of people eligible per month within a specified date range.

    The pandas version of BigQuery's
        "UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE('US/Eastern'),
            INTERVAL X MONTH), CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH))"

    Parameters:
        df (DataFrame): The DataFrame containing the spans of time with 'start_date'
            and 'end_date' columns.
        start_date (str or datetime): The start date of the date range
            (e.g., '2015-01-01'). Defaults to '2015-01-01'.
        frequency (str): The frequency of the date range ('D' for daily,
            'M' for monthly, etc.). Defaults to 'M'.

    Returns:
        DataFrame: A DataFrame with columns 'Month' and the eligibility columns,
            representing the count of eligible people in prison for each month.
    """

    # Convert input start_date to a pandas datetime object
    start_date = pd.to_datetime(start_date)

    # Always end on today's date
    end_date = pd.Timestamp(datetime.date.today())

    # Create the date range using pandas date_range function
    date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)

    # Create a DataFrame with 'Month' as the cross-join of date_range and df
    month_df = pd.DataFrame({"Month": date_range})
    month_df["key"] = 1

    df["key"] = 1

    merged_df = pd.merge(month_df, df, on="key")

    # Filter the merged DataFrame based on the conditions for start_date and end_date
    filtered_df = merged_df[
        (merged_df["start_date"] <= merged_df["Month"])
        & (merged_df["end_date"] >= merged_df["Month"])
    ]

    # Drop the 'key' column from the copied DataFrame
    filtered_df_copy = filtered_df.drop(columns="key").copy()

    # Group by 'Month' and sum the eligible people for each month
    agg_df = filtered_df_copy.groupby("Month").sum(numeric_only=True)

    # Check if 'person_id' column exists in the DataFrame columns
    if "person_id" in agg_df.columns:
        # If 'person_id' column exists, remove it from the DataFrame
        agg_df.drop(columns=["person_id"], inplace=True)

    return agg_df


def count_tes_spans_per_month_adding_each_criteria_one_by_one(
    task_name: str, state_code: str, project_id: str = "recidiviz-staging"
) -> pd.DataFrame:
    """
    Counts the number of eligible folks per month by adding each of the relevant
    criteria one by one. I.e., the first column of the output is the total candidate
    population, the second column is the number of folks who meet the first criteria,
    the third column is the number of folks who meet the first AND second criteria,
    and so on.

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.

    Returns:
        pd.DataFrame: DataFrame containing processed task data.
    """

    # Pull task query data to PD
    df = get_task_query(task_name, state_code, project_id)

    # Add criteria one by one in the original dataframe
    df = gen_tes_spans_by_adding_each_criteria_one_by_one(df)

    # Group by month
    agg_df = count_people_per_month(df)

    return agg_df


def count_tes_spans_per_month_removing_each_criteria_once(
    task_name: str, state_code: str, project_id: str = "recidiviz-staging"
) -> pd.DataFrame:
    """
    Counts the number of eligible folks per month if we removed one criteria from the
    eligibility list. This does not successively remove each criteria, like
    `count_tes_spans_per_month_adding_each_criteria_one_by_one` does. Instead, it
    looks at people at the margin of eligibility i.e. "who would be eligible if we
    removed this one criteria". This could help us understand which criteria are
    constraining eligibility the most.

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.

    Returns:
        pd.DataFrame: DataFrame containing processed task data.
    """

    # Pull task query data to PD
    df = get_task_query(task_name, state_code, project_id)

    # Remove criteria one by one in the original dataframe
    df = gen_tes_spans_by_removing_each_criteria_once(df)

    # Group by month
    agg_df = count_people_per_month(df)

    # substract the number of eligible folks to each column
    for col in range(1, len(agg_df.columns)):
        agg_df[agg_df.columns[col]] = (
            agg_df[agg_df.columns[col]] - agg_df["is_eligible"]
        )

    agg_df.drop(columns="is_eligible", inplace=True)

    return agg_df
