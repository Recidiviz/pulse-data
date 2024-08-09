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
        * EXCEPT (end_date, start_date, end_reason, task_name, task_eligibility_span_id),
        IFNULL( IF(start_date > '3000-01-01', NULL, start_date), CURRENT_DATE) AS start_date,
        IFNULL( IF(end_date > '3000-01-01', NULL, end_date), CURRENT_DATE) AS end_date,
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
    first_reason_blob = json.loads(df.reasons.iloc[0])  # we only need the first one

    return [item["criteria_name"] for item in first_reason_blob]


def gen_tes_spans_by_removing_each_criteria_once(
    df_task_query: pd.DataFrame,
) -> pd.DataFrame:
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
    criteria_names = get_criteria_names_from_tes_reasons(df_task_query)

    def check_criteria_removal(
        ineligible_criteria: List[str], criteria: str, is_eligible: int
    ) -> bool:
        if is_eligible:
            return True
        # Check if it is the only criteria in ineligible_criteria. We could check more than one criteria at once
        return bool(np.all(np.isin(ineligible_criteria, criteria)))

    vectorized_check = np.vectorize(check_criteria_removal)

    for criteria in criteria_names:
        # Use vectorized operation instead of loop
        df_task_query["remove_" + criteria] = vectorized_check(
            df_task_query["ineligible_criteria"].values,
            criteria,
            df_task_query["is_eligible"].values,
        )

    # Relevant columns for the eligibility graph
    relevant_columns = [
        "state_code",
        "person_id",
        "start_date",
        "end_date",
        "is_eligible",
    ] + [col for col in df_task_query.columns if col.startswith("remove_")]

    return df_task_query[relevant_columns]


def gen_tes_spans_by_adding_each_criteria_one_by_one(
    df_task_query: pd.DataFrame,
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
        df_task_query (pd.DataFrame): TES Task Query data frame
    """
    df_task_query_copy = df_task_query.copy()
    # Retrieve all criteria_names from reason blob
    criteria_names = get_criteria_names_from_tes_reasons(df_task_query_copy)

    # Calculate the impact of each criteria. Necessary to add criteria in correct order
    criteria_names = get_criteria_names_from_tes_reasons(df_task_query)
    final_sorted_lst = []

    # Calculate the impact of each criteria. Select the least impactful, add it to the final list
    # Run this process until we sorted every criteria. This ensures we are adding the least impactful in each step
    while criteria_names:
        criteria_impact = {}
        for criteria in criteria_names:
            # Create a column that indicates who is elegible considering only x criteria
            df_task_query_copy[f"eligible_considering_{criteria}"] = np.where(
                df_task_query_copy["ineligible_criteria"].apply(
                    lambda x, c=criteria: c not in x
                ),
                1,
                0,
            )
            # Elegible population considering x criteria minus elegible ppulation considering all criterion. Impact
            criteria_impact[criteria] = (
                df_task_query_copy[f"eligible_considering_{criteria}"].sum()
                - df_task_query_copy["is_eligible"].sum()
            )

        # Sort criteria from least restrictive to most restrictive
        sorted_criteria = sorted(
            criteria_impact.items(), key=lambda x: x[1], reverse=True
        )
        least_impactful_crtieria = sorted_criteria[0][0]

        # Add the least impactful criteria to the final sorted list
        final_sorted_lst.append(least_impactful_crtieria)

        # Remove the selected criteria from the list
        criteria_names.remove(least_impactful_crtieria)

        # Filter the dataframe
        df_task_query_copy = df_task_query_copy[
            df_task_query_copy[f"eligible_considering_{least_impactful_crtieria}"] == 1
        ]

        # Check if df_set is empty to avoid further processing
        if df_task_query_copy.empty:
            break

    # Add criteria one by one and remove folks that become ineligible after each added criteria
    df_task_query["everyone_is_eligible"] = 1

    def check_criteria(ineligible_criteria: List[str], criteria: str) -> bool:
        return criteria not in ineligible_criteria

    vectorized_check = np.vectorize(check_criteria)

    for i, criteria in enumerate(final_sorted_lst):
        # If first criteria, start assuming everyone is eligible
        if i == 0:
            df_task_query["add_" + criteria] = 1
        # Otherwise, start with the eligible population left by the previous criteria
        else:
            df_task_query["add_" + criteria] = df_task_query[
                "add_" + final_sorted_lst[i - 1]
            ]

        # Use vectorized operation to create mask. Multiply eligible population left by the previous criteria by the mask
        mask = vectorized_check(df_task_query["ineligible_criteria"].values, criteria)
        df_task_query["add_" + criteria] *= mask

    # Relevant columns for the eligibility graph
    relevant_columns = [
        "state_code",
        "person_id",
        "start_date",
        "end_date",
        "everyone_is_eligible",
    ] + [col for col in df_task_query.columns if col.startswith("add_")]

    return df_task_query[relevant_columns]


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

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    results = []
    for month in date_range:
        # Mask to filter df to only keep observations within range
        mask = (df["start_date"] <= month) & (df["end_date"] >= month)
        # Sum observations for numeric columns. This will return the total eligible persons, for example
        monthly_data = df[mask].sum(numeric_only=True)
        # Indicate the month of this observations
        monthly_data["Month"] = month
        results.append(monthly_data)

    # Transform time series into dataframe. Month is the new index and we have one column per numerical column
    agg_df = pd.DataFrame(results).set_index("Month")

    if "person_id" in agg_df.columns:
        agg_df.drop(columns=["person_id"], inplace=True)

    return agg_df


def count_tes_spans_per_month_adding_each_criteria_one_by_one(
    task_name: str, state_code: str, project_id: str = "recidiviz-staging"
) -> pd.DataFrame:
    """
    Counts the number of eligible folks per month by adding each of the relevant
    criteria one by one, from least restrictive to most restrictive. I.e., the first
    column of the output is the total candidate population, the second column is the number
    of folks who meet the first criteria, the third column is the number of folks who
    meet the first AND second criteria, and so on.

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.

    Returns:
        pd.DataFrame: DataFrame containing processed task data.
    """

    # Pull task query data to PD
    df = get_task_query(task_name, state_code, project_id)

    result_df = gen_tes_spans_by_adding_each_criteria_one_by_one(df)

    # Group by month
    agg_df = count_people_per_month(result_df)

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
