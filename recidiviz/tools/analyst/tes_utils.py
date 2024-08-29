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
from typing import List, Union, Dict, Optional, Tuple

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
    task_name: str,
    state_code: str,
    project_id: str = "recidiviz-staging",
    race: bool = False,
    gender: bool = False,
    age: bool = False,
) -> pd.DataFrame:
    """
    Gets the data for a given task from the 'task_eligibility.all_tasks_materialized'
    table in BigQuery. It also converts the 'start_date' and 'end_date' columns to
    datetime and sets NULL end_dates to the current date.

    Args:
        task_name (str): The name of the task. E.g., 'TRANSFER_TO_XCRC_REQUEST'.
        state_code (str): The state code. E.g., 'US_IX'.
        project_id (str): The project ID. Defaults to 'recidiviz-staging'.
        race (bool): Indicates if we want to analyze races
        gender (bool): Indicates if we want to analyze genders
        age (bool): Indicates if we want to analyze age groups

    Returns:
        pd.DataFrame: DataFrame containing modified task eligibility data.
    """
    # Define the SQL query
    # Join demographics if user calls any of them
    if race or gender or age:
        current_query = f"""
        SELECT
            s.* EXCEPT (end_date, start_date, end_reason, task_name, task_eligibility_span_id),
            d.prioritized_race_or_ethnicity AS race,
            d.birthdate AS birthdate,
            d.gender AS gender,
            IF(s.start_date > '3000-01-01', CURRENT_DATE("US/Eastern"), s.start_date) AS start_date,
            IF(s.end_date > '3000-01-01', CURRENT_DATE("US/Eastern"), s.end_date) AS end_date
        FROM `{project_id}.task_eligibility.all_tasks_materialized` s
        LEFT JOIN `{project_id}.sessions.person_demographics_materialized` d
            ON s.person_id = d.person_id
            AND s.state_code = d.state_code
        WHERE s.state_code = '{state_code}'
            AND s.task_name = '{task_name}'
        ORDER BY 1,2,3
        """
    else:
        current_query = f"""
        SELECT
            s.* EXCEPT (end_date, start_date, end_reason, task_name, task_eligibility_span_id),
            IF(s.start_date > '3000-01-01', CURRENT_DATE("US/Eastern"), s.start_date) AS start_date,
            IF(s.end_date > '3000-01-01', CURRENT_DATE("US/Eastern"), s.end_date) AS end_date
        FROM `{project_id}.task_eligibility.all_tasks_materialized` s
        WHERE s.state_code = '{state_code}'
            AND s.task_name = '{task_name}'
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


def sort_criteria_least_to_most_impactful_cumulative_impact(
    criteria_names: List, df_task_query: pd.DataFrame
) -> List:
    """
    Calculate the impact of each criteria. Select the least impactful and add it to the
    final list, remove that name from the criteria_names list. Then, use the remaining
    eligible population (considering the least impactful criteria from the previous step)
    and run the process again. This ensures we add the least impactful criteria in each
    step

    Args:
        crtieria_names (List): list returned by get_criteria_names_from_tes_reasons
        df_task_query_copy (DataFrame): DataFrame that indicates eligible population and
            inelegible_criteria
    """
    df_task_query_copy = df_task_query.copy()
    final_sorted_lst = []
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
    return final_sorted_lst


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
    # Retrieve all criteria_names from reason blob
    criteria_names = get_criteria_names_from_tes_reasons(df_task_query)
    # Sort criteria_names from least impactful to most impactful
    final_sorted_lst = sort_criteria_least_to_most_impactful_cumulative_impact(
        criteria_names, df_task_query
    )

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


def create_dfs_dict_demographics(
    df: pd.DataFrame,
    races: Optional[List[str]] = None,
    genders: Optional[List[str]] = None,
    age_brackets: Optional[List[Tuple[int, int]]] = None,
) -> Dict:
    """
    If we want to analyze demographics, create a dictionary that has the name of the group
    as key and the dataframe as value. Only use it if any demographic element is provided

    Inputs:
        df (DataFrame): output of get_task_query
        races (list): Races we want to analyze (WHITE, BLACK, HISPANIC, etc). Optional
        genders (list): Genders we want to analyze (MALE, FEMALE, TRANS_MALE, etc). Optional
        age_brackets (list): Age groups we want to analyze (Example: [(18, 25), (25, 40), (40, 60), (60, 100)]). Optional
    """
    # Generate cumulative filtered data frames by each demographic indicated
    dfs_dict = {}
    if genders:
        for gen in genders:
            # Filter to have df per gender
            gender_df = df[df["gender"] == gen].copy()
            # Define key using that gender and store filtered df
            dfs_dict[f"gender_{gen}"] = gender_df
    else:
        dfs_dict["all"] = df

    # If we have one df per gender, now break up each one by age bracket. If we didnt have genders, break up initial df
    if age_brackets:
        age_dfs_dict = {}
        for key, gender_df in dfs_dict.items():
            gender_df["birthdate"] = pd.to_datetime(gender_df["birthdate"])
            gender_df["age"] = (
                gender_df["end_date"] - gender_df["birthdate"]
            ).dt.days // 365
            for min_age, max_age in age_brackets:
                gender_age_filtered_df = gender_df[
                    (gender_df["age"] >= min_age) & (gender_df["age"] < max_age)
                ].copy()
                age_key = f"{key}_age_{min_age}_{max_age}"
                age_dfs_dict[age_key] = gender_age_filtered_df
        dfs_dict = age_dfs_dict

    # Break up each df stored by race
    if races:
        race_age_gender_dfs_dict = {}
        for key, gender_age_filtered_df in dfs_dict.items():
            for r in races:
                race_age_gender_filtered_df = gender_age_filtered_df[
                    gender_age_filtered_df["race"] == r
                ].copy()
                race_age_gender_key = f"{key}_race_{r}"
                race_age_gender_dfs_dict[
                    race_age_gender_key
                ] = race_age_gender_filtered_df
        dfs_dict = race_age_gender_dfs_dict

    # Final data frames can be the result of filtering initial df by any combination of demographics
    return dfs_dict


def count_tes_spans_per_month_adding_each_criteria_one_by_one(
    task_name: str,
    state_code: str,
    project_id: str = "recidiviz-staging",
    races: Optional[List[str]] = None,
    genders: Optional[List[str]] = None,
    age_brackets: Optional[List[Tuple[int, int]]] = None,
) -> Union[pd.DataFrame, Dict]:
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
        race (bool): Indicates if we want to analyze races
        gender (bool): Indicates if we want to analyze genders
        age (bool): Indicates if we want to analyze age groups
        races (list): Races we want to analyze (WHITE, BLACK, HISPANIC, etc)
        genders (list): Genders we want to analyze (MALE, FEMALE, TRANS_MALE, etc)
        age_brackets (list): Age groups we want to analyze (Example: [(18, 25), (25, 40), (40, 60), (60, 100)])

    Returns:
        pd.DataFrame: DataFrame containing processed task data.
    """
    # Booleans are true iff respective list is not empty
    race = bool(races)
    gender = bool(genders)
    age = bool(age_brackets)

    if race or gender or age:
        # Pull task query data to PD
        initial_df = get_task_query(
            task_name, state_code, project_id, race, gender, age
        )
        demographics_dfs_dict = create_dfs_dict_demographics(
            initial_df, races, genders, age_brackets
        )
        final_dfs_dict = {}

        for demographic_name, df_set in demographics_dfs_dict.items():
            if df_set.empty:
                print(
                    f"Warning: DataFrame '{demographic_name}' is empty. Skipping processing."
                )
                continue
            # Generate tes spans for each demographic group
            df_set = gen_tes_spans_by_adding_each_criteria_one_by_one(df_set)
            agg_df = count_people_per_month(df_set)
            final_dfs_dict[demographic_name] = agg_df

        return final_dfs_dict

    # If all demographics are False, return only one dataframe
    # Pull task query data to PD
    df = get_task_query(task_name, state_code, project_id)

    result_df = gen_tes_spans_by_adding_each_criteria_one_by_one(df)

    # Group by month
    agg_df = count_people_per_month(result_df)
    return agg_df


def count_tes_spans_per_month_removing_each_criteria_once(
    task_name: str,
    state_code: str,
    project_id: str = "recidiviz-staging",
    races: Optional[List[str]] = None,
    genders: Optional[List[str]] = None,
    age_brackets: Optional[List[Tuple[int, int]]] = None,
) -> Union[pd.DataFrame, Dict]:
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
    # Booleans are true iff respective list is not empty
    race = bool(races)
    gender = bool(genders)
    age = bool(age_brackets)
    # Pull task query data to PD
    all_population_df = get_task_query(
        task_name, state_code, project_id, race, gender, age
    )
    if race or gender or age:
        # Pull task query data to PD
        demographics_dfs_dict = create_dfs_dict_demographics(
            all_population_df, races, genders, age_brackets
        )
        final_dict = {}

        for demographics_name, df_set in demographics_dfs_dict.items():
            if (
                df_set.empty
                or not df_set.select_dtypes(include=[np.number]).columns.any()
            ):
                print(f"Warning: No data to process for {demographics_name}")
                continue
            # Remove criteria one by one in the original dataframe
            all_population_df = gen_tes_spans_by_removing_each_criteria_once(df_set)
            # Group by month
            agg_df = count_people_per_month(all_population_df)
            # substract the number of eligible folks to each column
            for col in range(1, len(agg_df.columns)):
                agg_df[agg_df.columns[col]] = (
                    agg_df[agg_df.columns[col]] - agg_df["is_eligible"]
                )

            agg_df.drop(columns="is_eligible", inplace=True)
            final_dict[demographics_name] = agg_df
        return final_dict

    # If no demographics are provided, return only one dataframe for all population
    # Remove criteria one by one in the original dataframe
    all_population_df = gen_tes_spans_by_removing_each_criteria_once(all_population_df)

    # Group by month
    agg_df = count_people_per_month(all_population_df)

    # substract the number of eligible folks to each column
    for col in range(1, len(agg_df.columns)):
        agg_df[agg_df.columns[col]] = (
            agg_df[agg_df.columns[col]] - agg_df["is_eligible"]
        )

    agg_df.drop(columns="is_eligible", inplace=True)
    return agg_df
