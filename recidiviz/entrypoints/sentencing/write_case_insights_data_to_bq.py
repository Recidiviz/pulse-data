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
"""Script to write Case Insights data to BigQuery
to be called only within the Airflow DAG's KubernetesPodOperator."""
import argparse
import datetime
import json
from typing import Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.tools.analyst.cohort_methods import (
    gen_aggregated_cohort_event_df,
    gen_cohort_time_to_first_event,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

CASE_INSIGHTS_RATES_TABLE_NAME = "sentencing.case_insights_rates"
CASE_INSIGHTS_RATES_SCHEMA = [
    {"name": "state_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_start", "type": "INT64", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_end", "type": "INT64", "mode": "REQUIRED"},
    {"name": "most_severe_description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_rollup", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_num_records", "type": "INT64", "mode": "REQUIRED"},
    {"name": "recidivism_probation_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_rider_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_term_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "disposition_num_records", "type": "INT64", "mode": "REQUIRED"},
    {"name": "disposition_probation_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_rider_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_term_pc", "type": "FLOAT", "mode": "REQUIRED"},
]
# Comma-delimited strings of supported states
SUPPORTED_STATES_STR = "'US_IX'"
UNKNOWN_ATTRIBUTE = "UNKNOWN"


class WriteRecidivismRatesToBQEntrypoint(EntrypointInterface):
    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the recidivism rate write to BQ process."""
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--project-id",
            dest="project_id",
            help="Which project to write to.",
            type=str,
            choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        write_case_insights_data_to_bq(project_id=args.project_id)


def get_gendered_assessment_score_bucket_range(
    cohort_df_row: pd.Series,
) -> Tuple[int, int]:
    """Get the risk bucket range associated with the given gender and LSI-R assessment score.

    These buckets were defined in an Idaho-specific memo:
    https://drive.google.com/file/d/1o7vWvQ507SUvCPShyyOZFXQq53wT2D0J/view.

    This returns an assessment_score_bucket_start and assessment_score_bucket_end, which are inclusive endpoints of the
    bucket range. If the bucket has no upper limit (e.g. "29+") then assessment_score_bucket_end is -1. If the
    input assessment_score is invalid or the gender is unhandled, both return values are -1.

    TODO(#31126): Revisit these buckets if we start working with other states that call for different buckets.
    """
    assessment_score = cohort_df_row["assessment_score"]
    if pd.isnull(assessment_score) or assessment_score < 0:
        return -1, -1
    if cohort_df_row["gender"] == "MALE":
        if assessment_score <= 20:
            return 0, 20
        if assessment_score <= 28:
            return 21, 28
        return 29, -1
    if cohort_df_row["gender"] == "FEMALE":
        if assessment_score <= 22:
            return 0, 22
        if assessment_score <= 30:
            return 23, 30
        return 31, -1
    # This doesn't handle EXTERNAL_UNKNOWN, TRANS_MALE, TRANS_FEMALE
    return -1, -1


def get_cohort_df(project_id: str) -> pd.DataFrame:
    cohort_query = f"""
    SELECT *
    FROM `sentencing_views.sentence_cohort`
    WHERE state_code IN ({SUPPORTED_STATES_STR})
    """
    cohort_df = pd.read_gbq(cohort_query, project_id=project_id)

    return cohort_df


def get_recidivism_event_df(project_id: str) -> pd.DataFrame:
    """Get recidivism events, which are all INCARCERATION starts or PROBATION starts inflowing from LIBERTY."""

    event_query = f"""
    SELECT *
    FROM `sentencing_views.recidivism_event`
    WHERE state_code IN ({SUPPORTED_STATES_STR})
    """
    event_df = pd.read_gbq(event_query, project_id=project_id)

    return event_df


def get_disposition_df(cohort_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns: DataFrame with columns 'gender', 'assessment_score_bucket_start', 'assessment_score_bucket_end',
        'most_severe_description', 'disposition_probation_pc', 'disposition_rider_pc', 'disposition_term_pc'
    """
    disposition_df = (
        cohort_df.groupby(
            [
                "state_code",
                "gender",
                "assessment_score_bucket_start",
                "assessment_score_bucket_end",
                "most_severe_description",
                "cohort_group",
            ]
        )
        .size()
        .unstack(level="cohort_group")
        .rename_axis(columns=None)
        .fillna(0)
    )

    # Convert counts to percentages
    disposition_df = disposition_df.div(disposition_df.sum(axis=1), axis=0)
    # Rename columns
    disposition_df = disposition_df.rename(
        columns=lambda x: "disposition_" + x.lower() + "_pc"
    )

    return disposition_df.reset_index()


# TODO(#31104): Move CI calculation to a centralized util method
def add_cis(df: pd.DataFrame) -> None:
    def calculate_ci(
        event_rate: float, cohort_size: int, alpha: float = 0.05
    ) -> Tuple[float, float]:
        z = norm.ppf(1 - alpha / 2)
        se = np.sqrt(event_rate * (1 - event_rate) / cohort_size)
        y1 = event_rate - z * se
        y2 = event_rate + z * se

        return y1, y2

    df["lower_ci"] = df.apply(
        # lower_ci can't be lower than 0%
        lambda row: max(0.0, calculate_ci(row.event_rate, row.cohort_size)[0]),
        axis=1,
    )
    df["upper_ci"] = df.apply(
        # upper_ci can't be higher than 100%
        lambda row: min(1.0, calculate_ci(row.event_rate, row.cohort_size)[1]),
        axis=1,
    )
    df["ci_size"] = df["upper_ci"] - df["lower_ci"]


def add_recidivism_rate_dicts(df: pd.DataFrame) -> None:
    df["event_rate_dict"] = (
        df[["cohort_months", "event_rate", "lower_ci", "upper_ci"]]
        .astype("O")
        .apply(lambda row: row.to_dict(), axis=1)
    )


def get_recidivism_series(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """For each gender, assessment_score_bucket_start, assessment_score_bucket_end, and most_severe_description,
    concatenate event_rate_dict for all cohort_months into a single JSON array.
    """

    return (
        aggregated_df.groupby(
            [
                "cohort_group",
                "state_code",
                "gender",
                "assessment_score_bucket_start",
                "assessment_score_bucket_end",
                "most_severe_description",
            ],
            group_keys=True,
        )["event_rate_dict"]
        .apply(lambda event_rate_dicts: json.dumps(list(event_rate_dicts)))
        .unstack(level="cohort_group")
        .rename_axis(columns=None)
        .rename(columns=lambda x: "recidivism_" + x.lower() + "_series")
    )


def get_cohort_sizes(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Returns dataframe with gender, assessment_score_bucket_start, assessment_score_bucket_end,
    most_severe_description as index and cohort_size as column"""
    # Cohort size is the same for all months, so take the sum (across cohort_groups) for cohort_months == 0
    return (
        aggregated_df[aggregated_df.cohort_months == 0]
        .groupby(
            [
                "state_code",
                "gender",
                "assessment_score_bucket_start",
                "assessment_score_bucket_end",
                "most_severe_description",
            ]
        )["cohort_size"]
        .sum()
        .to_frame()
    )


def create_final_table(
    aggregated_df: pd.DataFrame, disposition_df: pd.DataFrame
) -> pd.DataFrame:
    """Create final table from aggregated_df and disposition_df.

    This method creates a row for every state_code, gender, assessment_score_bucket_start, assessment_score_bucket_end,
    most_severe_description with:
    * The cohort size (copied to both disposition_num_records and recidivism_num_records)
    * The series of recidivism stats over all months for each of PROBATION, RIDER, TERM
    * The disposition percentages of each of  PROBATION, RIDER, TERM
    * recidivism_rollup (just a copy of most_severe_description for now)
    * gender and risk_score_bucket
    """
    final_table = get_recidivism_series(aggregated_df)
    final_table["cohort_size"] = get_cohort_sizes(aggregated_df)

    # Move index to columns
    final_table = final_table.reset_index()

    final_table = final_table.merge(
        disposition_df,
        on=[
            "state_code",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_description",
        ],
    )

    final_table["disposition_num_records"] = final_table["cohort_size"]
    # Since we're not yet rolling up, recidivism_num_records is the overall cohort_size and rollup is the offense
    # TODO(#30755): Add rollup, which will change the recidivism_num_records calculation
    final_table["recidivism_num_records"] = final_table["cohort_size"]
    final_table["recidivism_rollup"] = final_table["most_severe_description"]

    return final_table


def write_case_insights_data_to_bq(project_id: str) -> None:
    """Write case insights data to BigQuery."""
    cohort_df = get_cohort_df(project_id)
    cohort_df[
        ["assessment_score_bucket_start", "assessment_score_bucket_end"]
    ] = cohort_df.apply(
        get_gendered_assessment_score_bucket_range, axis=1, result_type="expand"
    )
    event_df = get_recidivism_event_df(project_id)
    disposition_df = get_disposition_df(cohort_df)

    recidivism_df = gen_cohort_time_to_first_event(
        cohort_df=cohort_df,
        event_df=event_df,
        cohort_date_field="cohort_start_date",
        event_date_field="recidivism_date",
        join_field=["person_id", "state_code"],
    )

    aggregated_df = gen_aggregated_cohort_event_df(
        recidivism_df,
        cohort_date_field="cohort_start_date",
        event_date_field="recidivism_date",
        time_index=[0, 3, 6, 9, 12, 18, 24, 30, 36],
        time_unit="months",
        cohort_attribute_col=[
            "state_code",
            "cohort_group",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_description",
        ],
        last_day_of_data=datetime.datetime.now(),
    )
    add_cis(aggregated_df)
    # Move cohort_months from index to column
    aggregated_df = aggregated_df.reset_index(["cohort_months"])
    add_recidivism_rate_dicts(aggregated_df)

    final_table = create_final_table(aggregated_df, disposition_df)

    schema_cols = list(map(lambda x: x["name"], CASE_INSIGHTS_RATES_SCHEMA))
    final_table = final_table[schema_cols]
    final_table.to_gbq(
        destination_table=CASE_INSIGHTS_RATES_TABLE_NAME,
        project_id=project_id,
        table_schema=CASE_INSIGHTS_RATES_SCHEMA,
        if_exists="replace",
    )
