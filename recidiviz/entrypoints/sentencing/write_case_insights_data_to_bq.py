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
import itertools
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# Ignore: Mypy: module is installed, but missing library stubs or py.typed marker  [import-untyped]
from statsmodels.stats.proportion import proportion_confint  # type: ignore

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.source_tables.sentencing_source_table_collection import (
    CASE_INSIGHTS_RATES_ADDRESS,
    CASE_INSIGHTS_RATES_SCHEMA,
)
from recidiviz.tools.analyst.cohort_methods import (
    gen_aggregated_cohort_event_df,
    gen_cohort_time_to_first_event,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

# TODO(#40901): Move these constant variables to a config

# Comma-delimited strings of supported states
SUPPORTED_STATES_STR = "'US_IX','US_ND'"
UNKNOWN_ATTRIBUTE = "UNKNOWN"

# Attributes to aggregate by at each level of rollup, starting with most fine-grained
ROLLUP_ATTRIBUTES = {
    "US_ND": [
        # Specific charge + LSI-R score bucket
        [
            "state_code",
            "cohort_group",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_description",
        ],
        # Specific charge
        ["state_code", "cohort_group", "most_severe_description", "gender"],
        # NCIC category + LSI-R score bucket
        [
            "state_code",
            "cohort_group",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_ncic_category_uniform",
        ],
        # NCIC category + gender
        ["state_code", "cohort_group", "most_severe_ncic_category_uniform", "gender"],
        # Combined offense category + gender
        ["state_code", "cohort_group", "combined_offense_category", "gender"],
        # Combined offense category
        ["state_code", "cohort_group", "combined_offense_category"],
        # Violent/non-violent
        ["state_code", "cohort_group", "any_is_violent"],
        # All offenses
        ["state_code", "cohort_group"],
    ],
    "US_IX": [
        # Specific charge + LSI-R score bucket
        [
            "state_code",
            "cohort_group",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_description",
        ],
        # Specific charge
        ["state_code", "cohort_group", "most_severe_description"],
        # NCIC category + LSI-R score bucket
        [
            "state_code",
            "cohort_group",
            "gender",
            "assessment_score_bucket_start",
            "assessment_score_bucket_end",
            "most_severe_ncic_category_uniform",
        ],
        # NCIC category
        ["state_code", "cohort_group", "most_severe_ncic_category_uniform"],
        # Combined offense category
        ["state_code", "cohort_group", "combined_offense_category"],
        # Violent/non-violent
        ["state_code", "cohort_group", "any_is_violent"],
        # All offenses
        ["state_code", "cohort_group"],
    ],
}

# NOTE: All keys in ATTRIBUTE_MAPPING must be in the first level of ROLLUP_ATTRIBUTES
ATTRIBUTE_MAPPING = {
    "most_severe_description": [
        "most_severe_ncic_category_uniform",
        "combined_offense_category",
        "any_is_violent",
    ]
}
RECIDIVISM_MONTHS = [0, 3, 6, 9, 12, 18, 24, 30, 36]
# If the CI size for any disposition is above this threshold, roll up to a higher level
ROLLUP_CI_THRESHOLDS = {"US_IX": 0.2, "US_ND": 0.3}
# Which disposition's CI to apply the threshold to (currently only "largest" and "2nd smallest" are supported)
ROLLUP_CRITERIA = {"US_IX": "largest", "US_ND": "2nd smallest"}
# After rollup, if any disposition has a CI above this threshold, don't include it; on the front end, we will indicate
# that there is insufficient data for that disposition and no recidivism rate will be shown for it
INCLUSION_CI_THRESHOLDS = {"US_IX": 0.2, "US_ND": 0.6}
RECIDIVISM_SERIES_COLUMN_REGEX = r"recidivism_(.*)_series"
DISPOSITION_PERCENTAGE_COLUMN_REGEX = r"disposition_(.*)_pc"


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
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        write_case_insights_data_to_bq(project_id=args.project_id)


def get_gendered_assessment_score_bucket_range(
    cohort_df_row: pd.Series,
) -> Tuple[int, int]:
    """Get the risk bucket range associated with the given gender and LSI-R assessment score.

    These buckets were defined in an Idaho-specific memo:
    https://drive.google.com/file/d/1o7vWvQ507SUvCPShyyOZFXQq53wT2D0J/view.

    This returns an assessment_score_bucket_start and assessment_score_bucket_end, which are inclusive endpoints of the
    bucket range. If the bucket has no upper limit (e.g. "29+") then assessment_score_bucket_end is -1. If the
    input assessment_score is invalid, both return values are -1. If the gender is not FEMALE or MALE, the return values
    are (0, -1), which is treated on the front end as matching all assessment scores.

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
    # For other genders (EXTERNAL_UNKNOWN, TRANS_MALE, TRANS_FEMALE), set the assessment score to be "all"
    return 0, -1


def adjust_any_is_sex_offense(cohort_df_row: pd.Series) -> bool:
    """Adjust the any_is_sex_offense value for a given row.

    There are some offenses that are not marked as is_sex_offense but should be.
    We override any_is_sex_offense to be true if most_severe_ncic_category_uniform is "Sexual Assault",
    "Sexual Assualt", or "Sex Offense", or if most_severe_description contains the word 'sexual'.
    Note the typo in "Sexual Assualt"; this is a known typo in our data for most_severe_ncic_category_uniform.
    """
    return (
        cohort_df_row.any_is_sex_offense
        or (
            not pd.isna(cohort_df_row.most_severe_ncic_category_uniform)
            and cohort_df_row.most_severe_ncic_category_uniform
            in ["Sexual Assault", "Sexual Assualt", "Sex Offense"]
        )
        or (
            not pd.isna(cohort_df_row.most_severe_description)
            and "SEXUAL" in cohort_df_row.most_severe_description.upper()
        )
    )


def adjust_ncic_category(cohort_df_row: pd.Series) -> str:
    """Adjust the most_severe_ncic_category_uniform value for a given row.

    There are some offenses that have incorrect NCIC categories. While we're not addressing this systematically, here
    we at least override the known cases of some sex offenses being miscategorized as Bribery. We also correct a known
    typo of "Sexual Assualt" -> "Sexual Assault".

    Specifically, offenses containing:
     * "SEXUAL ABUSE", "SEXUAL BATTERY", "SEXUAL CONTACT" are overridden to be "Sexual Assault"
     * "SEXUALLY EXPLOITATIVE MATERIAL" or "SEXUAL EXPLOITATION" are overridden to be "Commercial Sex"
    """
    if not pd.isna(cohort_df_row.most_severe_description) and (
        "SEXUAL ABUSE" in cohort_df_row.most_severe_description.upper()
        or "SEXUAL BATTERY" in cohort_df_row.most_severe_description.upper()
        or "SEXUAL CONTACT" in cohort_df_row.most_severe_description.upper()
    ):
        return "Sexual Assault"
    if not pd.isna(cohort_df_row.most_severe_description) and (
        "SEXUALLY EXPLOITATIVE MATERIAL"
        in cohort_df_row.most_severe_description.upper()
        or "SEXUAL EXPLOITATION" in cohort_df_row.most_severe_description.upper()
    ):
        return "Commercial Sex"
    if cohort_df_row.most_severe_ncic_category_uniform == "Sexual Assualt":
        return "Sexual Assault"
    return cohort_df_row.most_severe_ncic_category_uniform


def get_combined_offense_category(cohort_df_row: pd.Series) -> str:
    categories = []
    if cohort_df_row.any_is_violent:
        categories.append("Violent offense")
    if cohort_df_row.any_is_drug:
        categories.append("Drug offense")
    if cohort_df_row.any_is_sex_offense:
        categories.append("Sex offense")

    if categories:
        combined_str = ", ".join(categories)
    else:
        combined_str = "Non-drug, Non-violent, Non-sex offense"

    return combined_str


def add_offense_attributes(cohort_df: pd.DataFrame) -> pd.DataFrame:
    cohort_df[
        ["assessment_score_bucket_start", "assessment_score_bucket_end"]
    ] = cohort_df.apply(
        get_gendered_assessment_score_bucket_range, axis=1, result_type="expand"
    )

    cohort_df.any_is_violent = cohort_df.any_is_violent.fillna(False)
    cohort_df.any_is_drug = cohort_df.any_is_drug.fillna(False)
    cohort_df.any_is_sex_offense = cohort_df.any_is_sex_offense.fillna(False)
    cohort_df.any_is_sex_offense = cohort_df.apply(
        adjust_any_is_sex_offense,
        axis=1,
    )
    cohort_df.most_severe_ncic_category_uniform = cohort_df.apply(
        adjust_ncic_category,
        axis=1,
    )

    cohort_df["combined_offense_category"] = cohort_df.apply(
        get_combined_offense_category,
        axis=1,
    )

    return cohort_df


def get_cohort_df(project_id: str) -> pd.DataFrame:
    cohort_query = f"""
    SELECT *
    FROM `{project_id}.sentencing_views.sentence_cohort`
    WHERE state_code IN ({SUPPORTED_STATES_STR})
    """
    cohort_df = pd.read_gbq(cohort_query, project_id=project_id)

    return cohort_df


def get_recidivism_event_df(project_id: str) -> pd.DataFrame:
    """Get recidivism events, which are all INCARCERATION starts or PROBATION starts inflowing from LIBERTY."""

    event_query = f"""
    SELECT *
    FROM `{project_id}.sentencing_views.recidivism_event`
    WHERE state_code IN ({SUPPORTED_STATES_STR})
    """
    event_df = pd.read_gbq(event_query, project_id=project_id)

    return event_df


def get_charge_df(project_id: str) -> pd.DataFrame:
    charge_query = f"""
    SELECT *
    FROM `{project_id}.sentencing_views.sentencing_charge_record_materialized`
    WHERE state_code IN ({SUPPORTED_STATES_STR})
    """
    charge_df = pd.read_gbq(charge_query, project_id=project_id)

    return charge_df


def get_disposition_df(cohort_df: pd.DataFrame, state_code: str) -> pd.DataFrame:
    """
    Returns: DataFrame with columns 'state_code', 'gender', 'assessment_score_bucket_start',
        'assessment_score_bucket_end', 'most_severe_description', 'disposition_probation_pc', 'disposition_rider_pc',
        'disposition_term_pc'
    """
    raw_disposition_df = (
        cohort_df.groupby(ROLLUP_ATTRIBUTES[state_code][0])
        .size()
        .unstack(level="cohort_group")
        .rename_axis(columns=None)
        .fillna(0)
    )

    # Convert counts to percentages
    disposition_df = raw_disposition_df.div(raw_disposition_df.sum(axis=1), axis=0)
    # Rename columns
    disposition_df = disposition_df.rename(
        columns=lambda x: "disposition_" + x.lower() + "_pc"
    )
    disposition_df["disposition_num_records"] = raw_disposition_df.sum(axis=1).astype(
        int
    )

    return disposition_df.reset_index()


def add_placeholder_cohort_rows(cohort_df: pd.DataFrame) -> pd.DataFrame:
    """Add placeholder rows to the cohort DataFrame with each combination of violent/drug/sex offense attributes.

    Per #36670, this addresses the fact that not all possible roll-up values are necessarily represented in the case
    insights table by default. This adds a single row in the underlying data for each combination, which ensures that
    when the roll-ups are calculated, every combination will be represented.

    Note that the most_severe_description names have [PLACEHOLDER] in them, as this exact string is used in the
    front-end to filter these out so that they're not displayed as possible options for offenses.
    """
    state_codes = cohort_df["state_code"].unique()

    data = [
        state_codes,
        ["INTERNAL_UNKNOWN"],
        [-1],
        ["PROBATION"],
        [pd.to_datetime("1970-01-01").date()],
        ["[PLACEHOLDER] NCIC CATEGORY"],
        [True, False],
        [True, False],
        [True, False],
    ]
    cols = [
        "state_code",
        "gender",
        "assessment_score",
        "cohort_group",
        "cohort_start_date",
        "most_severe_ncic_category_uniform",
        "any_is_violent",
        "any_is_drug",
        "any_is_sex_offense",
    ]
    placeholder_rows = pd.DataFrame(columns=cols, data=itertools.product(*data))
    # person_id must be unique per row, and it's set to be negative numbers starting with -1
    placeholder_rows["person_id"] = range(-1, -1 * (placeholder_rows.shape[0] + 1), -1)
    placeholder_rows["most_severe_description"] = placeholder_rows.apply(
        lambda row: f'[PLACEHOLDER] {row["any_is_violent"]}, {row["any_is_drug"]}, {row["any_is_sex_offense"]}',
        axis=1,
    )
    return pd.concat([cohort_df, placeholder_rows]).reset_index(drop=True)


def add_missing_offenses(
    cohort_df: pd.DataFrame, charge_df: pd.DataFrame
) -> pd.DataFrame:
    """Add rows to the cohort DataFrame for offenses that aren't in cohort_df.

    There are offenses that exist in state_charge that aren't in cohort_df for various reasons (e.g. they were never the
    most severe offense or weren't part of an initial sentence). This adds a single row in the underlying data for each
    such offense so that the eventual case insights table will have them. The rows added have gender INTERNAL_UNKNOWN
    and assessment_score -1 so that they don't actually correspond to any real case or insight.
    """
    missing_offenses_df = charge_df[
        ~charge_df.charge.isin(cohort_df.most_severe_description.unique())
    ].copy()

    cols = [
        "state_code",
        "person_id",
        "gender",
        "assessment_score",
        "cohort_group",
        "cohort_start_date",
        "most_severe_description",
        # TODO(#41614): consolidate where these attributes are pulled from
        "most_severe_ncic_category_uniform",
        "any_is_violent",
        "any_is_drug",
        "any_is_sex_offense",
    ]
    missing_offenses_df.loc[:, "gender"] = "INTERNAL_UNKNOWN"
    missing_offenses_df.loc[:, "assessment_score"] = -1
    missing_offenses_df.loc[:, "cohort_group"] = "PROBATION"
    missing_offenses_df.loc[:, "cohort_start_date"] = pd.to_datetime(
        "1970-01-01"
    ).date()
    column_remapping = {
        "charge": "most_severe_description",
        "ncic_category_uniform": "most_severe_ncic_category_uniform",
        "is_violent": "any_is_violent",
        "is_drug": "any_is_drug",
        "is_sex_offense": "any_is_sex_offense",
    }
    missing_offenses_df = missing_offenses_df.rename(columns=column_remapping)

    # person_id must be unique per row, and it's set to be negative numbers starting with -10001
    missing_offenses_df["person_id"] = range(
        -10001, -1 * (missing_offenses_df.shape[0] + 10001), -1
    )

    return pd.concat([cohort_df, missing_offenses_df[cols]]).reset_index(drop=True)


def add_all_combinations(disposition_df: pd.DataFrame, state_code: str) -> pd.DataFrame:
    """Add rows for every possible combination of key columns.

    Key columns are 'state_code', 'gender', 'assessment_score_bucket_start', 'assessment_score_bucket_end',
    'most_severe_description'.
    get_disposition_df returns a DataFrame with a row for every combination of key columns observed in the data. This
    method takes that disposition_df as input and adds a row for every possible combination of values in those key
    columns, with None in the remaining columns for those rows not present in the input.

    Note that 'gender', 'assessment_score_bucket_start' and 'assessment_score_bucket_end' are considered together for
    the purposes of determining possible combinations. For example, if the input only has (MALE, 0, 22) and
    (FEMALE, 21, 30) as values for (gender, assessment_score_bucket_start, assessment_score_bucket_end), then those will
    be the only values present in the output; this method won't also add combinations such as (MALE, 21, 30),
    (MALE, 0, 30), (FEMALE, 0, 22), etc.
    """

    def flatten_row(row: Tuple[Any]) -> list[Any]:
        """Flatten e.g. ('DUI DRIVING', ('FEMALE', 0, 22)] into ['DUI DRIVING', 'FEMALE', 0, 22]"""
        return [
            item
            for sublist in row
            for item in ([sublist] if isinstance(sublist, str) else sublist)
        ]

    original_cols = disposition_df.columns
    risk_bucket_cols = [
        "gender",
        "assessment_score_bucket_start",
        "assessment_score_bucket_end",
    ]
    disposition_cols = [col for col in original_cols if col.startswith("disposition")]
    other_attribute_cols = list(
        set(original_cols)
        .difference(set(risk_bucket_cols))
        .difference(set(disposition_cols))
        .difference({"cohort_group", "state_code"})
    )

    attribute_cols = ["state_code"] + other_attribute_cols + risk_bucket_cols

    # Get the unique values for each column except for state_code
    unique_vals = [disposition_df[col].unique() for col in other_attribute_cols]
    # Get the unique combinations of values for the risk bucket columns
    unique_vals.append(list(disposition_df.groupby(risk_bucket_cols).size().index))
    # Create a row for each combination of unique values
    rows = list(itertools.product(*unique_vals))

    # Create a DataFrame with all combinations, adding back in the state_code, with 0's in the disposition columns
    data = [[state_code] + flatten_row(row) for row in rows]
    all_combinations_df = pd.DataFrame(data=data, columns=attribute_cols)
    all_combinations_df.set_index(attribute_cols, inplace=True)

    # Initialize disposition columns with correct dtypes matching source dataframe (Pandas 2.0 compatibility)
    state_disposition_df_indexed = disposition_df.set_index(attribute_cols)
    for col in disposition_cols:
        # Use same dtype as source to avoid Pandas 2.0 incompatible dtype assignment warnings
        if col in state_disposition_df_indexed.columns:
            all_combinations_df[col] = pd.Series(
                0,
                index=all_combinations_df.index,
                dtype=state_disposition_df_indexed[col].dtype,
            )
        else:
            all_combinations_df[col] = 0

    # Insert the disposition_df values into the DataFrame with all combinations
    all_combinations_df.loc[
        state_disposition_df_indexed.index, disposition_cols
    ] = state_disposition_df_indexed[disposition_cols].values

    # Set the index back to columns and make sure the original columns order is restored
    all_combinations_df.reset_index(inplace=True)

    all_combinations_df = all_combinations_df[original_cols]

    return all_combinations_df


def add_attributes_to_index(
    target_df: pd.DataFrame, reference_df: pd.DataFrame, attribute_mapping: dict
) -> pd.DataFrame:
    """Add attributes to the index of target_df by looking them up in reference_df.

    attribute_mapping is a mapping from attributes present in target_df to attributes present in reference_df that
    should be added to target_df. For example, the specific offense may be present in a rolled-up DataFrame, but the
    corresponding NCIC category is only available in the full recidivism Dataframe; that category attribute can be added
    to the rolled-up DataFrame with this method.

    The columns of target_df are assumed to have three levels.
    The columns of reference_df should contain the attributes in both the index of target_df and ROLLUP_ATTRIBUTES.
    The new attributes to add should be unique in reference_df based on existing index attributes (e.g.
    most_severe_ncic_category_uniform is unique given most_severe_description).
    """
    for from_attribute in attribute_mapping:
        if from_attribute not in target_df.index.names:
            raise ValueError(
                f"{from_attribute} not found: All keys in ATTRIBUTE_MAPPING must be in the first level of ROLLUP_ATTRIBUTES."
            )
        for to_attribute in attribute_mapping[from_attribute]:
            # Find the most frequent value of to_attribute in reference_df for from_attribute
            attr_to_add_df = (
                reference_df.groupby(from_attribute)[to_attribute]
                .agg(
                    # If there is more than one mode, arbitrarily return the first one
                    lambda x: pd.Series.mode(x).iloc[0]
                )
                .to_frame()
            )
            attr_to_add_df.columns = pd.MultiIndex.from_product(
                [attr_to_add_df.columns, [""], [""]]
            )

            original_attrs = list(target_df.index.names)
            original_cols = target_df.columns

            target_df = (
                target_df.merge(attr_to_add_df, left_index=True, right_index=True)
                .reset_index()
                .set_index(original_attrs + [to_attribute])
            )

            target_df.columns = original_cols

    return target_df


# TODO(#31104): Move CI calculation to a centralized util method
def add_cis(df: pd.DataFrame) -> pd.DataFrame:
    def calculate_ci(
        event_rate: float, cohort_size: int, alpha: float = 0.05, method: str = "beta"
    ) -> Tuple[float, float]:
        return proportion_confint(
            int(event_rate * cohort_size), cohort_size, alpha=alpha, method=method
        )

    df[["lower_ci", "upper_ci"]] = df.apply(
        lambda row: calculate_ci(row.event_rate, row.cohort_size),
        axis=1,
        result_type="expand",
    )
    df["ci_size"] = df["upper_ci"] - df["lower_ci"]
    return df


def add_recidivism_rate_dicts(df: pd.DataFrame) -> pd.DataFrame:
    """Add an event_rate_dict column which is a dict of column values related to the event rate."""
    # Temporarily move cohort_months from index to column
    index = df.index
    df = df.reset_index(["cohort_months"])
    df["event_rate_dict"] = (
        df[
            [
                "cohort_months",
                "event_rate",
                "lower_ci",
                "upper_ci",
                "ci_size",
                "cohort_size",
            ]
        ]
        .astype("O")
        .apply(lambda row: row.to_dict(), axis=1)
    )
    return df.reset_index().set_index(index.names)


def extract_rate_dicts_info(
    event_rate_dicts_df: pd.DataFrame, col_name: str
) -> Dict[str, str]:
    """Select the specified column, return the concatenated string and the last element's CI size and cohort size."""
    concatenated_str = json.dumps(list(event_rate_dicts_df[col_name]))
    cohort_size = event_rate_dicts_df[col_name].iloc[-1]["cohort_size"]
    final_ci_size = event_rate_dicts_df[col_name].iloc[-1]["ci_size"]
    return {
        "event_rate_dict": concatenated_str,
        "cohort_size": cohort_size,
        "final_ci_size": final_ci_size,
    }


def concatenate_recidivism_series(
    aggregated_df: pd.DataFrame, cohort_attribute_col: List[str]
) -> pd.DataFrame:
    """Get the concatenated string and final CI size for each grouping across months, for each cohort_group.

    For each group of cohort_attribute_col, produce the concatenated event_rate_dict for all cohort_months (as a string
    of a JSON array), the CI size of the final month, and the cohort size.

    The returned DataFrame is indexed by the columns of cohort_attribute_col except for 'cohort_group', and has the
    following three columns for each cohort_group:
      (event_rate_dict, cohort_group)
      (cohort_size, cohort_group)
      (final_ci_size, cohort_group)
    """

    grouped_df = aggregated_df.groupby(
        cohort_attribute_col,
        group_keys=True,
    ).apply(extract_rate_dicts_info, col_name="event_rate_dict")

    unstacked_df = pd.DataFrame(grouped_df.to_list(), index=grouped_df.index).unstack(
        level="cohort_group"
    )
    return unstacked_df


def get_recidivism_series_df_for_rollup_level(
    recidivism_df: pd.DataFrame,
    time_index: Iterable[int],
    cohort_attribute_col: List[str],
) -> pd.DataFrame:
    """Get the recidivism series DataFrame for a given rollup level."""
    aggregated_df = gen_aggregated_cohort_event_df(
        recidivism_df,
        cohort_date_field="cohort_start_date",
        event_date_field="recidivism_date",
        time_index=time_index,
        time_unit="months",
        cohort_attribute_col=cohort_attribute_col,
        last_day_of_data=datetime.datetime.now(),
        full_observability=True,
    )

    aggregated_df = add_cis(df=aggregated_df)
    aggregated_df = add_recidivism_rate_dicts(df=aggregated_df)

    concatenated_recidivism_series_df = concatenate_recidivism_series(
        aggregated_df, cohort_attribute_col
    )

    return concatenated_recidivism_series_df


def get_all_rollup_aggregated_df(
    recidivism_df: pd.DataFrame,
    time_index: Iterable[int],
    index_df: pd.DataFrame,
    state_code: str,
) -> pd.DataFrame:
    """Create a combined DataFrame that has columns for calculations at each rollup level.

    This calls gen_aggregated_cohort_event_df for each rollup level and then adds the rollup level information to the
    columns. The columns have three levels: metric, cohort_group, and rollup_level. The columns will thus be:
    (event_rate_dict, PROBATION, 0), ..., (final_ci_size, TERM, 0),
    (event_rate_dict, PROBATION, 1), ..., (final_ci_size, TERM, 1),
    ...

    This allows for a subsequent call to extract_rollup_columns, which extracts the columns from the appropriate level.

    The input index_df is used to seed the index of the output dataframe. This is to ensure that during rollup we don't
    drop any rows that would be dropped because there is no data at a given rollup level.
    """
    index_attributes = set(ROLLUP_ATTRIBUTES[state_code][0]).difference(
        {"cohort_group"}
    )
    index = index_df.set_index(sorted(list(index_attributes))).index
    # Pandas 2.0: Explicitly specify dtypes for empty MultiIndex levels to avoid object dtype inference
    all_rollup_levels_df = pd.DataFrame(
        index=index,
        columns=pd.MultiIndex(
            levels=[
                pd.Index([], dtype="object"),  # metric
                pd.Index([], dtype="object"),  # cohort_group
                pd.Index([], dtype="int64"),  # rollup_level - ensure int64 from start
            ],
            codes=[[], [], []],
            names=["metric", "cohort_group", "rollup_level"],
        ),
    )
    for idx, cohort_attribute_col in enumerate(ROLLUP_ATTRIBUTES[state_code]):
        recidivism_series_df = get_recidivism_series_df_for_rollup_level(
            recidivism_df, time_index, cohort_attribute_col
        )

        # Add the current rollup level to each column
        # Pandas 2.0: Create rollup_level as Index with int64 dtype explicitly
        rollup_level_index = pd.Index([idx], dtype="int64")
        recidivism_series_df.columns = pd.MultiIndex.from_product(
            recidivism_series_df.columns.levels + [rollup_level_index],
            names=["metric", "cohort_group", "rollup_level"],
        )

        if idx == 0:
            all_rollup_levels_df = all_rollup_levels_df.merge(
                recidivism_series_df,
                left_index=True,
                right_index=True,
                how="left",
            )
            all_rollup_levels_df = add_attributes_to_index(
                target_df=all_rollup_levels_df,
                reference_df=recidivism_df,
                attribute_mapping=ATTRIBUTE_MAPPING,
            )
        else:
            all_rollup_levels_df = all_rollup_levels_df.merge(
                recidivism_series_df, left_index=True, right_index=True, how="left"
            )

    # Pandas 2.0: Ensure rollup_level in column MultiIndex is int64
    if "rollup_level" in all_rollup_levels_df.columns.names:
        level_idx = all_rollup_levels_df.columns.names.index("rollup_level")
        new_levels = list(all_rollup_levels_df.columns.levels)
        new_levels[level_idx] = all_rollup_levels_df.columns.levels[level_idx].astype(
            "int64"
        )
        all_rollup_levels_df.columns = all_rollup_levels_df.columns.set_levels(
            new_levels[level_idx], level=level_idx
        )

    return all_rollup_levels_df


def extract_rollup_columns(
    all_rollup_levels_df: pd.DataFrame, state_code: str
) -> pd.DataFrame:
    """Determine which rollup level is appropriate for each row and select the corresponding columns.

    A rollup level qualifies as appropriate if the CI size corresponding to ROLLUP_CRITERIA is under
    ROLLUP_CI_THRESHOLDS for the given state. The following two criteria are currently supported:
    - "largest": requires that the largest CI across all dispositions is under the CI threshold
    - "2nd smallest": requires that 2nd smallest CI across all dispositions is under the CI threshold

    Following get_all_rollup_aggregated_df, if the appropriate rollup level is determined to be 2, for example, then
    (event_rate_dict, PROBATION, 2), etc. will be copied to recidivism_probation_series, etc.

    The recidivism_rollup column will be populated with a JSON string of all columns and their values for the
    corresponding rollup level.

    The recidivism_num_records column will be the sum of cohort_size across dispositions for the corresponding rollup
    level.
    """

    def get_recidivism_rollup(row: pd.Series) -> str:
        cols_in_rollup = set(
            ROLLUP_ATTRIBUTES[state_code][row.rollup_level]
        ).difference({"cohort_group"})
        recidivism_rollup_dict = {col: row[col] for col in cols_in_rollup}

        return json.dumps(recidivism_rollup_dict, default=int)

    def get_nsmallest(row: pd.Series, n: int = 2) -> float:
        return row.nsmallest(n).max()

    # Get the appropriate rollup level for each row
    if ROLLUP_CRITERIA[state_code] == "largest":
        exceeds_ci_threshold = (
            all_rollup_levels_df.stack(
                ["rollup_level", "cohort_group"], future_stack=True
            )["final_ci_size"]
            .unstack("cohort_group")
            .max(1)
            > ROLLUP_CI_THRESHOLDS[state_code]
        )
    elif ROLLUP_CRITERIA[state_code] == "2nd smallest":
        exceeds_ci_threshold = (
            all_rollup_levels_df.stack(
                ["rollup_level", "cohort_group"], future_stack=True
            )["final_ci_size"]
            .unstack("cohort_group")
            .apply(lambda row: get_nsmallest(row, 2), axis=1)
            > ROLLUP_CI_THRESHOLDS[state_code]
        )
    else:
        raise ValueError(
            f"Unsupported ROLLUP_CRITERIA value {ROLLUP_CRITERIA[state_code]}"
        )

    roll_up_level = (
        exceeds_ci_threshold.unstack("rollup_level").fillna(True).idxmin(axis=1)
    )

    # Keep only the appropriate rollup level for each row
    all_rollup_levels_df = all_rollup_levels_df.stack(
        level="rollup_level", future_stack=True
    )
    all_rollup_levels_df = all_rollup_levels_df[
        all_rollup_levels_df.index.get_level_values("rollup_level")
        == roll_up_level.reindex(all_rollup_levels_df.index)
    ]
    recidivism_num_records = all_rollup_levels_df["cohort_size"].sum(axis=1)
    all_rollup_levels_df.columns = all_rollup_levels_df.columns.to_flat_index()
    all_rollup_levels_df["recidivism_num_records"] = recidivism_num_records
    columns_to_rename = filter(
        lambda col: (
            isinstance(col, tuple)
            and len(col) == 2
            and col[0] == "event_rate_dict"
            and isinstance(col[1], str)
        ),
        all_rollup_levels_df.columns,
    )

    column_remapping = {
        col: f"recidivism_{col[1].lower()}_series" for col in columns_to_rename
    }

    all_rollup_levels_df = all_rollup_levels_df.rename(columns=column_remapping)
    all_rollup_levels_df = all_rollup_levels_df.reset_index()
    # Pandas 2.0 with future_stack=True may infer object dtype for rollup_level, so explicitly cast to int
    if "rollup_level" in all_rollup_levels_df.columns:
        all_rollup_levels_df["rollup_level"] = all_rollup_levels_df[
            "rollup_level"
        ].astype(int)
    all_rollup_levels_df["recidivism_rollup"] = all_rollup_levels_df.apply(
        get_recidivism_rollup, axis=1
    )

    return all_rollup_levels_df


def add_combined_attributes_to_rollup(
    rolled_up_recidivism_df: pd.DataFrame,
) -> pd.DataFrame:
    def add_combined_attributes(recidivism_rollup: str) -> str:
        recidivism_rollup_dict = json.loads(recidivism_rollup)

        if "combined_offense_category" in recidivism_rollup_dict:
            combined_offense = recidivism_rollup_dict["combined_offense_category"]
            recidivism_rollup_dict["rollupCombinedOffenseCategory"] = {
                "isViolent": "Violent offense" in combined_offense,
                "isDrug": "Drug offense" in combined_offense,
                "isSex": "Sex offense" in combined_offense,
            }
        else:
            recidivism_rollup_dict["rollupCombinedOffenseCategory"] = None

        recidivism_rollup_with_added_attributes = json.dumps(recidivism_rollup_dict)

        return recidivism_rollup_with_added_attributes

    rolled_up_recidivism_df["recidivism_rollup"] = rolled_up_recidivism_df[
        "recidivism_rollup"
    ].apply(add_combined_attributes)

    return rolled_up_recidivism_df


def create_final_table(
    rolled_up_recidivism_df: pd.DataFrame, disposition_df: pd.DataFrame, state_code: str
) -> pd.DataFrame:
    """Create final table that merges rolled_up_recidivism_df and disposition_df."""
    attrs_to_join = set(ROLLUP_ATTRIBUTES[state_code][0])
    attrs_to_join.remove("cohort_group")
    final_table = rolled_up_recidivism_df.merge(disposition_df, on=list(attrs_to_join))

    return final_table


def extract_cohort_attributes(cohort_column_name: str, columns_regex: str) -> Dict:
    """Extract the sentence type and length from the recidivism series column name.

    Columns are named recidivism_<cohort_name>_series (e.g. recidivism_INCARCERATION|1-2 years_series), and cohort_name
    is a pipe-delimited string that has the sentence type and optionally the sentence length.
    """

    def extract_sentence_length(length_str: str) -> Tuple[int, Any]:
        sentence_length_match = re.match(sentence_length_regex, length_str)
        if not sentence_length_match:
            raise ValueError(f"Invalid sentence length for cohort_group {length_str}")
        try:
            bucket_start = int(sentence_length_match.group(1))
            if sentence_length_match.group(2) == "?":
                bucket_end = -1
            else:
                bucket_end = int(sentence_length_match.group(2))
        except (AttributeError, ValueError) as parse_error:
            raise ValueError(
                f"Invalid sentence length for cohort_group {length_str}"
            ) from parse_error

        return bucket_start, bucket_end

    sentence_length_regex = r"(\d+)-(.+) years"

    cohort_match = re.match(columns_regex, cohort_column_name)
    if not cohort_match:
        raise ValueError(f"Invalid column name {cohort_column_name}")
    cohort_str = cohort_match.group(1)
    type_and_length = cohort_str.split("|")

    cohort_attributes = {}
    if len(type_and_length) == 1:
        # If there is no sentence length, only set the sentence type
        cohort_attributes["sentence_type"] = type_and_length[0].capitalize()
    elif len(type_and_length) == 2:
        cohort_attributes["sentence_type"] = type_and_length[0].capitalize()
        (
            cohort_attributes["sentence_length_bucket_start"],
            cohort_attributes["sentence_length_bucket_end"],
        ) = extract_sentence_length(type_and_length[1])
    else:
        raise ValueError(f"Invalid cohort_group {cohort_str}")

    return cohort_attributes


def consolidate_cohort_columns(
    final_table: pd.DataFrame, state_code: str, columns_regex: str, column_name: str
) -> pd.Series:
    """Consolidate the various columns corresponding to different cohorts into a single column.

    This is used to consolidate both the recidivism series columns into a single recidivism_series column, and the
    disposition percentage columns into a single disposition_percentages column.

    The consolidated column value is a JSON string with the cohort info of each column along with the value from that
    column, e.g.:
        "[{'sentence_type': 'probation',
           'percentage': 0.6},
          {'sentence_type': 'incarceration',
           'percentage': 0.4},
         ]"
    """
    columns_to_consolidate = list(
        filter(
            lambda col: (isinstance(col, str) and re.match(columns_regex, col)),
            final_table.columns,
        )
    )

    def create_consolidated_dict(row: pd.Series, col: str) -> Optional[Dict]:
        cohort_attributes = extract_cohort_attributes(col, columns_regex)

        # If the column value is a JSON string, first convert it to a dict
        if isinstance(row[col], str):
            column_val = json.loads(row[col])
        else:
            column_val = row[col]

        # If a particular disposition is above the inclusion threshold, return None so that it's not included
        if (
            column_name == "data_points"
            and column_val[-1]["ci_size"] > INCLUSION_CI_THRESHOLDS[state_code]
        ):
            return None

        # If the sentence type is "Other_sentence_type", return None so that it's not included
        if cohort_attributes["sentence_type"] == "Other_sentence_type":
            return None

        return cohort_attributes | {column_name: column_val}

    def consolidate_cohorts(row: pd.Series) -> str:
        consolidated_series = [
            create_consolidated_dict(row, col)
            for col in columns_to_consolidate
            # Exclude any empty or NaN columns
            if row[col] is not None and not pd.isna(row[col])
        ]
        consolidated_series_json = json.dumps(
            [
                consolidated_dict
                for consolidated_dict in consolidated_series
                if consolidated_dict is not None
            ]
        )
        return consolidated_series_json

    return final_table.apply(consolidate_cohorts, axis=1)


def add_consolidated_columns(
    final_table: pd.DataFrame, state_code: str
) -> pd.DataFrame:
    final_table["recidivism_series"] = consolidate_cohort_columns(
        final_table, state_code, RECIDIVISM_SERIES_COLUMN_REGEX, "data_points"
    )
    final_table["dispositions"] = consolidate_cohort_columns(
        final_table, state_code, DISPOSITION_PERCENTAGE_COLUMN_REGEX, "percentage"
    )
    return final_table


def create_table_for_state(
    state_code: str,
    cohort_df: pd.DataFrame,
    event_df: pd.DataFrame,
    charge_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create the case insights table for a given state."""

    cohort_df_for_state = cohort_df[cohort_df.state_code == state_code]
    event_df_for_state = event_df[event_df.state_code == state_code]
    charge_df_for_state = charge_df[charge_df.state_code == state_code]

    # Trim out any cohort start dates in the future, which can apparently happen
    # TODO(#41003): Filter these out in sentence_cohort instead
    cohort_df_for_state = cohort_df_for_state[
        cohort_df_for_state.cohort_start_date <= datetime.date.today()
    ]
    cohort_df_for_state = add_missing_offenses(cohort_df_for_state, charge_df_for_state)
    cohort_df_for_state = add_offense_attributes(cohort_df_for_state)

    disposition_df_for_state = get_disposition_df(cohort_df_for_state, state_code)
    disposition_df_for_state = add_all_combinations(
        disposition_df_for_state, state_code
    )

    recidivism_df_for_state = gen_cohort_time_to_first_event(
        cohort_df=cohort_df_for_state,
        event_df=event_df_for_state,
        cohort_date_field="cohort_start_date",
        event_date_field="recidivism_date",
        join_field=["person_id", "state_code"],
    )
    # Replace None with INTERNAL_UNKNOWN so that these rows aren't dropped by any groupby
    recidivism_df_for_state.most_severe_ncic_category_uniform = (
        recidivism_df_for_state.most_severe_ncic_category_uniform.fillna(
            "INTERNAL_UNKNOWN"
        )
    )
    # TODO(#34529): Remove drop_duplicates here once it's properly handled in gen_cohort_time_to_first_event
    recidivism_df_for_state = recidivism_df_for_state.drop_duplicates(
        ["state_code", "person_id", "cohort_start_date"]
    )

    all_rollup_levels_df = get_all_rollup_aggregated_df(
        recidivism_df_for_state, RECIDIVISM_MONTHS, disposition_df_for_state, state_code
    )
    rolled_up_recidivism_df = extract_rollup_columns(all_rollup_levels_df, state_code)
    rolled_up_recidivism_df = add_combined_attributes_to_rollup(rolled_up_recidivism_df)

    final_table_for_state = create_final_table(
        rolled_up_recidivism_df, disposition_df_for_state, state_code
    )
    final_table_for_state = add_consolidated_columns(final_table_for_state, state_code)

    return final_table_for_state, recidivism_df_for_state, disposition_df_for_state


def write_case_insights_data_to_bq(project_id: str) -> None:
    """Write case insights data to BigQuery.

    The final table has a row for every state_code, gender, assessment_score_bucket_start, assessment_score_bucket_end,
    most_severe_description with:
    * The disposition percentages of each of PROBATION, RIDER, TERM (to be deprecated) TODO(#40788)
    * A `dispositions` column with disposition percentages for all sentence types and lengths
    * The cohort size for this row (disposition_num_records)
    * The series of recidivism stats over all months for each of PROBATION, RIDER, TERM for the appropriate rollup level
        (to be deprecated) TODO(#40788)
    * A `recidivism_series` column with recidivism rates for all sentence types and lengths
    * The cohort size of the corresponding rollup level (recidivism_num_records)
    * The comma-separated values of the rollup level (recidivism_rollup)
    """
    cohort_df = get_cohort_df(project_id)
    cohort_df = add_placeholder_cohort_rows(cohort_df)
    event_df = get_recidivism_event_df(project_id)
    charge_df = get_charge_df(project_id)

    final_table = None
    recidivism_df = None
    disposition_df = None
    for state_code in cohort_df["state_code"].unique():
        (
            final_table_for_state,
            recidivism_df_for_state,
            disposition_df_for_state,
        ) = create_table_for_state(state_code, cohort_df, event_df, charge_df)
        if final_table is None:
            final_table = final_table_for_state
            recidivism_df = recidivism_df_for_state
            disposition_df = disposition_df_for_state
        else:
            final_table = pd.concat([final_table, final_table_for_state])
            recidivism_df = pd.concat([recidivism_df, recidivism_df_for_state])
            disposition_df = pd.concat([disposition_df, disposition_df_for_state])

    if final_table is None:
        return

    # TODO(#40788): Temporary stop-gap until we deprecate these columns
    if "disposition_probation_pc" in final_table.columns:
        final_table.disposition_probation_pc = (
            final_table.disposition_probation_pc.fillna(0.0)
        )
    if "disposition_rider_pc" in final_table.columns:
        final_table.disposition_rider_pc = final_table.disposition_rider_pc.fillna(0.0)
    if "disposition_term_pc" in final_table.columns:
        final_table.disposition_term_pc = final_table.disposition_term_pc.fillna(0.0)

    schema_cols = list(map(lambda x: x["name"], CASE_INSIGHTS_RATES_SCHEMA))
    final_table = final_table[schema_cols]
    final_table.to_gbq(
        destination_table=CASE_INSIGHTS_RATES_ADDRESS.to_str(),
        project_id=project_id,
        table_schema=CASE_INSIGHTS_RATES_SCHEMA,
        if_exists="replace",
    )
