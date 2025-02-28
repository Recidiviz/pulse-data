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
from typing import Dict, Iterable, List, Tuple

import pandas as pd

# Ignore: Mypy: module is installed, but missing library stubs or py.typed marker  [import-untyped]
from statsmodels.stats.proportion import proportion_confint  # type: ignore

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.sentencing.datasets import (
    CASE_INSIGHTS_RATES_ADDRESS,
    CASE_INSIGHTS_RATES_SCHEMA,
)
from recidiviz.tools.analyst.cohort_methods import (
    gen_aggregated_cohort_event_df,
    gen_cohort_time_to_first_event,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

# Comma-delimited strings of supported states
SUPPORTED_STATES_STR = "'US_IX'"
UNKNOWN_ATTRIBUTE = "UNKNOWN"

# Attributes to aggregate by at each level of rollup, starting with most fine-grained
ROLLUP_ATTRIBUTES = [
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
    ["state_code", "cohort_group", "any_is_violent_uniform"],
    # All offenses
    ["state_code", "cohort_group"],
]
RECIDIVISM_MONTHS = [0, 3, 6, 9, 12, 18, 24, 30, 36]
# If the CI size for any disposition is above this threshold, roll up to a higher level
ROLLUP_CI_THRESHOLD = 0.2


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
    """Adjust the adjust_ncic_category value for a given row.

    There are some offenses that have incorrect NCIC categories. While we're not addressing this systematically, here
    we at least override the known cases of some sex offenses being miscategorized as Bribery.

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
    return cohort_df_row.most_severe_ncic_category_uniform


def get_combined_offense_category(cohort_df_row: pd.Series) -> str:
    categories = []
    if cohort_df_row.any_is_violent_uniform:
        categories.append("Violent offense")
    if cohort_df_row.any_is_drug_uniform:
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

    cohort_df.any_is_violent_uniform = cohort_df.any_is_violent_uniform.fillna(False)
    cohort_df.any_is_drug_uniform = cohort_df.any_is_violent_uniform.fillna(False)
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
    raw_disposition_df = (
        cohort_df.groupby(ROLLUP_ATTRIBUTES[0])
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


def add_attributes_to_index(
    target_df: pd.DataFrame, reference_df: pd.DataFrame
) -> pd.DataFrame:
    """Add the attributes in ROLLUP_ATTRIBUTES to the index of target_df by looking them up in reference_df.

    The columns of target_df are assumed to have three levels.
    The columns of reference_df should contain the attributes in both the index of target_df and ROLLUP_ATTRIBUTES.

    The new attributes to add should be unique in reference_df based on existing index attributes (e.g.
    most_severe_ncic_category_uniform is unique given most_severe_description).
    """
    all_attrs = {
        attr for attrs_for_level in ROLLUP_ATTRIBUTES for attr in attrs_for_level
    }
    attrs_to_add = all_attrs.difference(target_df.index.names)
    attrs_to_add.remove("cohort_group")
    original_attrs = list(target_df.index.names)
    original_cols = target_df.columns
    new_attrs_df = reference_df.groupby(original_attrs)[list(attrs_to_add)].agg(
        lambda x: x.iloc[0]
    )
    new_attrs_df.columns = pd.MultiIndex.from_product(
        [new_attrs_df.columns, [""], [""]]
    )

    merged_df = (
        target_df.merge(new_attrs_df, left_index=True, right_index=True)
        .reset_index()
        .set_index(original_attrs + list(attrs_to_add))
    )
    merged_df.columns = original_cols
    return merged_df


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
    of a JSON array, the CI size of the final month, and the cohort size.

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
    recidivism_df: pd.DataFrame, time_index: Iterable[int], index_df: pd.DataFrame
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
    index_attributes = set(ROLLUP_ATTRIBUTES[0]).difference({"cohort_group"})
    index = index_df.set_index(sorted(list(index_attributes))).index
    all_rollup_levels_df = pd.DataFrame(
        index=index,
        columns=pd.MultiIndex(
            levels=[[], [], []],
            codes=[[], [], []],
            names=["metric", "cohort_group", "rollup_level"],
        ),
    )
    for idx, cohort_attribute_col in enumerate(ROLLUP_ATTRIBUTES):
        recidivism_series_df = get_recidivism_series_df_for_rollup_level(
            recidivism_df, time_index, cohort_attribute_col
        )

        # Add the current rollup level to each column
        recidivism_series_df.columns = pd.MultiIndex.from_product(
            recidivism_series_df.columns.levels + [[idx]],
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
                target_df=all_rollup_levels_df, reference_df=recidivism_df
            )
        else:
            all_rollup_levels_df = all_rollup_levels_df.merge(
                recidivism_series_df, left_index=True, right_index=True, how="left"
            )

    return all_rollup_levels_df


def extract_rollup_columns(all_rollup_levels_df: pd.DataFrame) -> pd.DataFrame:
    """Determine which rollup level is appropriate for each row and select the corresponding columns.

    A rollup level qualifies as appropriate if the CI sizes for all three dispositions are under ROLLUP_CI_THRESHOLD.

    Following get_all_rollup_aggregated_df, if the appropriate rollup level is determined to be 2, for example, then
    (event_rate_dict, PROBATION, 2), etc. will be copied to recidivism_probation_series, etc.

    The recidivism_rollup column will be populated with a JSON string of all columns and their values for the
    corresponding rollup level.

    The recidivism_num_records column will be the sum of cohort_size across PROBATION, RIDER, TERM for the corresponding
    rollup level.
    """

    def get_recidivism_rollup(row: pd.Series) -> str:
        cols_in_rollup = set(ROLLUP_ATTRIBUTES[row.rollup_level]).difference(
            {"cohort_group"}
        )
        recidivism_rollup = json.dumps(
            {col: row[col] for col in cols_in_rollup}, default=int
        )
        return recidivism_rollup

    # Get the appropriate rollup level for each row
    exceeds_ci_threshold = (
        all_rollup_levels_df.stack(["rollup_level", "cohort_group"])["final_ci_size"]
        .unstack("cohort_group")
        .max(1)
        > ROLLUP_CI_THRESHOLD
    )
    roll_up_level = (
        exceeds_ci_threshold.unstack("rollup_level").fillna(True).idxmin(axis=1)
    )

    # Keep only the appropriate rollup level for each row
    all_rollup_levels_df = all_rollup_levels_df.stack(level="rollup_level")
    all_rollup_levels_df = all_rollup_levels_df[
        all_rollup_levels_df.index.get_level_values("rollup_level")
        == roll_up_level.reindex(all_rollup_levels_df.index)
    ]
    recidivism_num_records = all_rollup_levels_df["cohort_size"].sum(axis=1)
    all_rollup_levels_df.columns = all_rollup_levels_df.columns.to_flat_index()
    all_rollup_levels_df["recidivism_num_records"] = recidivism_num_records
    all_rollup_levels_df = all_rollup_levels_df.rename(
        columns={
            ("event_rate_dict", "PROBATION"): "recidivism_probation_series",
            ("event_rate_dict", "RIDER"): "recidivism_rider_series",
            ("event_rate_dict", "TERM"): "recidivism_term_series",
        }
    )
    all_rollup_levels_df = all_rollup_levels_df.reset_index()
    all_rollup_levels_df["recidivism_rollup"] = all_rollup_levels_df.apply(
        get_recidivism_rollup, axis=1
    )

    return all_rollup_levels_df


def create_final_table(
    rolled_up_recidivism_df: pd.DataFrame, disposition_df: pd.DataFrame
) -> pd.DataFrame:
    """Create final table that merges rolled_up_recidivism_df and disposition_df."""
    attrs_to_join = set(ROLLUP_ATTRIBUTES[0])
    attrs_to_join.remove("cohort_group")
    final_table = rolled_up_recidivism_df.merge(disposition_df, on=list(attrs_to_join))

    return final_table


def write_case_insights_data_to_bq(project_id: str) -> None:
    """Write case insights data to BigQuery.

    The final tables has a row for every state_code, gender, assessment_score_bucket_start, assessment_score_bucket_end,
    most_severe_description with:
    * The disposition percentages of each of PROBATION, RIDER, TERM
    * The cohort size for this row (disposition_num_records)
    * The series of recidivism stats over all months for each of PROBATION, RIDER, TERM for the appropriate rollup level
    * The cohort size of the corresponding rollup level (recidivism_num_records)
    * The comma-separated values of the rollup level (recidivism_rollup)
    """
    cohort_df = get_cohort_df(project_id)
    cohort_df = add_offense_attributes(cohort_df)

    event_df = get_recidivism_event_df(project_id)
    disposition_df = get_disposition_df(cohort_df)

    recidivism_df = gen_cohort_time_to_first_event(
        cohort_df=cohort_df,
        event_df=event_df,
        cohort_date_field="cohort_start_date",
        event_date_field="recidivism_date",
        join_field=["person_id", "state_code"],
    )
    # Replace None with INTERNAL_UNKNOWN so that these rows aren't dropped by any groupby
    recidivism_df.most_severe_ncic_category_uniform = (
        recidivism_df.most_severe_ncic_category_uniform.fillna("INTERNAL_UNKNOWN")
    )
    # TODO(#34529): Remove drop_duplicates here once it's properly handled in gen_cohort_time_to_first_event
    recidivism_df = recidivism_df.drop_duplicates(
        ["state_code", "person_id", "cohort_start_date"]
    )

    all_rollup_levels_df = get_all_rollup_aggregated_df(
        recidivism_df,
        RECIDIVISM_MONTHS,
        disposition_df,
    )
    rolled_up_recidivism_df = extract_rollup_columns(all_rollup_levels_df)

    final_table = create_final_table(rolled_up_recidivism_df, disposition_df)

    schema_cols = list(map(lambda x: x["name"], CASE_INSIGHTS_RATES_SCHEMA))
    final_table = final_table[schema_cols]
    final_table.to_gbq(
        destination_table=CASE_INSIGHTS_RATES_ADDRESS.to_str(),
        project_id=project_id,
        table_schema=CASE_INSIGHTS_RATES_SCHEMA,
        if_exists="replace",
    )
