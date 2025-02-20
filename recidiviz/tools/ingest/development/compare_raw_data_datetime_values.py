"""
Quick and dirty barebones script to compare datetime values for testing parsing; not 
super polished.

The intent here is to compare the columns between the current latest datasets and a 
new potential dataset (with some Sandbox prefix) based on SAME raw
datasets in order to verify that parsing (through the YAML) is being done correctly,
with no adverse impact to the parsing being done currently. 

Usage:
    python -m recidiviz.tools.ingest.development.compare_raw_data_datetime_values \
        --state_code <STATE_CODE> \
        --original-dataset <ORIGINAL_DATASET_ID>\
        --new-dataset <NEW_DATASET_ID> \
        --project-id <PROJECT_ID> \
            
An example concretely:

    python -m recidiviz.tools.ingest.development.compare_raw_data_datetime_values \
        --state_code US_ND\
        --original-dataset us_nd_raw_data_up_to_date_views\
            docstars_contacts_latest\
        --new-dataset <SANDBOX_PREFIX>_us_nd_raw_data_up_to_date_views\
        --project-id recividiz-staging\


Note this is usually used to compare views that we load to sandbox, presumably
with some changes, to the existing (latest) views in BigQuery. In the script, the 
'original' dataset refers to the existing (latest) views in BQ, and the 'new' dataset refers to the
views we generate and deploy to sandbox with some changes to YAML files we'd like to verify.   
"""

import argparse
import logging
import sys

import numpy as np
import pandas as pd
import pandas_gbq as gbq

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
    get_region_raw_file_config,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

RAW_DATA_LATEST_VIEW_ID_SUFFIX = "_latest"
logger = logging.getLogger()


def read_in_datetime_columns(
    original_dataset: str,
    new_dataset: str,
    col_names: list[str],
    project_id: str,
    file_tag: str,
) -> pd.DataFrame:
    """
    For each dataset, read in the relevant datetime columns for each table into a dataframe.
    Note we're reading into memory here; if datasets continue to get larger this is not ideal.
    At that point, we might have to change the way we do comparisons. This was intended as a
    relatively rough/simple way to verify for now.
    """

    query_string_col1 = f"""
    SELECT
     {", ".join(col_names)},
    FROM
     `{project_id}.{original_dataset}.{file_tag}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}`;
    """

    query_string_col2 = f"""
    SELECT
     {", ".join(col_names)},
    FROM
     `{project_id}.{new_dataset}.{file_tag}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}`;
    """

    try:
        # Read the relevant columns into a dataframe for the table in the original dataset
        orig_df = gbq.read_gbq(query_string_col1, project_id=project_id)

        # Read the relevant columns into a dataframe for the table in the new dataset
        new_df = gbq.read_gbq(query_string_col2, project_id=project_id)
    except Exception as e:
        logger.error(str(e))
        return pd.DataFrame(), pd.DataFrame()

    return orig_df, new_df


def compare_col_values(
    original_dataset: str,
    new_dataset: str,
    col_names: list[str],
    project_id: str,
    file_tag: str,
) -> None:
    """
    This is where we do any value comparisons for a given column.
    For now, just checking that column values are the same,
    if the original is not Null/NaT, to make sure
    we haven't introduced any new null values with the new column/table and that values
    align.
    """

    orig_df, new_df = read_in_datetime_columns(
        original_dataset, new_dataset, col_names, project_id, file_tag
    )
    if orig_df.empty:
        logger.info("No datetime columns found in view for table %s", file_tag)
        return

    # The only comparison/check we do for now
    check_diffs_and_null_values(orig_df, new_df)


def check_diffs_and_null_values(old_col: pd.DataFrame, new_col: pd.DataFrame) -> None:
    """
    Sort datetime column (coercing errors as None, for malformed date strings)
    and check that values are not different.
    """

    # Find row indices where new dataset is null but old dataset is not null in the
    # specified datetime column
    for column in old_col.columns:
        logger.info("Checking column %s", column)

        try:
            # Currently, dates separated by '/' are only in mm/dd/yyyy format
            # Otherwise, they are either yyyy-mm-dd or yyyymmdd. I.e, yearFirst format,
            # Which is why we specify it explicitly below when parsing
            old_col[column] = np.where(
                old_col[column].str.contains("/"),
                pd.to_datetime(old_col[column], errors="coerce"),
                pd.to_datetime(old_col[column], yearfirst=True, errors="coerce"),
            )
            old_col = old_col.sort_values(by=column).reset_index(drop=True)
            new_col[column] = np.where(
                new_col[column].str.contains("/"),
                pd.to_datetime(new_col[column], errors="coerce"),
                pd.to_datetime(new_col[column], yearfirst=True, errors="coerce"),
            )
            new_col = new_col.sort_values(by=column).reset_index(drop=True)
            print(old_col[column], new_col[column])
            check_bad_diffs(old_col, new_col, column)
        except Exception as e:
            logger.error(e)


def check_bad_diffs(
    old_col: pd.DataFrame,
    new_col: pd.DataFrame,
    column: str,
) -> None:
    """
    A 'Bad' difference at index i is one where the original dataset is nonull at i but the
    new dataset either differs or is null at i.

    Note, when only relying on the YAML parsers, we may run into instances of rows in the
    new dataset being slightly off in order compared to the old dataset, resulting in
    rows value mismatches. This is often caused by datetime values failing to be
    parsed by the parsers defined in the YAML, resulting in the row missing from the new
    dataset entirely.

    The solution currently is not elegant; examine where the script is showing a mismatch
    (it should name the file and column name),
    and/or print out rows before and after the mismatch to see what datetime format the
    YAML parsers are currently failing to account for, and add the corresponding parser
    to the file/column.
    """

    is_equal = old_col[column] == new_col[column]
    logger.info(
        "Previous null count for %s: %d", column, old_col[column].isnull().sum()
    )
    logger.info("New null count for %s: %d", column, new_col[column].isnull().sum())

    if not is_equal.all():
        # Get all row indices where the columns differ
        diff_indices = is_equal[is_equal is False].index
        logger.warning("Differences found! Checking if any are bad...")
        for idx in diff_indices:
            # Check if the new dataset is different from the original if original is nonnull
            if new_col.at[idx, column] != old_col.at[idx, column] and not pd.isna(
                old_col.at[idx, column]
            ):
                logger.error(
                    "Bad difference at index %d for column %s: \
                        original dataset has %s \
                            and new dataset has %s",
                    idx,
                    column,
                    old_col.at[idx, column],
                    new_col.at[idx, column],
                )
                logger.debug(old_col.iloc[: idx + 2], new_col.iloc[: idx + 1])
                print("Exiting")
                sys.exit(1)
        logger.info("No bad differences found. Continuing...")


def get_column_configs(state_code: StateCode) -> list[DirectIngestRawFileConfig]:
    """
    Get all tables with configs that contain datetime columns,
    for the raw dataset for a state
    """

    region_config = get_region_raw_file_config(state_code.value)
    all_raw_file_configs: list[DirectIngestRawFileConfig] = list(
        region_config.raw_file_configs.values()
    )
    configs_with_datetimes = [
        config
        for config in all_raw_file_configs
        if config.current_datetime_cols is not None
    ]

    return configs_with_datetimes


def get_datetime_columns_from_config(
    config: DirectIngestRawFileConfig,
) -> list[RawTableColumnInfo]:
    """
    Get all datetime columns for a particular raw data table
    """

    return [column for column in config.current_columns if column.is_datetime]


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)

    original_dataset = args.original_dataset
    new_dataset = args.new_dataset
    state_code = args.state_code
    project_id = args.project_id
    logger.info("Gathering files with relevant columns...")
    relevant_configs = get_column_configs(state_code)

    # For each file/table that contains the relevant columns, check to make sure
    # the new column is at least no worse than the original
    for config in relevant_configs:
        relevant_columns = get_datetime_columns_from_config(config)
        logger.info("Processing %s...", config.file_tag)
        col_names = [col.name for col in relevant_columns]
        with metadata.local_project_id_override(args.project_id):
            compare_col_values(
                original_dataset=original_dataset,
                new_dataset=new_dataset,
                col_names=col_names,
                project_id=project_id,
                file_tag=config.file_tag,
            )

    logger.info("New parsing method successfully verified!")


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parse command line arguments for use in script"""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state-code",
        dest="state_code",
        help="State for which datasets contain data",
        type=StateCode,
        required=True,
    )

    parser.add_argument(
        "--original-dataset",
        dest="original_dataset",
        help="Dataset with original column parsing method",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--new-dataset",
        dest="new_dataset",
        help="Dataset generated with new column type/parsing method",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--project-id",
        dest="project_id",
        help="Which project to read raw data from.",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        required=False,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    main()
