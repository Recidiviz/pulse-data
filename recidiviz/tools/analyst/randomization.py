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
"""Tools for conducting and testing randomization"""

import numpy as np
import pandas as pd
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType
from scipy import stats

from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    build_source_table_repository_for_externally_managed_tables,
)
from recidiviz.source_tables.externally_managed.datasets import (
    MANUALLY_UPDATED_SOURCE_TABLES_DATASET,
)
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.types import assert_type


def stratified_randomization(
    df: pd.DataFrame,
    stratas_columns: list,
    share_to_treatment: float = 0.5,
    random_seed: int = 0,
) -> pd.DataFrame:
    """
    Perform stratified randomization on a dataframe. It will assign a treatment to each
    row of the dataframe. The treatment will be assigned randomly, but it will be
    balanced within each strata.

    Args:
        df: dataframe to randomize
        stratas_columns: list of columns to stratify on.
        share_to_treatment: share of units to assign to treatment. Default is 0.5 (half).
    """
    df_copy = df.copy()

    # Create a random uniform value for each row
    rng = np.random.default_rng(random_seed)
    df_copy["random_uniform"] = rng.uniform(0, 1, size=len(df_copy))
    # Sort by blocking columns and random uniform
    df_copy = df_copy.sort_values(stratas_columns + ["random_uniform"])
    # Create a count per block and total per block
    df_copy["count_per_block"] = df_copy.groupby(stratas_columns).cumcount() + 1
    df_copy["total_per_block"] = df_copy.groupby(stratas_columns)[
        stratas_columns[0]
    ].transform("count")
    # For cases where the total_per_block is odd, make some of the counts even randomly.
    #    That way the number of units in both group will be the same on average
    if round(1 / share_to_treatment, 1).is_integer():
        df_copy["total_per_block"] = df_copy["total_per_block"] + (
            (df_copy["total_per_block"] % (1 / share_to_treatment))
            * rng.choice([0, 1], size=len(df_copy))
        )
    # Assign treatment based to the first share_to_treatment of each block
    df_copy["treatment"] = (
        df_copy["count_per_block"] <= (share_to_treatment * df_copy["total_per_block"])
    ).astype(int)
    # Drop helper columns
    df_copy = df_copy.drop(
        ["random_uniform", "count_per_block", "total_per_block"], axis=1
    )

    return df_copy


def orthogonality_table(
    df: pd.DataFrame, columns_to_test: list, treatment_col: str = "treatment"
) -> pd.DataFrame:
    """
    Returns a table of t-statistics and p-values for the difference in means between
    the treatment and control groups. The null hypothesis is that the difference in means
    is zero.

    Args:
        df: A pandas dataframe with the treatment column and columns to test
        columns_to_test: List of column names to test
        treatment_col: Name of the treatment column
    """
    results = []

    for column in columns_to_test:
        group1 = df[df[treatment_col] == 1][column]
        group2 = df[df[treatment_col] == 0][column]

        t_stat, p_value = stats.ttest_ind(
            group1, group2, equal_var=False, nan_policy="omit"
        )

        mean1 = group1.mean()
        mean2 = group2.mean()
        mean_diff = mean1 - mean2

        results.append([column, mean1, mean2, mean_diff, t_stat, p_value])

    results_df = pd.DataFrame(
        results,
        columns=[
            "Column",
            "Treatment",
            "Control",
            "Difference",
            "T-Statistic",
            "P-Value",
        ],
    )
    results_df.loc[len(results_df)] = [
        "Number of units",
        len(df[df["treatment"] == 1]),
        len(df[df["treatment"] == 0]),
    ] + [None] * (len(results_df.columns) - 3)

    return results_df.round(2)


EXPERIMENT_ASSIGNMENTS_UNIT_ID_COLUMN = "unit_id"
EXPERIMENT_ASSIGNMENTS_UNIT_TYPE_COLUMN = "unit_type"
EXPERIMENT_ASSIGNMENTS_UPLOAD_DATETIME_COLUMN = "upload_datetime"
EXPERIMENT_ASSIGNMENTS_VARIANT_DATE_COLUMN = "variant_date"

_REFERENCED_EXPERIMENT_ASSIGNMENTS_COLUMNS = [
    EXPERIMENT_ASSIGNMENTS_UNIT_ID_COLUMN,
    EXPERIMENT_ASSIGNMENTS_UNIT_TYPE_COLUMN,
    EXPERIMENT_ASSIGNMENTS_UPLOAD_DATETIME_COLUMN,
    EXPERIMENT_ASSIGNMENTS_VARIANT_DATE_COLUMN,
]


def get_large_experiment_assignments_source_table_config() -> SourceTableConfig:
    """Returns the SourceTableConfig with schema information about the
    manually_updated_source_tables.experiment_assignments_large table, as defined in
    experiment_assignments_large.yaml.
    """
    table_address = BigQueryAddress(
        dataset_id=MANUALLY_UPDATED_SOURCE_TABLES_DATASET,
        table_id="experiment_assignments_large",
    )

    source_table_repository: SourceTableRepository = (
        build_source_table_repository_for_externally_managed_tables(project_id=None)
    )

    config = source_table_repository.get_config(table_address)
    for col in _REFERENCED_EXPERIMENT_ASSIGNMENTS_COLUMNS:
        if not config.has_column(col):
            raise ValueError(
                f"Expected [{config.address.to_str()}] to have column [{col}]"
            )
    return config


def upload_assignments_to_gbq(
    df: pd.DataFrame,
    unit_type: MetricUnitOfAnalysisType,
    upload_to_gbq: bool = False,
) -> None:
    """
    Appends an experiment assignment DataFrame to the BigQuery table <experiment_assignments_large>
    in the <manually_updated_source_tables> dataset. The final DF must have the same
    columns as defined in the table schema definition in
    experiment_assignments_large.yaml.

    Args:
        df: DataFrame containing the experiment assignments
        unit_type: The type of unit that the experiment assignments are associated with
        upload_to_gbq: If True, the DataFrame will be uploaded to BigQuery. If False, the
            DataFrame will not be uploaded to BigQuery and the function will only validate df.
    """
    experiments_table_config = get_large_experiment_assignments_source_table_config()
    df_copy = df.copy()

    assert_type(unit_type, MetricUnitOfAnalysisType)

    df_copy.loc[:, EXPERIMENT_ASSIGNMENTS_UNIT_TYPE_COLUMN] = unit_type.value
    df_copy.loc[:, EXPERIMENT_ASSIGNMENTS_UPLOAD_DATETIME_COLUMN] = pd.to_datetime(
        "today"
    )
    _validate_assignment_df_columns(df_copy, experiments_table_config)
    _validate_unique_id_and_variant_per_day(df_copy)

    num_rows = len(df_copy)
    if upload_to_gbq:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            project_specific_address = (
                experiments_table_config.address.to_project_specific_address(project_id)
            )
            print(
                f"Uploading [{num_rows}] rows to "
                f"`{project_specific_address.to_str()}`..."
            )
            df_copy.to_gbq(
                destination_table=experiments_table_config.address.to_str(),
                project_id=project_id,
                table_schema=[
                    col.to_api_repr() for col in experiments_table_config.schema_fields
                ],
                if_exists="append",
            )
            print(
                f"Done uploading [{num_rows}] rows to "
                f"`{project_specific_address.to_str()}`."
            )
    else:
        print("DataFrame was validated, but not uploaded to BigQuery.")


def _convert_df_column_to_type_if_necessary(
    df: pd.DataFrame, column_name: str, desired_type: BigQueryFieldType
) -> None:
    """Converts the values in the column with name |column_name| in the provided
    DataFrame to a type that is compatible for uploading to a BQ table with the
    |desired_type|.
    """
    try:
        if desired_type == BigQueryFieldType.STRING:
            if pd.api.types.is_string_dtype(df[column_name]):
                # No modification needed
                return
            df[column_name] = df[column_name].astype(str)
        elif desired_type == BigQueryFieldType.INT64:
            if pd.api.types.is_integer_dtype(df[column_name]):
                # No modification needed
                return
            df[column_name] = pd.to_numeric(df[column_name], downcast="integer")
        elif desired_type in (BigQueryFieldType.DATE, BigQueryFieldType.TIMESTAMP):
            if pd.api.types.is_datetime64_any_dtype(df[column_name]):
                # No modification needed
                return
            df[column_name] = pd.to_datetime(df[column_name])
        else:
            raise ValueError(
                f"Unsupported column type for column [{column_name}]: {desired_type}"
            )
    except Exception as e:
        raise ValueError(
            f"Column [{column_name}] cannot be converted to type [{desired_type}]: {e}"
        ) from e


def _validate_assignment_df_columns(
    df: pd.DataFrame, experiments_table_config: SourceTableConfig
) -> None:
    """Raises a ValueError if the DataFrame does not contain the required columns, if
    the columns are not in the correct format (or cannot be reasonably converted to the
    correct format), or if there are extra columns.

    Where possible, converts columns to the appropriate type to match the BQ schema.
    """
    required_columns = {
        column.name for column in experiments_table_config.schema_fields
    }
    df_columns = set(df.columns)

    # Check for extra columns
    extra_columns = df_columns - required_columns
    if extra_columns:
        raise ValueError(f"Extra columns present: {extra_columns}")

    # Check for missing columns
    missing_columns = required_columns - df_columns
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Validate column types
    for col in experiments_table_config.schema_fields:
        _convert_df_column_to_type_if_necessary(
            df, column_name=col.name, desired_type=BigQueryFieldType(col.field_type)
        )


def _validate_unique_id_and_variant_per_day(df: pd.DataFrame) -> None:
    """
    Checks if a single unit_id can have at most one variant_id on a given day.
    Raises a ValueError if any unit_id has more than one variant_id on the same
    variant_date.
    """
    # Group by unit_id and variant_date and count the number of unique variant_ids
    grouped = df.groupby(
        [
            EXPERIMENT_ASSIGNMENTS_UNIT_ID_COLUMN,
            EXPERIMENT_ASSIGNMENTS_VARIANT_DATE_COLUMN,
        ]
    ).variant_id.nunique()

    # Find any groups with more than one unique variant_id
    invalid_groups = grouped[grouped > 1]

    if not invalid_groups.empty:
        raise ValueError(
            f"Found unit_id(s) with multiple variant_id(s) on the same day: "
            f"{invalid_groups}"
        )
