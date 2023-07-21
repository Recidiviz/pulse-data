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

"""The queries below generate a validation table of the most recent data values
processed for various tables in our pipeline. CSG uses the tables mentioned in
their monthly reports as such and therefore needs all results from this query
to be fresh through the beginning of the month that data is being sent in.
For example, if we are sending them data for the month of March 2023 on
April 3rd, 2023, the results of these queries should show no dates less than
2023-04-01. If one of the results is a date before what is expected, the
export_csg_files script should not be used until an investigation is done to
determine why data is not present in that table through the end of the previous
month.

If necessary, more validation queries can be added in over time.

For now, these queries should be copied and pasted into BQ and results evaluated there.
"""
import os
from typing import List, Optional, Tuple


class Validation:
    """
    Creates an object containing title, table, name, and SQL commands to insert into CSG data freshness validation query template
    """

    def __init__(
        self,
        validation_title: str,
        dataset_id: str,
        table_id: str,
        name: str,
        value_sql: str,
    ):
        self.validation_title = validation_title
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.name = name
        self.value_sql = value_sql


# This should be filled in for the state you are validation for. Example: "US_MO", or "US_PA" ;
state_code = "US_MO"

# Specifically to determine percent of is_draft violations, this should be filled in with the date that you are
# beginning the transfer in the export_csg_files script. Ex, "2023-02-28 ;
start_date_of_data_transfer = "2023-02-28"

validations = [
    Validation(
        validation_title="daily_supervision_population_val",
        dataset_id="dataflow_metrics_materialized",
        table_id="most_recent_supervision_population_metrics_materialized",
        name="last_date_metric_run_daily_supervision_pop",
        value_sql="MAX(created_on)",
    ),
    Validation(
        validation_title="daily_supervision_population_val",
        dataset_id="dataflow_metrics_materialized",
        table_id="most_recent_supervision_population_metrics_materialized",
        name="max_date_of_supervision",
        value_sql="MAX(date_of_supervision)",
    ),
    Validation(
        validation_title="commitments_from_supervision_val",
        dataset_id="dataflow_metrics_materialized",
        table_id="most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized",
        name="last_date_metric_run_comm_from_sup",
        value_sql="MAX(created_on)",
    ),
    Validation(
        validation_title="commitments_from_supervision_val",
        dataset_id="dataflow_metrics_materialized",
        table_id="most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized",
        name="max_admission_date_comm_from_sup",
        value_sql="MAX(admission_date)",
    ),
    Validation(
        validation_title="state_assessment_val",
        dataset_id="state",
        table_id="state_assessment",
        name="max_assessment_date",
        value_sql="MAX(assessment_date)",
    ),
    Validation(
        validation_title="state_charge_val",
        dataset_id="state",
        table_id="state_charge",
        name="max_charge_offense_date",
        value_sql="MAX(offense_date)",
    ),
    Validation(
        validation_title="state_incarceration_period_val",
        dataset_id="state",
        table_id="state_incarceration_period",
        name="max_admission_date",
        value_sql="MAX(admission_date)",
    ),
    Validation(
        validation_title="state_incarceration_period_val",
        dataset_id="state",
        table_id="state_incarceration_period",
        name="max_release_date",
        value_sql="MAX(release_date)",
    ),
    Validation(
        validation_title="state_incarceration_sentence_val",
        dataset_id="state",
        table_id="state_incarceration_sentence",
        name="max_imposed_date",
        value_sql="MAX(date_imposed)",
    ),
    Validation(
        validation_title="state_incarceration_sentence_val",
        dataset_id="state",
        table_id="state_incarceration_sentence",
        name="max_effective_date",
        value_sql="MAX(effective_date)",
    ),
    Validation(
        validation_title="state_supervision_period_val",
        dataset_id="state",
        table_id="state_supervision_period",
        name="max_start_date",
        value_sql="MAX(start_date)",
    ),
    Validation(
        validation_title="state_supervision_period_val",
        dataset_id="state",
        table_id="state_supervision_period",
        name="max_termination_date",
        value_sql="MAX(termination_date)",
    ),
    Validation(
        validation_title="state_supervision_sentence_val",
        dataset_id="state",
        table_id="state_supervision_sentence",
        name="max_imposed_date",
        value_sql="MAX(date_imposed)",
    ),
    Validation(
        validation_title="state_supervision_sentence_val",
        dataset_id="state",
        table_id="state_supervision_sentence",
        name="max_effective_date",
        value_sql="MAX(effective_date)",
    ),
    Validation(
        validation_title="state_supervision_violation_val",
        dataset_id="state",
        table_id="state_supervision_violation",
        name="max_violation_date",
        value_sql="MAX(violation_date)",
    ),
    Validation(
        validation_title="state_supervision_violation_response_val",
        dataset_id="state",
        table_id="state_supervision_violation_response",
        name="max_response_date",
        value_sql="MAX(response_date)",
    ),
    Validation(
        validation_title="state_supervision_violation_response_val",
        dataset_id="state",
        table_id="state_supervision_violation_response",
        name="percentage_of_not_draft_this_month",
        value_sql=f'SUM(IF(NOT is_draft and response_date > "{start_date_of_data_transfer}", 1, 0))/SUM(IF('
        f'response_date > "{start_date_of_data_transfer}", 1, 0))',
    ),
    Validation(
        validation_title="compartment_sessions_validation",
        dataset_id="sessions",
        table_id="compartment_sessions_materialized",
        name="last_day_of_data",
        value_sql="MAX(last_day_of_data)",
    ),
    Validation(
        validation_title="revocation_sessions_validation",
        dataset_id="sessions",
        table_id="revocation_sessions_materialized",
        name="max_revocation_date",
        value_sql="MAX(revocation_date)",
    ),
    Validation(
        validation_title="revocation_sessions_validation",
        dataset_id="sessions",
        table_id="revocation_sessions_materialized",
        name="max_last_day_of_data",
        value_sql="MAX(last_day_of_data)",
    ),
    Validation(
        validation_title="revocation_sessions_validation",
        dataset_id="sessions",
        table_id="reincarceration_sessions_from_sessions_materialized",
        name="max_release_date",
        value_sql="MAX(release_date)",
    ),
    Validation(
        validation_title="reincarceration_sessions_from_sessions_validation",
        dataset_id="sessions",
        table_id="reincarceration_sessions_from_sessions_materialized",
        name="max_reincarceration_date",
        value_sql="MAX(reincarceration_date)",
    ),
    Validation(
        validation_title="supervision_super_sessions_validation",
        dataset_id="sessions",
        table_id="supervision_super_sessions_materialized",
        name="max_start_date",
        value_sql="MAX(start_date)",
    ),
    Validation(
        validation_title="supervision_super_sessions_validation",
        dataset_id="sessions",
        table_id="supervision_super_sessions_materialized",
        name="max_last_day_of_data",
        value_sql="MAX(last_day_of_data)",
    ),
    Validation(
        validation_title="violation_responses_sessions_validation",
        dataset_id="sessions",
        table_id="violation_responses_materialized",
        name="max_response_date",
        value_sql="MAX(response_date)",
    ),
    Validation(
        validation_title="violation_responses_sessions_validation",
        dataset_id="sessions",
        table_id="violation_responses_materialized",
        name="max_most_recent_violation_date",
        value_sql="MAX(most_recent_violation_date)",
    ),
]

query_fragments = []
for validation in validations:
    query_fragments.append(
        f"""
    SELECT
        "{validation.validation_title}" as validation_title,
        "{validation.table_id}" as table_name,
        "{validation.name}" as validation_name,
        CAST({validation.value_sql} AS STRING) as value
    FROM `recidiviz-123.{validation.dataset_id}.{validation.table_id}`
    WHERE state_code = "{state_code}"
    """
    )

CSG_DATA_FRESHNESS_QUERY = "\nUNION ALL\n".join(query_fragments)


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ("CSG_DATA_FRESHNESS_QUERY", CSG_DATA_FRESHNESS_QUERY),
    ]


def _output_sql_queries(
    query_name_to_query_list: List[Tuple[str, str]], dir_path: Optional[str] = None
) -> None:
    """If |dir_path| is unspecified, prints the provided |query_name_to_query_list| to the console. Otherwise
    writes the provided |query_name_to_query_list| to the specified |dir_path|.
    """
    if not dir_path:
        _print_all_queries_to_console(query_name_to_query_list)
    else:
        _write_all_queries_to_files(dir_path, query_name_to_query_list)


def _write_all_queries_to_files(
    dir_path: str, query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Writes the provided queries to files in the provided path."""
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    for query_name, query_str in query_name_to_query_list:
        with open(
            os.path.join(dir_path, f"{query_name}.sql"), "w", encoding="utf-8"
        ) as output_path:
            output_path.write(query_str)


def _print_all_queries_to_console(
    query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Prints all the provided queries onto the console."""
    for query_name, query_str in query_name_to_query_list:
        print(f"\n\n/* {query_name.upper()} */\n")
        print(query_str)


if __name__ == "__main__":
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/csg_queries')
    _output_sql_queries(get_query_name_to_query_list(), output_dir)
