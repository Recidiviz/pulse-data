#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
A script to export all relevant files for CSG to a GCP bucket.

python -m recidiviz.tools.export_csg_files \
  --project-id [PROJECT_ID] \
  --state-code US_XX \
  --target-bucket [TARGET_EXPORT_BUCKET] \
  --start-date YYYY-MM-DD \
  --dry-run True
"""
import argparse
import logging
import sys
from datetime import date
from typing import Dict, List, Tuple

from google.cloud.bigquery.enums import DestinationFormat

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.common.date import is_date_str
from recidiviz.export.state import state_bq_table_export_to_csv
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

UNDERSCORE_TODAY = date.today().isoformat().replace("-", "_")
TEMP_DATASET_NAME = "temp_csg_export"

# list of sessions views to export
SESSIONS_TO_EXPORT: List[str] = [
    "compartment_sessions",
    "compartment_sentences",
    "revocation_sessions",
    "reincarceration_sessions_from_sessions",
    "supervision_super_sessions",
    "violation_responses",
]

# map from sessions views to list of fields to be serialized from array to string
SESSIONS_FIELDS_TO_SERIALIZE: Dict[str, List[str]] = {
    "compartment_sentences": [
        "classification_type",
        "description",
        "offense_type",
        "offense_type_short",
        "ncic_code",
        "felony_class",
    ],
    "violation_responses": [
        "violations_array",
    ],
}

# map from sessions views to list of fields to be serialized from date array to string
SESSIONS_DATE_FIELDS_TO_SERIALIZE: Dict[str, List[str]] = {
    "violation_responses": [
        "violation_dates_array",
    ],
}


def export_csg_files(
    dry_run: bool,
    project_id: str,
    state_code: str,
    target_bucket: str,
    start_date: date,
) -> None:
    """Exports all files requested by CSG for the specific state to the given bucket."""
    # First, export state tables
    state_bq_table_export_to_csv.run_export(
        dry_run=dry_run,
        state_code=state_code,
        target_bucket=target_bucket,
    )

    # second, export individual queries

    # create a temp dataset for exports
    client = BigQueryClientImpl(project_id=project_id)

    if not dry_run:
        client.create_dataset_if_necessary(f"{project_id}.{TEMP_DATASET_NAME}", 3600000)

    # now do a bunch of metric exports
    export_configs = []
    export_configs += generate_metric_export_configs(
        project_id, state_code, start_date, target_bucket
    )
    export_configs += generate_sessions_export_configs(
        project_id,
        state_code,
        target_bucket,
        SESSIONS_TO_EXPORT,
        SESSIONS_FIELDS_TO_SERIALIZE,
        SESSIONS_DATE_FIELDS_TO_SERIALIZE,
    )
    export_configs += generate_state_specific_export_configs(
        project_id, state_code, start_date, target_bucket
    )

    # export the views
    if dry_run:
        logging.info("[DRY RUN] Created %d export configs:", len(export_configs))
        for config in export_configs:
            logging.info("%s", config)
    else:
        try:
            client.export_query_results_to_cloud_storage(export_configs, True)
        except Exception as e:
            logging.error("Failed to export data: %s", e)
        finally:
            client.delete_dataset(TEMP_DATASET_NAME, delete_contents=True)


def generate_metric_export_configs(
    project_id: str, state_code: str, start_date: date, target_bucket: str
) -> List[ExportQueryConfig]:
    """Generate a list of ExportQueryConfigs for dataflow metric views."""
    exports = []

    supervision_query = f"""
      SELECT
        DISTINCT state_code,
        year,
        month,
        supervision_type,
        assessment_score_bucket,
        assessment_type,
        supervising_officer_external_id,
        supervising_district_external_id,
        age,
        prioritized_race_or_ethnicity,
        gender,
        case_type,
        person_id,
        person_external_id,
        supervision_level,
        supervision_level_raw_text,
        judicial_district_code,
        date_of_supervision
      FROM
        `{project_id}.dataflow_metrics_materialized.most_recent_supervision_population_metrics_materialized`
      WHERE
        state_code = '{state_code.upper()}'
        AND date_of_supervision >= '{start_date.isoformat()}'
    """

    exports.append(
        metric_export_config(
            supervision_query,
            "daily_supervision_population",
            start_date,
            state_code,
            target_bucket,
        )
    )

    commitment_query = f"""
      SELECT
        DISTINCT state_code,
        year,
        month,
        supervision_type,
        assessment_score_bucket,
        assessment_type,
        specialized_purpose_for_incarceration,
        purpose_for_incarceration_subtype,
        supervising_officer_external_id,
        supervising_district_external_id,
        age,
        prioritized_race_or_ethnicity,
        gender,
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        most_severe_response_decision,
        response_count,
        person_external_id,
        secondary_person_external_id,
        person_id,
        violation_history_description,
        case_type,
        admission_date
      FROM
        `{project_id}.dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population`
      WHERE
        state_code = '{state_code.upper()}'
        AND Date(year, month, 1) >= Date({start_date.year}, {start_date.month}, 1)
      ORDER BY
        year,
        month
    """

    exports.append(
        metric_export_config(
            commitment_query,
            "commitments_from_supervision",
            start_date,
            state_code,
            target_bucket,
        )
    )

    return exports


def metric_export_config(
    query: str, export_name: str, start_date: date, state_code: str, target_bucket: str
) -> ExportQueryConfig:
    underscore_start_date = start_date.isoformat().replace("-", "_")

    return ExportQueryConfig(
        query=query,
        query_parameters=[],
        intermediate_dataset_id=TEMP_DATASET_NAME,
        intermediate_table_name=f"{state_code}_{export_name}",
        output_uri=f"gs://{target_bucket}/recidiviz_calculation_output/{export_name}/{state_code.lower()}/{underscore_start_date}_to_{UNDERSCORE_TODAY}/{state_code.lower()}_{export_name}-*.csv",
        output_format=DestinationFormat.CSV,
    )


def generate_sessions_export_configs(
    project_id: str,
    state_code: str,
    target_bucket: str,
    sessions_to_export: List[str],
    sessions_fields_to_serialize: Dict[str, List[str]],
    sessions_date_fields_to_serialize: Dict[str, List[str]],
) -> List[ExportQueryConfig]:
    """Generate a list of ExportQueryConfigs for the given list of sessions views.
    Serializes any array fields given in `sessions_fields_to_serialize` via
    pipe delimitation.
    Serializes any date array fields given in `sessions_date_fields_to_serialize` via
    unnest and pipe delimitation.
    """
    exports = []

    # export person_id to external_id mapping
    state_person_external_id_query = f"""
      SELECT *
      FROM `{project_id}.state.state_person_external_id`
      WHERE state_code = "{state_code.upper()}"  
    """

    exports.append(
        ExportQueryConfig(
            query=state_person_external_id_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="state_person_external_id",
            output_uri=f"gs://{target_bucket}/recidiviz_sessionized_data/{state_code.lower()}/{UNDERSCORE_TODAY}/{state_code.lower()}_external_id.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    # export sessions views defined at top
    exports += [
        session_export_config(
            project_id,
            state_code,
            session,
            target_bucket,
            sessions_fields_to_serialize,
            sessions_date_fields_to_serialize,
        )
        for session in sessions_to_export
    ]

    return exports


def session_export_config(
    project_id: str,
    state_code: str,
    session_name: str,
    target_bucket: str,
    sessions_fields_to_serialize: Dict[str, List[str]],
    sessions_date_fields_to_serialize: Dict[str, List[str]],
) -> ExportQueryConfig:
    """Configures SQL statements based on arrays needed to be serialized and outputs ExportQueryConfig"""
    if (
        session_name in sessions_fields_to_serialize
        and session_name in sessions_date_fields_to_serialize
    ):
        exploded_fields = sessions_fields_to_serialize[session_name]
        exploded_date_fields = sessions_date_fields_to_serialize[session_name]
        query = f"""
          SELECT * EXCEPT({", ".join(exploded_fields)},{", ".join(exploded_date_fields)}),
          {", ".join(f'ARRAY_TO_STRING({field}, "|") AS {field}' for field in exploded_fields)},
          {", ".join(f'(SELECT STRING_AGG(FORMAT_DATE("%Y-%m-%d", d),"|") FROM UNNEST({field}) d) AS {field}' for field in exploded_date_fields)}
          FROM `{project_id}.sessions.{session_name}_materialized`
          WHERE state_code = "{state_code.upper()}"
        """
    elif (
        session_name in sessions_fields_to_serialize
        and session_name not in sessions_date_fields_to_serialize
    ):
        exploded_fields = sessions_fields_to_serialize[session_name]
        query = f"""
          SELECT * EXCEPT({", ".join(exploded_fields)}),
          {", ".join(f'ARRAY_TO_STRING({field}, "|") AS {field}' for field in exploded_fields)}
          FROM `{project_id}.sessions.{session_name}_materialized`
          WHERE state_code = "{state_code.upper()}"
        """
    elif (
        session_name not in sessions_fields_to_serialize
        and session_name in sessions_date_fields_to_serialize
    ):
        exploded_date_fields = sessions_date_fields_to_serialize[session_name]
        query = f"""
          SELECT * EXCEPT({", ".join(exploded_date_fields)}),
          {", ".join(f'(SELECT STRING_AGG(FORMAT_DATE("%Y-%m-%d", d),"|") FROM UNNEST({field}) d) AS {field}' for field in exploded_date_fields)}
          FROM `{project_id}.sessions.{session_name}_materialized`
          WHERE state_code = "{state_code.upper()}"
        """
    else:
        query = f"""
          SELECT *
          FROM `{project_id}.sessions.{session_name}_materialized`
          WHERE state_code = "{state_code.upper()}"
        """

    return ExportQueryConfig(
        query=query,
        query_parameters=[],
        intermediate_dataset_id=TEMP_DATASET_NAME,
        intermediate_table_name=f"{state_code}_{session_name}_session",
        output_uri=f"gs://{target_bucket}/recidiviz_sessionized_data/{state_code.lower()}/{UNDERSCORE_TODAY}/{state_code.lower()}_{session_name}_sessions.csv",
        output_format=DestinationFormat.CSV,
    )


def generate_state_specific_export_configs(
    project_id: str,
    state_code: str,
    start_date: date,
    target_bucket: str,
) -> List[ExportQueryConfig]:
    """Generates a list of exports specific to the given state or returns an
    empty list if there are none."""
    if state_code.upper() == "US_PA":
        return us_pa_specific_exports(project_id, start_date, target_bucket)

    return []


def us_pa_specific_exports(
    project_id: str,
    start_date: date,
    target_bucket: str,
) -> List[ExportQueryConfig]:
    """Generate a list of exports specific to US_PA."""
    configs = []

    treatment_query = f"""
    SELECT *
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_Treatment_latest`
    WHERE LastModifiedDateTime >= '{start_date.isoformat()}'
    """

    configs.append(
        ExportQueryConfig(
            query=treatment_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_treatment",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_treatment_data/dbo_Treatment_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    hist_treatment_query = f"""
    SELECT *
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_Hist_Treatment_latest`
    """

    configs.append(
        ExportQueryConfig(
            query=hist_treatment_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_hist_treatment",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_treatment_data/dbo_Hist_Treatment_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    board_action_query = f"""
    SELECT *
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_BoardAction_latest`
    WHERE CONCAT(BdActEntryDateYear, '-', BdActEntryDateMonth, '-', BdActEntryDateDay) >= '{start_date.isoformat()}'
    ORDER BY BdActEntryDateYear, BdActEntryDateMonth, BdActEntryDateDay
    """

    configs.append(
        ExportQueryConfig(
            query=board_action_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_board_action",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_board_conditions_data/dbo_BoardAction_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    condition_code_query = f"""
    SELECT codes.*
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_ConditionCode_latest` codes
    JOIN `{project_id}.us_pa_raw_data_up_to_date_views.dbo_BoardAction_latest` actions
    USING (ParoleNumber, ParoleCountID, BdActionId)
    WHERE CONCAT(BdActEntryDateYear, '-', BdActEntryDateMonth, '-', BdActEntryDateDay) >= '{start_date.isoformat()}'
    """

    configs.append(
        ExportQueryConfig(
            query=condition_code_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_condition_code",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_board_conditions_data/dbo_ConditionCode_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    condition_code_description_query = f"""
    SELECT descriptions.*
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_ConditionCodeDescription_latest` descriptions
    JOIN `{project_id}.us_pa_raw_data_up_to_date_views.dbo_ConditionCode_latest` codes
    USING (ParoleNumber, ParoleCountID, BdActionId, ConditionCodeID)
    JOIN `{project_id}.us_pa_raw_data_up_to_date_views.dbo_BoardAction_latest` actions
    USING (ParoleNumber, ParoleCountID, BdActionId)
    WHERE CONCAT(BdActEntryDateYear, '-', BdActEntryDateMonth, '-', BdActEntryDateDay) >= '{start_date.isoformat()}'
    """

    configs.append(
        ExportQueryConfig(
            query=condition_code_description_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_condition_code_description",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_board_conditions_data/dbo_ConditionCodeDescription_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    lsihistory_query = f"""
    SELECT *
    FROM `{project_id}.us_pa_raw_data_up_to_date_views.dbo_LSIHistory_latest`
    WHERE LastModifiedDateTime >= '{start_date.isoformat()}'
    """

    configs.append(
        ExportQueryConfig(
            query=lsihistory_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_pa_dbo_lsihistory",
            output_uri=f"gs://{target_bucket}/raw_data/us_pa/{UNDERSCORE_TODAY}_LSIR_data/dbo_LSIHistory_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    return configs


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument(
        "--target-bucket",
        required=True,
        help="The target Google Cloud Storage bucket to export data to, e.g. recidiviz-123-some-data",
    )

    parser.add_argument(
        "--state-code", required=True, help="The state code to export data for"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="The earliest data to be exported in YYY-MM-DD format.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    if not is_date_str(known_args.start_date):
        logging.error(
            "start_date must be of the form YYY-MM-DD. Received %s",
            known_args.start_date,
        )
        sys.exit()

    with local_project_id_override(known_args.project_id):
        export_csg_files(
            dry_run=known_args.dry_run,
            project_id=known_args.project_id,
            state_code=known_args.state_code,
            target_bucket=known_args.target_bucket,
            start_date=date.fromisoformat(known_args.start_date),
        )
