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
A script to export MO Case Plan files for CSG to a GCP bucket.

python -m recidiviz.tools.export_mo_case_plans \
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
from typing import List, Tuple

from google.cloud.bigquery.enums import DestinationFormat

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

UNDERSCORE_TODAY = date.today().isoformat().replace("-", "_")
TEMP_DATASET_NAME = "temp_csg_export"


def export_csg_files(
    dry_run: bool,
    project_id: str,
    state_code: str,
    target_bucket: str,
) -> None:
    """Exports all files requested by CSG for the specific state to the given bucket."""

    # create a temp dataset for exports
    client = BigQueryClientImpl(project_id=project_id)
    if not dry_run:
        client.create_dataset_if_necessary(
            TEMP_DATASET_NAME, default_table_expiration_ms=3600000
        )

    # now do a bunch of metric exports
    export_configs = []
    export_configs = generate_state_specific_export_configs(
        project_id, state_code, target_bucket
    )

    # export the views
    if dry_run:
        logging.info("[DRY RUN] Created %d export configs:", len(export_configs))
        for config in export_configs:
            logging.info("%s", config)
    else:
        try:
            client.export_query_results_to_cloud_storage(
                export_configs=export_configs,
                print_header=True,
                use_query_cache=True,
            )
        except Exception as e:
            logging.error("Failed to export data: %s", e)
        finally:
            client.delete_dataset(TEMP_DATASET_NAME, delete_contents=True)


def generate_state_specific_export_configs(
    project_id: str,
    state_code: str,
    target_bucket: str,
) -> List[ExportQueryConfig]:
    """Generates a list of exports specific to the given state or returns an
    empty list if there are none."""
    if state_code.upper() == "US_MO":
        return us_mo_specific_exports(project_id, target_bucket)

    return []


def us_mo_specific_exports(
    project_id: str,
    target_bucket: str,
) -> List[ExportQueryConfig]:
    """Generate a list of exports specific to US_MO."""
    configs = []

    case_plans_query = f"""
    SELECT *
    FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLANS_DB2_latest`
    """

    configs.append(
        ExportQueryConfig(
            query=case_plans_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLANS_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLANS_data/MO_CASEPLANS_DB2_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    case_plans_goals_query = f"""
        SELECT *
        FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLAN_GOALS_DB2_latest`
        """

    configs.append(
        ExportQueryConfig(
            query=case_plans_goals_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLAN_GOALS_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLAN_GOALS_data/MO_CASEPLAN_GOALS_DB2_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    case_plans_info_query = f"""
        SELECT *
        FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLAN_INFO_DB2_latest`
        """

    configs.append(
        ExportQueryConfig(
            query=case_plans_info_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLAN_INFO_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLAN_INFO_data/MO_CASEPLAN_INFO_DB2_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    case_plans_objectives_query = f"""
            SELECT *
            FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLAN_OBJECTIVES_DB2_latest`
            """

    configs.append(
        ExportQueryConfig(
            query=case_plans_objectives_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLAN_OBJECTIVES_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLAN_OBJECTIVES_data/MO_CASEPLAN_OBJECTIVES_DB2_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    case_plans_targets_query = f"""
            SELECT *
            FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLAN_TARGETS_DB2_latest`
            """

    configs.append(
        ExportQueryConfig(
            query=case_plans_targets_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLAN_TARGETS_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLAN_TARGETS_data/MO_CASEPLAN_TARGETS_DB2_latest.csv",
            output_format=DestinationFormat.CSV,
        )
    )

    case_plans_techniques_query = f"""
            SELECT *
            FROM `{project_id}.us_mo_raw_data_up_to_date_views.MO_CASEPLAN_TECHNIQUES_DB2_latest`
            """

    configs.append(
        ExportQueryConfig(
            query=case_plans_techniques_query,
            query_parameters=[],
            intermediate_dataset_id=TEMP_DATASET_NAME,
            intermediate_table_name="us_mo_MO_CASEPLAN_TECHNIQUES_DB2",
            output_uri=f"gs://{target_bucket}/raw_data/us_mo/{UNDERSCORE_TODAY}_CASEPLAN_TECHNIQUES_data/MO_CASEPLAN_TECHNIQUES_DB2_latest.csv",
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

    with local_project_id_override(known_args.project_id):
        export_csg_files(
            dry_run=known_args.dry_run,
            project_id=known_args.project_id,
            state_code=known_args.state_code,
            target_bucket=known_args.target_bucket,
        )
