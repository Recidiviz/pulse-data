# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Entrypoint for Intercom outbound content data export"""

import argparse
import csv
import logging
import tempfile
from datetime import datetime, timezone

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import INTERCOM_EXPORT_DATASET
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.intercom.client import IntercomAPIClient
from recidiviz.intercom.intercom_export_bq_table_manager import (
    IntercomExportBigQueryTableManager,
)
from recidiviz.intercom.intercom_gcs_upload import (
    generate_intercom_outbound_content_gcs_path,
    intercom_gcs_upload,
)
from recidiviz.intercom.manager import IntercomAPIManager
from recidiviz.intercom.types import IntercomCloudRunJobInfo, IntercomCloudRunJobStatus
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.utils import metadata


def verify_headers(
    file_paths: dict[str, str],
) -> None:
    """
    Verify the CSV header matches the schema in the corresponding YAML file

    Args:
        file_paths: temp filepaths for Intercom outbound export data CSVs
    """

    repo = build_source_table_repository_for_yaml_managed_tables(project_id=None)

    for base_name, source_path in file_paths.items():

        big_query_address = BigQueryAddress(
            dataset_id=INTERCOM_EXPORT_DATASET, table_id=base_name
        )

        if big_query_address not in repo.source_tables:
            raise KeyError(
                f"No source table config found for [{big_query_address.to_str()}]. "
                f"Check that a YAML for this table exists under "
                f"recidiviz/source_tables/yaml_managed/gcs_backed_tables/intercom_export"
            )
        source_table_config = repo.source_tables[big_query_address]
        schema_fields_by_name = [f.name for f in source_table_config.schema_fields]

        with open(file=source_path, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

        for i, col_name in enumerate(headers):
            if col_name not in schema_fields_by_name:
                raise ValueError(
                    f"Column [{col_name}] not found in source table YAML for "
                    f"[{big_query_address.to_str()}]. "
                    f"Available columns: [{schema_fields_by_name}]"
                )
            if col_name != schema_fields_by_name[i]:
                raise ValueError(
                    f"CSV header does not match the ordering of the YAML schema fields "
                    f"found in [{big_query_address.to_str()}]. "
                    f"CSV columns: [{headers}]. "
                    f"Source table YAML schema fields: [{schema_fields_by_name}]."
                )


def export_and_upload_intercom_data(
    start_datetime_inclusive: datetime,
    end_datetime_inclusive: datetime,
    update_datetime: datetime,
) -> None:
    """Runs the full Intercom outbound data export process."""

    intercom_api_manager = IntercomAPIManager(client=IntercomAPIClient())

    # Download the data and export the CSVs to the GCS bucket
    export_job = intercom_api_manager.export_intercom_data(
        start_datetime_inclusive=start_datetime_inclusive,
        end_datetime_inclusive=end_datetime_inclusive,
    )

    intercom_api_manager.poll_export_status(job_identifier=export_job.job_identifier)

    with tempfile.TemporaryDirectory() as temp_output_dir:
        file_paths = intercom_api_manager.download_and_process_export(
            job_identifier=export_job.job_identifier,
            output_dir=temp_output_dir,
            update_datetime=update_datetime,
        )
    verify_headers(file_paths)

    current_project_id = metadata.project_id()
    for base_name, source_path in file_paths.items():
        destination_gcs_path = generate_intercom_outbound_content_gcs_path(
            project_id=current_project_id,
            file_base_name=base_name,
            update_datetime=update_datetime,
        )
        intercom_gcs_upload(
            intercom_source_path=source_path, destination_gcs_path=destination_gcs_path
        )


class IntercomOutboundDataExport(EntrypointInterface):
    """Entrypoint for Intercom outbound content data export"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--start-datetime-inclusive",
            help="UTC datetime for start of date range, inclusive",
            type=datetime.fromisoformat,
            required=False,
        )
        parser.add_argument(
            "--end-datetime-inclusive",
            help="UTC datetime for end of date range, inclusive",
            type=datetime.fromisoformat,
            required=False,
        )

        return parser

    @staticmethod
    def run_entrypoint(
        *,
        args: argparse.Namespace,
    ) -> None:
        """Runs Intercom outbound content data export and writes to the
        intercom_export.export_tracker table."""

        start_datetime_inclusive = args.start_datetime_inclusive
        end_datetime_inclusive = args.end_datetime_inclusive
        update_datetime = datetime.now(tz=timezone.utc)

        if (start_datetime_inclusive is not None) != (
            end_datetime_inclusive is not None
        ):
            raise ValueError(
                "start_datetime_inclusive and end_datetime_inclusive must either both be "
                "a datetime, or both be None. You entered: "
                f"start_datetime_inclusive: [{start_datetime_inclusive}] and "
                f"end_datetime_inclusive: [{end_datetime_inclusive}]. "
            )

        bq_table_manager = IntercomExportBigQueryTableManager(
            client=BigQueryClientImpl()
        )
        if not start_datetime_inclusive and not end_datetime_inclusive:
            start_datetime_inclusive = bq_table_manager.get_latest_export_window_end()
            end_datetime_inclusive = update_datetime

        try:
            export_and_upload_intercom_data(
                start_datetime_inclusive=start_datetime_inclusive,
                end_datetime_inclusive=end_datetime_inclusive,
                update_datetime=update_datetime,
            )
            job_status = IntercomCloudRunJobStatus.SUCCESS
        except Exception as e:
            logging.error("Failed to export Intercom data", exc_info=e)
            job_status = IntercomCloudRunJobStatus.FAILURE

        export_job_info = IntercomCloudRunJobInfo(
            export_datetime=update_datetime,
            export_window_start_inclusive=start_datetime_inclusive,
            export_window_end_inclusive=end_datetime_inclusive,
            status=job_status,
        )

        # Write the data to the BQ Intercom export tracker table
        bq_table_manager.write_to_table(cloud_run_job_info=export_job_info)

        if job_status == IntercomCloudRunJobStatus.FAILURE:
            raise ValueError(
                "Intercom export cloud run job failed due to a failure in the Intercom data export."
            )


if __name__ == "__main__":
    IntercomOutboundDataExport.run_entrypoint(
        args=IntercomOutboundDataExport.get_parser().parse_args()
    )
