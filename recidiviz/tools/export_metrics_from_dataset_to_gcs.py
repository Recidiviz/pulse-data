# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A script for exporting data from a dataset to a specified GCS bucket.
The dataset can be a staging/production dataset, or a sandbox dataset.
The script ensures that the metric buckets that serve the API are protected
such that those metric files are not overwritten.

This can be run on-demand whenever locally with the following command:
    python -m recidiviz.tools.export_metrics_from_dataset_to_gcs \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] (optional)\
        --destination_bucket [GCS_BUCKET_PATH] \
        --export_name [LANTERN/CORE/etc], \
        --state_code [US_PA/US_MO/etc] (optional), \
"""
import argparse
import logging
import sys
from typing import List, Tuple, Optional

from recidiviz.common.constants import states
from recidiviz.metrics.export import export_config
from recidiviz.metrics.export.export_config import (
    ExportViewCollectionConfig,
    VIEW_COLLECTION_EXPORT_CONFIGS,
)
from recidiviz.metrics.export.view_export_manager import trigger_export_for_configs
from recidiviz.tools.utils.dataset_overrides_for_all_view_datasets import (
    dataset_overrides_for_all_view_datasets,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


def get_protected_buckets(project_id: str) -> List[str]:
    protected_bucket_templates = {
        config.output_directory_uri_template
        for config in VIEW_COLLECTION_EXPORT_CONFIGS
    }

    return [
        bucket_template.format(project_id=project_id)
        for bucket_template in protected_bucket_templates
    ]


def export_metrics_from_dataset_to_gcs(
    destination_bucket: str,
    project_id: str,
    export_name: str,
    state_code: Optional[str],
    sandbox_dataset_prefix: Optional[str],
) -> None:
    """Exports metric files into a sandbox GCS bucket."""
    sandbox_dataset_overrides = None
    if sandbox_dataset_prefix:
        sandbox_dataset_overrides = dataset_overrides_for_all_view_datasets(
            view_dataset_override_prefix=sandbox_dataset_prefix,
        )

    # find the export config given the export name and state_code filter args
    filtered_export_config = None
    for dataset_export_config in export_config.VIEW_COLLECTION_EXPORT_CONFIGS:
        if not dataset_export_config.matches_filter(export_name):
            continue
        if not dataset_export_config.matches_filter(state_code):
            continue
        filtered_export_config = dataset_export_config

    # handle error states
    if not filtered_export_config:
        raise ValueError(
            "Export config not found. Please try a different combination of "
            "--export_config and --state_code"
        )

    if destination_bucket in get_protected_buckets(project_id):
        raise ValueError(
            f"Must specify a destination_bucket that is not a protected bucket. "
            f"Protected buckets are: {get_protected_buckets(project_id)}"
        )

    # override the filtered export config output directory with the destination_bucket arg
    custom_export_config = ExportViewCollectionConfig(
        view_builders_to_export=filtered_export_config.view_builders_to_export,
        output_directory_uri_template=destination_bucket,
        state_code_filter=filtered_export_config.state_code_filter,
        export_name=filtered_export_config.export_name,
        bq_view_namespace=filtered_export_config.bq_view_namespace,
        export_output_formats=filtered_export_config.export_output_formats,
    )

    trigger_export_for_configs(
        export_configs=[custom_export_config],
        dataset_overrides=sandbox_dataset_overrides,
    )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        help="Optional prefix specifying the sandbox datasets from which the metrics will be exported.",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--destination_bucket",
        dest="destination_bucket",
        help="Path of GCS bucket as destination for metrics file export.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--export_name",
        dest="export_name",
        choices=[config.export_name for config in VIEW_COLLECTION_EXPORT_CONFIGS],
        type=str,
        required=True,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        choices=[state.value for state in states.StateCode],
        help="State code to use when filtering dataset to create metrics export",
        type=str,
        required=False,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        logging.info(
            "Exporting metrics from sandbox with prefix [%s] to GCS bucket [%s].",
            known_args.sandbox_dataset_prefix,
            known_args.destination_bucket,
        )

        export_metrics_from_dataset_to_gcs(
            known_args.destination_bucket,
            known_args.project_id,
            known_args.export_name,
            known_args.state_code,
            known_args.sandbox_dataset_prefix,
        )
