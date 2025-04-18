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
        --export_name [LANTERN/CORE/etc], \
        --state_code [US_PA/US_MO/etc] (optional), \
"""
import argparse
import logging
import sys
from typing import List, Optional, Tuple

from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants import states
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_INDEX
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
)
from recidiviz.metrics.export.view_export_manager import (
    export_view_data_to_cloud_storage,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders


def export_metrics_from_dataset_to_gcs(
    export_name: str, state_code: Optional[str], sandbox_dataset_prefix: Optional[str]
) -> None:
    """Exports metric files into a sandbox GCS bucket."""
    view_sandbox_context = None
    if sandbox_dataset_prefix:
        sandbox_address_overrides = address_overrides_for_view_builders(
            view_dataset_override_prefix=sandbox_dataset_prefix,
            view_builders=deployed_view_builders(),
        )

        view_sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=sandbox_address_overrides,
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix=sandbox_dataset_prefix,
        )

    product_configs = ProductConfigs.from_file(path=PRODUCTS_CONFIG_PATH)
    _ = product_configs.get_export_config(
        export_job_name=export_name, state_code=state_code
    )

    export_view_data_to_cloud_storage(
        export_job_name=export_name,
        state_code=state_code,
        gcs_output_sandbox_subdir=sandbox_dataset_prefix,
        view_sandbox_context=view_sandbox_context,
    )

    logging.info(
        "Done exporting metrics from sandbox with prefix [%s].",
        sandbox_dataset_prefix,
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
        "--export_name",
        dest="export_name",
        choices=VIEW_COLLECTION_EXPORT_INDEX.keys(),
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


# TODO(#22440): Delete file after migration to using calculation dag for validation in secondary.
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        logging.info(
            "Exporting metrics from sandbox with prefix [%s]",
            known_args.sandbox_dataset_prefix,
        )

        export_metrics_from_dataset_to_gcs(
            known_args.export_name,
            known_args.state_code,
            known_args.sandbox_dataset_prefix,
        )
