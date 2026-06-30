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
"""Script that creates sandbox datasets (as appropriate) and launches a sandbox
identity ingest pipeline for the given tenant.

Usage:
    python -m recidiviz.tools.ingest.development.run_sandbox_identity_ingest_pipeline \
        --project PROJECT_ID \
        --tenant US_XX \
        --output_sandbox_prefix output_sandbox_prefix \
        [--raw_data_source_instance INSTANCE] \
        [--skip_build True/False]

Example:
    python -m recidiviz.tools.ingest.development.run_sandbox_identity_ingest_pipeline \
        --project recidiviz-staging \
        --tenant US_OZ \
        --output_sandbox_prefix my_prefix
"""
import json
import logging

from tabulate import tabulate

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_dataflow_sandbox,
)
from recidiviz.tools.ingest.development.sandbox_pipeline_utils import (
    build_extra_pipeline_parameter_args,
    common_sandbox_argument_parser,
)
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import (
    prompt_for_confirmation,
    requires_google_adc,
)
from recidiviz.utils.metadata import local_project_id_override


def run_sandbox_identity_ingest_pipeline(
    params: IdentityIngestPipelineParameters, skip_build: bool
) -> None:
    """Creates appropriate sandbox datasets then runs the sandbox identity ingest
    pipeline with the given parameters.
    """
    output_sandbox_prefix = params.output_sandbox_prefix
    if not output_sandbox_prefix:
        raise ValueError("Must specify an --output_sandbox_prefix")

    upper_bounds = json.loads(
        params.raw_data_upper_bound_dates_json,
    )
    logging.info(
        "Using raw data watermarks from latest run:\n%s",
        tabulate(
            [(tbl, upper_bounds[tbl]) for tbl in sorted(upper_bounds)],
            headers=["Table", "Raw Data Watermark"],
        ),
    )

    output_datasets = [
        getattr(params, prop_name)
        for prop_name in params.get_output_dataset_property_names()
    ]

    prompt_for_confirmation(
        "\n\nCreating Sandbox Pipeline With Parameters:\n"
        + tabulate(
            [
                ("Job Name", params.job_name),
                ("Tenant", params.tenant),
                ("Project", params.project),
                ("Output Datasets", output_datasets),
            ],
            headers=["Parameter", "Value"],
            tablefmt="rounded_grid",
        )
        + "\nWould you like to continue?"
    )

    create_or_update_dataflow_sandbox(
        sandbox_dataset_prefix=output_sandbox_prefix,
        pipelines=[IngestPipelineType.IDENTITY.pipeline_name],
        recreate=True,
        state_code_filter=StateCode(params.state_code),
    )

    run_sandbox_dataflow_pipeline(params, skip_build)


@requires_google_adc
def main() -> None:
    """Creates sandbox datasets (as appropriate) and launches a sandbox identity
    ingest pipeline as specified by the script args.
    """
    parser = common_sandbox_argument_parser()
    parser.add_argument(
        "--tenant",
        help="The tenant to run the identity ingest pipeline for. Choices are "
        "restricted to state-convertible tenants.",
        type=Tenant,
        choices=[
            Tenant.from_state_code(state_code)
            for state_code in get_existing_direct_ingest_states()
        ],
        required=True,
    )
    known_args, remaining_args = parser.parse_known_args()
    tenant = known_args.tenant
    state_code = tenant.to_state_code()
    # Re-add the args the wrapper's argparse consumed above so the pipeline's
    # own argparse (invoked by `parse_from_args` below) can see them too.
    remaining_args += [
        "--tenant",
        tenant.value,
        *build_extra_pipeline_parameter_args(
            project=known_args.project,
            state_code=state_code,
            raw_data_source_instance=known_args.raw_data_source_instance,
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        ),
    ]

    params = IdentityIngestPipelineParameters.parse_from_args(
        remaining_args, sandbox_pipeline=True
    )
    if params.tenant != tenant.value:
        raise ValueError(
            f"Generated params tenant [{params.tenant}] does not match the "
            f"input tenant [{tenant.value}]."
        )
    if params.raw_data_source_instance != known_args.raw_data_source_instance.value:
        raise ValueError(
            f"Generated params raw_data_source_instance "
            f"[{params.raw_data_source_instance}] does not match the input "
            f"raw_data_source_instance [{known_args.raw_data_source_instance.value}]."
        )

    with local_project_id_override(params.project):
        run_sandbox_identity_ingest_pipeline(params, skip_build=known_args.skip_build)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
