# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A script that ;oads data into a BigQuery / GCS sandbox that reflects all local
changes that impact Dataflow pipelines or BigQuery views.

To see usage info:
python -m recidiviz.tools.load_end_to_end_data_sandbox --help

Example usages:
python -m recidiviz.tools.load_end_to_end_data_sandbox \
    --state_code US_XX \
    --sandbox_prefix my_prefix \
    --load_up_to_datasets dashboard_views

python -m recidiviz.tools.load_end_to_end_data_sandbox \
    --state_code US_XX \
    --sandbox_prefix my_prefix \
    --load_up_to_addresses aggregated_metrics.incarceration_state_aggregated_metrics,aggregated_metrics.supervision_state_aggregated_metrics
"""
import argparse
import json
import logging

from tabulate import tabulate

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.pipelines.flex_pipeline_runner import pipeline_cls_for_pipeline_name
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.pipeline_names import (
    INGEST_PIPELINE_NAME,
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.pipelines.pipeline_parameters import (
    PipelineParameters,
    PipelineParametersT,
)
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_dataflow_sandbox,
)
from recidiviz.tools.ingest.development.run_sandbox_ingest_pipeline import (
    get_raw_data_upper_bound_dates_json_for_sandbox_pipeline,
)
from recidiviz.tools.load_views_to_sandbox import (
    collect_changed_views_and_descendants_to_load,
    load_collected_views_to_sandbox,
)
from recidiviz.tools.utils.arg_parsers import str_to_address_list
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_all_reference_query_input_datasets_for_pipeline,
    get_sandbox_pipeline_username,
    push_sandbox_dataflow_pipeline_docker_image,
    run_sandbox_dataflow_pipeline,
)
from recidiviz.tools.utils.script_helpers import (
    prompt_for_confirmation,
    prompt_for_step,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_list
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict


def get_ingest_pipeline_params(
    *,
    project_id: str,
    state_code: StateCode,
    raw_data_source_instance: DirectIngestInstance,
    sandbox_username: str,
    output_sandbox_prefix: str,
) -> IngestPipelineParameters:
    raw_data_upper_bound_dates_json = (
        get_raw_data_upper_bound_dates_json_for_sandbox_pipeline(
            project_id,
            state_code,
            raw_data_source_instance,
        )
    )

    return IngestPipelineParameters(
        project=project_id,
        state_code=state_code.value,
        output_sandbox_prefix=output_sandbox_prefix,
        pipeline=INGEST_PIPELINE_NAME,
        region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[state_code],
        sandbox_username=sandbox_username,
        input_dataset_overrides_json=None,
        raw_data_source_instance=raw_data_source_instance.value,
        raw_data_upper_bound_dates_json=raw_data_upper_bound_dates_json,
    )


def _build_sandbox_post_ingest_pipeline_params(
    sandbox_ingest_pipeline_params: IngestPipelineParameters,
    params_cls: type[PipelineParametersT],
    pipeline_config_dict: YAMLDict,
) -> PipelineParametersT:
    """Builds parameters for a single pipeline that will read from the output of the
    sandbox ingest pipeline with the provided parameters.
    """

    standard_params = params_cls(
        project=sandbox_ingest_pipeline_params.project,
        **pipeline_config_dict.get(),  # type: ignore
    )
    pipeline_cls = pipeline_cls_for_pipeline_name(standard_params.pipeline)
    standard_input_datasets = standard_params.get_standard_input_datasets(
        get_all_reference_query_input_datasets_for_pipeline(
            pipeline_cls, StateCode(standard_params.state_code)
        )
    )

    output_dataset_overrides = assert_type(
        sandbox_ingest_pipeline_params.output_dataset_overrides,
        BigQueryAddressOverrides,
    )
    all_ingest_output_dataset_overrides = (
        output_dataset_overrides.get_full_dataset_overrides_dict()
    )

    valid_input_overrides = {
        original_dataset: override
        for original_dataset, override in all_ingest_output_dataset_overrides.items()
        if original_dataset in standard_input_datasets
    }
    sandbox_params = params_cls(
        project=sandbox_ingest_pipeline_params.project,
        sandbox_username=sandbox_ingest_pipeline_params.sandbox_username,
        output_sandbox_prefix=sandbox_ingest_pipeline_params.output_sandbox_prefix,
        input_dataset_overrides_json=json.dumps(valid_input_overrides),
        **pipeline_config_dict.get(),  # type: ignore
    )
    return sandbox_params


def get_sandbox_post_ingest_pipeline_params(
    state_code_filter: StateCode,
    ingest_pipeline_params: IngestPipelineParameters,
) -> list[MetricsPipelineParameters | SupplementalPipelineParameters]:
    """Returns the set of parameters for sandbox post-ingest pipelines which will read
    from the outputs of the sandbox ingest pipeline with the given params.
    """
    if state_code_filter.value != ingest_pipeline_params.state_code:
        raise ValueError(
            f"Found state_code for ingest parameters "
            f"[{ingest_pipeline_params.state_code}] which does not match "
            f"state_code_filter [{state_code_filter.value}]."
        )

    all_pipeline_configs = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

    all_pipeline_parameters: list[
        MetricsPipelineParameters | SupplementalPipelineParameters
    ] = []
    for pipeline_config in all_pipeline_configs.pop_dicts("metric_pipelines"):
        if pipeline_config.peek("state_code", str) != state_code_filter.value:
            continue

        all_pipeline_parameters.append(
            _build_sandbox_post_ingest_pipeline_params(
                ingest_pipeline_params, MetricsPipelineParameters, pipeline_config
            )
        )

    for pipeline_config in all_pipeline_configs.pop_dicts(
        "supplemental_dataset_pipelines"
    ):
        if pipeline_config.peek("state_code", str) != state_code_filter.value:
            continue

        all_pipeline_parameters.append(
            _build_sandbox_post_ingest_pipeline_params(
                ingest_pipeline_params, SupplementalPipelineParameters, pipeline_config
            )
        )

    for params in all_pipeline_parameters:
        pipeline_cls = pipeline_cls_for_pipeline_name(params.pipeline)
        params.check_for_valid_input_dataset_overrides(
            get_all_reference_query_input_datasets_for_pipeline(
                pipeline_cls, StateCode(params.state_code)
            )
        )

    return all_pipeline_parameters


def get_view_update_input_dataset_overrides_dict(
    ingest_pipeline_params: IngestPipelineParameters,
    post_ingest_pipeline_params: list[
        MetricsPipelineParameters | SupplementalPipelineParameters
    ],
) -> dict[str, str]:
    """Returns the dictionary of source table dataset overrides that should be used
    when loading sandbox views that read from the given sandbox pipelines.
    """
    input_dataset_overrides_dict: dict[str, str] = {}
    for params in [ingest_pipeline_params, *post_ingest_pipeline_params]:
        output_dataset_overrides = assert_type(
            params.output_dataset_overrides, BigQueryAddressOverrides
        )
        if address_overrides := output_dataset_overrides.get_address_overrides_dict():
            # We expect overrides for a pipeline to only be at the dataset level
            raise ValueError(
                f"Did not expect output_dataset_overrides to have address-specific "
                f"overrides, found: {address_overrides}"
            )
        input_dataset_overrides_dict.update(
            output_dataset_overrides.get_full_dataset_overrides_dict()
        )

    valid_source_table_datasets = get_source_table_datasets(
        ingest_pipeline_params.project
    )
    return {
        dataset: override
        for dataset, override in input_dataset_overrides_dict.items()
        if dataset in valid_source_table_datasets
    }


def get_exports_to_run() -> list[ExportViewCollectionConfig]:
    # TODO(#26138): Implement - return the set of metric exports that should be run as
    #  part of this sandbox run (e.g. based on some new script argument)
    return []


def _get_params_summary(params_list: list[PipelineParameters]) -> str:
    table_data = []
    for params in params_list:
        input_dataset_overrides = (
            params.input_dataset_overrides.get_full_dataset_overrides_dict()
            if params.input_dataset_overrides
            else None
        )
        # We expect these to be sandbox pipeline parameters which will always have
        # output overrides.
        output_dataset_overrides = assert_type(
            params.output_dataset_overrides, BigQueryAddressOverrides
        ).get_full_dataset_overrides_dict()
        metadata = {
            "state_code": params.state_code,
            "pipeline_name": params.pipeline,
            "input_dataset_overrides": input_dataset_overrides,
            "output_dataset_overrides": output_dataset_overrides,
        }
        if isinstance(params, MetricsPipelineParameters):
            metadata["metric_types"] = params.metric_types

        table_data.append([params.job_name, json.dumps(metadata, indent=2)])

    return tabulate(
        table_data,
        headers=["Job name", "Metadata"],
        tablefmt="fancy_grid",
    )


def load_end_to_end_sandbox(
    *,
    project_id: str,
    state_code: StateCode,
    output_sandbox_prefix: str,
    raw_data_source_instance: DirectIngestInstance,
    load_up_to_addresses: list[BigQueryAddress] | None,
    load_up_to_datasets: list[str] | None,
    changed_datasets_to_include: list[str] | None,
    changed_datasets_to_ignore: list[str] | None,
) -> None:
    """Loads data into a BigQuery / GCS sandbox that reflects all local changes that
    impact Dataflow pipelines or BigQuery views.
    """
    print("\n~~~~~~~~~~~~~~~~~~~~~~ [PREPARING SANDBOX LOAD] ~~~~~~~~~~~~~~~~~~~~~~\n")

    sandbox_username = get_sandbox_pipeline_username()

    print("\nCollecting sandbox pipeline parameters...\n")
    ingest_pipeline_params = get_ingest_pipeline_params(
        project_id=project_id,
        state_code=state_code,
        raw_data_source_instance=raw_data_source_instance,
        output_sandbox_prefix=output_sandbox_prefix,
        sandbox_username=sandbox_username,
    )

    with local_project_id_override(project_id):
        post_ingest_pipeline_params = get_sandbox_post_ingest_pipeline_params(
            state_code_filter=state_code, ingest_pipeline_params=ingest_pipeline_params
        )

        print("\nFound the following pipelines to run:")
        print(
            _get_params_summary([ingest_pipeline_params, *post_ingest_pipeline_params])
        )
        prompt_for_confirmation("Continue? ")

        print("\nCollecting views to load after pipelines run...\n")
        view_update_input_dataset_overrides_dict = (
            get_view_update_input_dataset_overrides_dict(
                ingest_pipeline_params, post_ingest_pipeline_params
            )
        )
        view_builders_to_load = collect_changed_views_and_descendants_to_load(
            prompt=True,
            input_source_table_dataset_overrides_dict=(
                view_update_input_dataset_overrides_dict
            ),
            changed_datasets_to_include=changed_datasets_to_include,
            changed_datasets_to_ignore=changed_datasets_to_ignore,
            load_changed_views_only=False,
            load_up_to_addresses=load_up_to_addresses,
            load_up_to_datasets=load_up_to_datasets,
        )

        print("\nCollecting views to load after pipelines run...\n")
        exports_for_views = get_exports_to_run()
        # TODO(#26138): Print out which exports will run when we hydrate that

        print(
            "\n~~~~~~~~~~~~~~~~~~~~~~ [STARTING SANDBOX LOAD] ~~~~~~~~~~~~~~~~~~~~~~\n"
        )

        if prompt_for_step(
            "Will upload Docker image with Dataflow pipeline code changes."
        ):
            push_sandbox_dataflow_pipeline_docker_image(
                project_id=project_id, sandbox_username=sandbox_username
            )

        if prompt_for_step(
            "Will create sandbox datasets for Dataflow pipelines output."
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix=output_sandbox_prefix,
                pipelines=[
                    INGEST_PIPELINE_NAME,
                    METRICS_PIPELINE_NAME,
                    SUPPLEMENTAL_PIPELINE_NAME,
                ],
                allow_overwrite=True,
                state_code_filter=state_code,
            )

        if prompt_for_step("Will start the sandbox ingest Dataflow pipeline."):
            run_sandbox_dataflow_pipeline(ingest_pipeline_params, skip_build=True)

            while not prompt_for_confirmation(
                "The ingest pipeline will take some time to run. Has it completed?",
                exit_on_cancel=False,
            ):
                continue

        if post_ingest_pipeline_params:
            if prompt_for_step(
                "Will run the sandbox metric/supplemental Dataflow pipelines."
            ):
                for params in post_ingest_pipeline_params:
                    run_sandbox_dataflow_pipeline(params, skip_build=True)

                while not prompt_for_confirmation(
                    "The metric/supplemental pipeline(s) will take some time to run. "
                    "Have they completed?",
                    exit_on_cancel=False,
                ):
                    continue

        if prompt_for_step(
            f"Will load the [{len(view_builders_to_load)}] collected BigQuery views to "
            f"a sandbox."
        ):
            load_collected_views_to_sandbox(
                sandbox_dataset_prefix=output_sandbox_prefix,
                state_code_filter=state_code,
                input_source_table_dataset_overrides_dict=(
                    view_update_input_dataset_overrides_dict
                ),
                allow_slow_views=True,
                collected_builders=view_builders_to_load,
            )

        # TODO(#26138): Add a prompt when we're ready to hydrate these
        for _export_config in exports_for_views:
            raise NotImplementedError(
                "#26138: No support yet for running metric exports"
            )
        print("\n~~~~~~~~~~~~~~~~~~~~~~ [COMPLETE!] ~~~~~~~~~~~~~~~~~~~~~~\n")


def parse_arguments() -> argparse.Namespace:
    """Parses the script arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        required=False,
    )
    parser.add_argument(
        "--state_code",
        help="The state code to load this sandbox for",
        type=StateCode,
        required=True,
    )
    parser.add_argument(
        "--sandbox_prefix",
        dest="sandbox_prefix",
        help="A prefix to append to all names of all output datasets and Dataflow "
        "pipeline names",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--raw_data_source_instance",
        help="The raw data instance the ingest should read from from",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=False,
        default=DirectIngestInstance.PRIMARY,
    )
    view_load_type_group = parser.add_mutually_exclusive_group(required=True)
    view_load_type_group.add_argument(
        "--load_up_to_addresses",
        dest="load_up_to_addresses",
        help=(
            "If provided, the sandbox load will stop after all of these views have "
            "been loaded. Views that are only descendants of these views will not be "
            "loaded."
        ),
        type=str_to_address_list,
    )

    view_load_type_group.add_argument(
        "--load_up_to_datasets",
        dest="load_up_to_datasets",
        help=(
            "If provided, the sandbox load will stop after all of these views in these "
            "datasets have been loaded. Views that are only descendants of the views "
            "in these datasets will not be loaded."
        ),
        type=str_to_list,
    )
    ignored_changes_type_group = parser.add_mutually_exclusive_group(required=False)
    ignored_changes_type_group.add_argument(
        "--changed_datasets_to_ignore",
        dest="changed_datasets_to_ignore",
        help="A list of dataset ids (comma-separated) for datasets we should skip when "
        "detecting which views have changed. Views in these datasets will still "
        "be loaded to the sandbox if they are downstream of other views not in these "
        "datasets which have been changed. This argument cannot be used if "
        "--changed_datasets_to_include is set.",
        type=str_to_list,
        required=False,
    )

    ignored_changes_type_group.add_argument(
        "--changed_datasets_to_include",
        dest="changed_datasets_to_include",
        help="A list of dataset ids (comma-separated) for datasets we should consider "
        "when detecting which views have changed. Views outside of these datasets will "
        "still be loaded to the sandbox if they are downstream of other changed views "
        "in these datasets. This argument cannot be used if "
        "--changed_datasets_to_ignore is set.",
        type=str_to_list,
        required=False,
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()
    load_end_to_end_sandbox(
        project_id=args.project_id,
        state_code=args.state_code,
        output_sandbox_prefix=args.sandbox_prefix,
        raw_data_source_instance=args.raw_data_source_instance,
        load_up_to_addresses=args.load_up_to_addresses,
        load_up_to_datasets=args.load_up_to_datasets,
        changed_datasets_to_include=args.changed_datasets_to_include,
        changed_datasets_to_ignore=args.changed_datasets_to_ignore,
    )
