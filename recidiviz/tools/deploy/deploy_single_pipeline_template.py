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
"""
Functions for deploying a single calculation pipeline.
The contents of this Python interpreter session are pickled and re-hydrated into the Dataflow worker process.

The script finds and deploys a template that are defined in our configs:
- recidiviz/calculator/pipeline/staging_only_calculation_pipelines.yaml
- recidiviz/calculator/pipeline/production_calculation_pipelines.yaml

For example, to deploy the `us-id-historical-incarceration-calculations-240` template:

python -m recidiviz.tools.deploy.deploy_single_pipeline_template \
    --project_id recidiviz-staging \
    --job_name us-id-historical-incarceration-calculations-240
"""

import argparse
import logging
import sys
from typing import List, Optional, Tuple

from recidiviz.tools.deploy.build_dataflow_source_distribution import (
    build_source_distribution,
)
from recidiviz.tools.deploy.dataflow_template_helpers import (
    PIPELINE_CONFIG_YAML_PATHS,
    PipelineConfig,
    load_pipeline_config_yaml,
)
from recidiviz.tools.pipeline_launch_util import load_all_pipelines, run_pipeline


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to deploy a pipeline template."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        required=True,
        help="The project to deploy the templates to",
        choices=["recidiviz-staging", "recidiviz-123"],
    )

    parser.add_argument(
        "--source_distribution",
        dest="source_distribution",
        default=None,
        help="The path to a built `recidiviz-calculation-pipelines` source distribution. This can be built by running "
        "`python -m recidiviz.tools.deploy.recidiviz.tools.deploy.build_dataflow_source_distribution`",
    )

    parser.add_argument(
        "--job_name",
        dest="job_name",
        required=True,
        help="The name of the job as specified in the pipeline config yaml",
    )

    return parser.parse_known_args(argv)


def build_arguments_for_pipeline(
    project_id: str, source_distribution: Optional[str], pipeline_config: PipelineConfig
) -> List[str]:
    """ Builds the list of argv args to pass to Beam when deploying a dataflow template"""
    pipeline_args = [
        "--project",
        project_id,
        "--extra_package",
        source_distribution or build_source_distribution(),
        "--save_as_template",
    ]

    for key, value in pipeline_config.items():
        if key == "pipeline":
            pass
        elif key == "metric_types" and isinstance(
            value, str
        ):  # The only arg that allows a list
            pipeline_args.extend([f"--{key}"])
            metric_type_values = value.split(" ")

            for metric_type_value in metric_type_values:
                pipeline_args.extend([metric_type_value])
        else:
            pipeline_args.extend([f"--{key}", f"{value}"])

    return pipeline_args


def find_pipeline_with_job_name(job_name: str) -> PipelineConfig:
    for template_yaml_path in PIPELINE_CONFIG_YAML_PATHS.values():
        pipeline_config_yaml = load_pipeline_config_yaml(template_yaml_path)

        for pipeline in pipeline_config_yaml.all_pipelines:
            if pipeline["job_name"] == job_name:
                return pipeline

    raise ValueError(f"Can not find a configured pipeline with the name {job_name}")


def deploy_pipeline_template_to_project() -> None:
    """ Finds an runs a pipeline with the specified arguments"""
    known_args, _ = parse_arguments(sys.argv)

    pipeline_to_deploy_config = find_pipeline_with_job_name(known_args.job_name)

    pipeline_args = build_arguments_for_pipeline(
        known_args.project_id, known_args.source_distribution, pipeline_to_deploy_config
    )

    load_all_pipelines()
    pipeline_module_name = pipeline_to_deploy_config["pipeline"]
    # Runs the pipeline with the arguments needed for deployment
    run_pipeline(pipeline_module_name, pipeline_args)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    deploy_pipeline_template_to_project()
