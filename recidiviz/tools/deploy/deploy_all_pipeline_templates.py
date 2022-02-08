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
"""Functions for deploying calculation pipelines to templates.

Run the following command to execute from the command-line:

    python -m recidiviz.tools.deploy.deploy_all_pipeline_templates --project_id recidiviz-staging
"""
import argparse
import logging
import os
import subprocess
import sys
from typing import List, Tuple

import recidiviz
from recidiviz.calculator.dataflow_config import PIPELINE_CONFIG_YAML_PATH
from recidiviz.tools.deploy.build_dataflow_source_distribution import (
    build_source_distribution,
)
from recidiviz.tools.deploy.dataflow_template_helpers import load_pipeline_config_yaml
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.future_executor import FutureExecutor

RECIDIVIZ_ROOT = os.path.abspath(os.path.join(recidiviz.__file__, "../.."))
DEPLOY_ROOT = os.path.join(RECIDIVIZ_ROOT, "recidiviz/tools/deploy")
DEPLOY_LOG_DIRECTORY = os.path.join(DEPLOY_ROOT, "log")
DEPLOY_PIPELINE_TEMPLATES_LOG = (
    f"{DEPLOY_LOG_DIRECTORY}/deploy_all_pipeline_templates.log"
)


def build_process_log(process: subprocess.CompletedProcess) -> str:
    return "\n".join(
        [
            "=====" * 20,
            "COMMAND:",
            str(process.args),
            "STDOUT:",
            f"{process.stdout.decode('utf-8')}",
            "STDERR:",
            f"{process.stderr.decode('utf-8')}",
            "",
        ]
    )


def write_process_log(process: subprocess.CompletedProcess) -> None:
    if not os.path.exists(DEPLOY_LOG_DIRECTORY):
        os.mkdir(DEPLOY_LOG_DIRECTORY)

    with open(DEPLOY_PIPELINE_TEMPLATES_LOG, "a+", encoding="utf-8") as f:
        f.writelines(build_process_log(process))


def find_and_deploy_single_pipeline_template(
    project_id: str, source_distribution: str, job_name: str
) -> None:
    """Spawns a process to deploy a single pipeline template. Writes failure log if necessary"""
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "recidiviz.tools.deploy.deploy_single_pipeline_template",
            "--project_id",
            project_id,
            "--source_distribution",
            source_distribution,
            "--job_name",
            job_name,
        ],
        capture_output=True,
        # We manually handle errors below
        check=False,
    )

    if process.returncode != 0:
        write_process_log(process)

        print(f"!! Failed to deploy {job_name} pipeline template")

        raise DeployPipelineFailedError()


class DeployPipelineFailedError(ValueError):
    pass


def deploy_pipeline_templates(template_yaml_path: str, project_id: str) -> None:
    """Deploys all pipelines listed in the given config yaml to the given project."""
    logging.info("Deploying pipeline templates to %s", project_id)

    pipeline_config_yaml = load_pipeline_config_yaml(template_yaml_path)
    source_distribution = build_source_distribution()

    deploy_pipeline_kwargs = [
        {
            "project_id": project_id,
            "source_distribution": source_distribution,
            "job_name": pipeline["job_name"],
        }
        for pipeline in pipeline_config_yaml.all_pipelines
        if project_id == GCP_PROJECT_STAGING or not pipeline.get("staging_only")
    ]

    try:
        with FutureExecutor.build(
            find_and_deploy_single_pipeline_template,
            deploy_pipeline_kwargs,
            max_workers=8,
        ) as execution:
            execution.wait_with_progress_bar(
                "Deploying pipeline templates...", timeout=20 * 60
            )
    except DeployPipelineFailedError:
        sys.exit(1)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to deploy the pipeline templates."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=["recidiviz-123", "recidiviz-staging"],
        required=True,
    )

    return parser.parse_known_args(argv)


def deploy_pipeline_templates_to_project() -> None:
    """Deploys pipelines to the project given by the --project_id argument. Whether the pipeline is deployed to
    staging or production is determined by the `staging_only` attribute in the pipeline config."""
    known_args, _ = parse_arguments(sys.argv)

    deploy_pipeline_templates(
        template_yaml_path=PIPELINE_CONFIG_YAML_PATH, project_id=known_args.project_id
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    deploy_pipeline_templates_to_project()
