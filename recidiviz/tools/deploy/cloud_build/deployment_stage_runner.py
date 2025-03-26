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
"""CLI for running particular build stages of deployments

This script currently relies on bash for:
- ensuring the correct sequencing of build stages
- generation of the version tag

python -m recidiviz.tools.deploy.cloud_build.deployment_stage_runner \
    --project-id {recidiviz-staging|recidiviz-123} \
    --commit-ref {commit_ref_to_deploy} \
    --version-tag {version_tag} \
    --stage {RunMigrations|TagImages|BuildImages}

"""
import argparse
import logging
import re
import sys

import proto
import yaml

from recidiviz.tools.deploy.cloud_build.build_configuration import (
    DeploymentContext,
    create_deployment_build_api_obj,
)
from recidiviz.tools.deploy.cloud_build.cloud_build_client import CloudBuildClient
from recidiviz.tools.deploy.cloud_build.deployment_stage_interface import (
    DeploymentStageInterface,
)
from recidiviz.tools.deploy.cloud_build.stages.build_images import BuildImages
from recidiviz.tools.deploy.cloud_build.stages.create_terraform_plan import (
    CreateTerraformPlan,
)
from recidiviz.tools.deploy.cloud_build.stages.run_migrations_from_cloud_build import (
    RunMigrations,
)
from recidiviz.tools.deploy.cloud_build.stages.tag_images import TagImages
from recidiviz.tools.utils.script_helpers import interactive_prompt_retry_on_exception
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

AVAILABLE_DEPLOYMENT_STAGES: set[type[DeploymentStageInterface]] = {
    RunMigrations,
    TagImages,
    BuildImages,
    CreateTerraformPlan,
}


def get_deployment_stage_name(entrypoint: type[DeploymentStageInterface]) -> str:
    return entrypoint.__name__.split(".")[-1]


DEPLOYMENT_STAGES_BY_NAME = {
    get_deployment_stage_name(deployment_stage): deployment_stage
    for deployment_stage in AVAILABLE_DEPLOYMENT_STAGES
}


def parse_arguments(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """Parses arguments for the Cloud SQL to BQ refresh process."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--commit-ref",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--version-tag",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--stage",
        required=True,
        type=str,
        choices=list(DEPLOYMENT_STAGES_BY_NAME.keys()),
    )
    parser.add_argument(
        "--dry-run",
        required=False,
        action="store_true",
        default=False,
    )
    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parsed_args, unknown_args = parse_arguments(sys.argv[1:])
    deployment_context = DeploymentContext.build_from_argparse(parsed_args)

    with local_project_id_override(parsed_args.project_id):
        cloud_build_client = CloudBuildClient.build(
            location=CloudBuildClient.DEFAULT_LOCATION
        )

        deployment_stage_cls = DEPLOYMENT_STAGES_BY_NAME[parsed_args.stage]
        deployment_stage = deployment_stage_cls()
        deployment_stage_args = deployment_stage_cls.get_parser().parse_args(
            unknown_args
        )

        if parsed_args.dry_run:
            yaml_output = yaml.dump(
                proto.Message.to_dict(  # type: ignore[attr-defined]
                    create_deployment_build_api_obj(
                        build_configuration=deployment_stage_cls().configure_build(
                            deployment_context=deployment_context,
                            args=deployment_stage_args,
                        ),
                        deployment_context=deployment_context,
                    )
                )
            )
            # Some protobuf fields are suffixed with `_` to avoid collision with Python global names ie `dir_` vs `dir`
            # Replace the field name with the non-suffixed key
            yaml_output = re.sub(r"(\s+.+)_(:.+)", r"\g<1>\g<2>", yaml_output)
            logging.info(
                "Command generated the following build configuration: %s", yaml_output
            )

            sys.exit()

        deployment_build = interactive_prompt_retry_on_exception(
            fn=lambda: cloud_build_client.run_build(
                create_deployment_build_api_obj(
                    build_configuration=deployment_stage_cls().configure_build(
                        deployment_context=deployment_context,
                        args=deployment_stage_args,
                    ),
                    deployment_context=deployment_context,
                )
            ),
            input_text="An error occurred while running the build. See logs for more details.\n"
            "Would you like to retry this step?",
        )
