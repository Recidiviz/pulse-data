# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Script to run the build-prototypes-server Cloud Build Trigger, which builds a Docker
image containing the Prototypes endpoints.
You need to specify:
- version of the code to use (either the name of a branch or tag)

Example usage (run from `pipenv shell`):
python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-tag {tag_name}

python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-branch {branch_name}

"""
import argparse
import logging
from typing import Optional

# using a from import causes mypy errors
# pylint: disable=consider-using-from-import
import google.cloud.devtools.cloudbuild_v1 as cloudbuild_v1

from recidiviz.utils.metadata import local_project_id_override

PROJECT_ID = "recidiviz-staging"
CLOUD_BUILD_TRIGGER = "build-prototypes-server"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Deploy the Prototypes application to staging Cloud Run."
    )

    version_group = parser.add_mutually_exclusive_group(required=True)
    version_group.add_argument(
        "--version-tag", help="Name of the version tag to deploy."
    )
    version_group.add_argument(
        "--version-branch", help="Name of the version branch to deploy."
    )
    return parser


def main(
    version_tag: Optional[str],
    version_branch: Optional[str],
) -> None:
    if version_branch:
        repo_source = cloudbuild_v1.RepoSource(branch_name=version_branch)
    elif version_tag:
        repo_source = cloudbuild_v1.RepoSource(tag_name=version_tag)
    else:
        raise ValueError(
            "Must specify either the `version-branch` or `version-tag` argument."
        )

    request = cloudbuild_v1.RunBuildTriggerRequest(
        project_id=PROJECT_ID,
        trigger_id=CLOUD_BUILD_TRIGGER,
        source=repo_source,
    )

    logging.info(
        "Submitting request to Cloud Build Trigger %s in %s...",
        CLOUD_BUILD_TRIGGER,
        PROJECT_ID,
    )
    client = cloudbuild_v1.CloudBuildClient()
    operation = client.run_build_trigger(request=request)
    response = operation.result()
    logging.info("Cloud Build Trigger job finished with status %s.", response.status)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(PROJECT_ID):
        main(
            version_tag=args.version_tag,
            version_branch=args.version_branch,
        )
