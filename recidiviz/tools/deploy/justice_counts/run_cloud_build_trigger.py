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
Script to run the Build-JC Cloud Build Trigger, which builds a Docker image
containing both Publisher and Agency Dashboard. Generally meant to be called 
from the Justice Counts deploy_to_staging script.
You need to specify:
- version of the backend code to use (either the name of a branch or tag)
- version of the frontend code to use (either the name of a branch or tag)
- subdirectory in which to place the built Docker image

Example usage (run from `pipenv shell`):
python -m recidiviz.tools.deploy.justice_counts.run_cloud_build_trigger \
    --backend-tag jc.publisher.v1.0.0 \
    --frontend-tag publisher.v1.0.0 \
    --subdirectory justice-counts

python -m recidiviz.tools.deploy.justice_counts.run_cloud_build_trigger \
    --backend-branch main \
    --frontend-branch testing-out \
    --subdirectory justice-counts/playtesting
"""
import argparse
import logging
from typing import Optional

# using a from import causes mypy errors
# pylint: disable=consider-using-from-import
import google.cloud.devtools.cloudbuild_v1 as cloudbuild_v1

from recidiviz.utils.metadata import local_project_id_override

PROJECT_ID = "recidiviz-staging"
CLOUD_BUILD_TRIGGER = "Build-JC"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Deploy the Justice Counts Publisher application to staging Cloud Run."
    )

    backend_group = parser.add_mutually_exclusive_group(required=True)
    backend_group.add_argument(
        "--backend-tag", help="Name of the backend tag to deploy."
    )
    backend_group.add_argument(
        "--backend-branch", help="Name of the backend branch to deploy."
    )

    frontend_group = parser.add_mutually_exclusive_group(required=True)
    frontend_group.add_argument(
        "--frontend-tag", help="Name of the frontend tag to deploy."
    )
    frontend_group.add_argument(
        "--frontend-branch-or-sha",
        help="Name of the frontend branch or commit sha to deploy.",
    )

    parser.add_argument(
        "--subdirectory",
        help="Name of the subdirectory in Container Registry in which to place the built image.",
        required=True,
    )
    return parser


def main(
    backend_tag: Optional[str],
    backend_branch: Optional[str],
    frontend_tag: Optional[str],
    frontend_branch_or_sha: Optional[str],
    subdirectory: str,
) -> None:
    """Run the Justice Counts Build-JC Cloud Build Trigger."""
    if frontend_branch_or_sha:
        frontend_url = f"https://github.com/Recidiviz/justice-counts/archive/{frontend_branch_or_sha}.tar.gz"
    elif frontend_tag:
        frontend_url = f"https://github.com/Recidiviz/justice-counts/archive/refs/tags/{frontend_tag}.tar.gz"
    else:
        raise ValueError(
            "Must specify either the `frontend_branch_or_sha` or `frontend_tag` argument."
        )

    substitutions = {
        "_FRONTEND_URL": frontend_url,
        "_SUBDIRECTORY": subdirectory,
    }

    if backend_branch:
        repo_source = cloudbuild_v1.RepoSource(
            branch_name=backend_branch, substitutions=substitutions
        )
    elif backend_tag:
        repo_source = cloudbuild_v1.RepoSource(
            tag_name=backend_tag, substitutions=substitutions
        )
    else:
        raise ValueError(
            "Must specify either the `backend_branch` or `backend_tag` argument."
        )

    request = cloudbuild_v1.RunBuildTriggerRequest(
        project_id=PROJECT_ID,
        trigger_id=CLOUD_BUILD_TRIGGER,
        source=repo_source,
    )

    logging.info(
        "Submitting request to Cloud Build Trigger %s in %s with the substitutions %s...",
        CLOUD_BUILD_TRIGGER,
        PROJECT_ID,
        substitutions,
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
            backend_tag=args.backend_tag,
            backend_branch=args.backend_branch,
            frontend_tag=args.frontend_tag,
            frontend_branch_or_sha=args.frontend_branch_or_sha,
            subdirectory=args.subdirectory,
        )
