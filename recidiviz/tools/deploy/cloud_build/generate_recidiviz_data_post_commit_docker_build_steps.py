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
"""Generates the Cloud Build steps YAML for the pulse-data post-commit
Docker image builds.

These are the Docker build steps that run in staging on every commit to the
`main` branch (and release branches), triggered by the
staging_release_build_trigger Terraform resource in build-trigger.tf. They build
the Docker images used by App Engine, Cloud Run, GKE, and the admin panel — but
NOT Dataflow images, which are built separately and only run in GCP.

Run this script whenever build_images.py or related constants change, then
commit the updated YAML file:

    python -m recidiviz.tools.deploy.cloud_build.generate_recidiviz_data_post_commit_docker_build_steps
"""
import argparse
import os
from typing import Any

import attrs
import yaml
from google.cloud.devtools.cloudbuild_v1 import BuildOptions

import recidiviz
from recidiviz.tools.deploy.cloud_build.build_configuration import (
    DeploymentContext,
    build_config_to_dict,
)
from recidiviz.tools.deploy.cloud_build.constants import (
    IMAGE_BUILD_PLATFORMS,
    PLATFORM_LINUX_ARM64,
)
from recidiviz.tools.deploy.cloud_build.stages.build_images import BuildImages

# Image kinds built by the staging release trigger. These are the images that
# include arm64 builds — i.e. the ones that are pulled and run locally on
# developer machines. Images that only target amd64 (e.g. Dataflow) are built
# separately and only run in GCP.
TRIGGER_IMAGE_KINDS = [
    image_kind
    for image_kind, platforms in IMAGE_BUILD_PLATFORMS.items()
    if PLATFORM_LINUX_ARM64 in platforms
]

# Use Cloud Build built-in substitution variables as placeholders. These are
# resolved at build time by Cloud Build, not at Terraform plan time.
_DEPLOYMENT_CONTEXT = DeploymentContext(
    project_id="$PROJECT_ID",
    commit_ref="$COMMIT_SHA",
    version_tag="$COMMIT_SHA",
    stage="BuildImages",
)

OUTPUT_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/deploy/terraform/config/recidiviz_data_post_commit_docker_build_steps.yaml",
)


# Fields from the BuildStep proto that are response-only and should not appear
# in the generated config.
_STEP_RESPONSE_ONLY_FIELDS = {
    "status",
    "exit_code",
    "allow_exit_codes",
    "allow_failure",
    "script",
    "automap_substitutions",
}


def _strip_defaults(value: Any) -> Any:
    """Recursively remove fields with default proto values and response-only
    step fields, so the generated YAML only contains meaningful config."""
    if isinstance(value, dict):
        cleaned = {
            k: _strip_defaults(v)
            for k, v in value.items()
            if v not in ("", "0", [], None, 0, False, {})
            and k not in _STEP_RESPONSE_ONLY_FIELDS
        }
        # Drop keys whose values became empty dicts after recursion
        return {k: v for k, v in cleaned.items() if v != {}}
    if isinstance(value, list):
        return [_strip_defaults(item) for item in value]
    return value


def generate() -> dict:
    """Generate the post-commit Docker build config for the staging release
    trigger. Returns the full Cloud Build configuration dict with default and
    response-only fields stripped."""
    build_configuration = BuildImages().configure_build(
        deployment_context=_DEPLOYMENT_CONTEXT,
        args=argparse.Namespace(
            images=TRIGGER_IMAGE_KINDS,
            # This gives each branch its own cache tag so main and
            # release branches don't thrash each other's cache. Cloud Build substitutes
            # $BRANCH_NAME at trigger fire time and build_images.py sanitizes to
            # Docker-tag-safe chars in the shell command.
            cache_scope_key="$BRANCH_NAME",
        ),
    )
    # Match the timeout historically set on the staging release trigger's HCL.
    # The BuildConfiguration default (1800s) still applies to deployment-runner
    # invocations of BuildImages.
    build_configuration = attrs.evolve(build_configuration, timeout_seconds=3600)
    config_dict = build_config_to_dict(
        build_configuration=build_configuration,
        deployment_context=_DEPLOYMENT_CONTEXT,
        # The trigger handles authentication via its own service account
        # configuration in Terraform, not via the build config.
        service_account=None,
    )
    config_dict = _strip_defaults(config_dict)
    # The proto enum serializes as an integer; convert to the string name
    # that Terraform expects.
    config_dict["options"]["machine_type"] = BuildOptions.MachineType(
        config_dict["options"]["machine_type"]
    ).name
    return config_dict


_HEADER_COMMENT = (
    "# Auto-generated by generate_recidiviz_data_post_commit_docker_build_steps.py.\n"
    "# Do not edit manually. To regenerate, run:\n"
    "#   python -m recidiviz.tools.deploy.cloud_build"
    ".generate_recidiviz_data_post_commit_docker_build_steps\n"
)


class _PrettierYamlDumper(yaml.Dumper):
    """YAML dumper that indents list items under their parent key, matching
    prettier's formatting expectations."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        return super().increase_indent(flow, indentless=False)


if __name__ == "__main__":
    output = generate()
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(_HEADER_COMMENT)
        yaml.dump(
            output,
            f,
            Dumper=_PrettierYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )
    print(f"Wrote {OUTPUT_PATH}")
