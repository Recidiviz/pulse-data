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
"""Helpers to make it easy to interact with Terraform from python"""
import subprocess


def terraform_import(resource_addr: str, resource_id: str) -> None:
    subprocess.run(
        f"""
        terraform -chdir=./recidiviz/tools/deploy/terraform import \
        -var=project_id="" \
        -var=docker_image_tag="" \
        -var=git_hash="" \
        {resource_addr} {resource_id}
        """,
        shell=True,
        check=True,  # TODO(#8842): Consider removing this and returning an error instead to make it easier to resume if a run gets interrupted
    )
