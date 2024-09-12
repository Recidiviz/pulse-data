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
"""CLI tool for adding / editing a Terraform plan PR comment

python -m recidiviz.tools.github.upsert_terraform_plan \
    --pull-request-number [pr-number] \
    --commit-ref [commit-ref] \
    --terraform-plan-output-path [file containing output of `terraform_show`]
"""
import argparse
import logging
import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.github import upsert_helperbot_comment
from recidiviz.utils.metadata import local_project_id_override


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pull-request-number", type=int, required=True)
    parser.add_argument("--commit-ref", type=str, required=True)
    parser.add_argument(
        "--terraform-plan-output-path",
        type=str,
        required=True,
        help="Path to the Terraform plan contents",
    )
    return parser


def main(args: argparse.Namespace) -> None:
    with open(args.terraform_plan_output_path, mode="r", encoding="utf-8") as file:
        plan_output = file.read()

    # This jinja renderer does not render html to be served to clients, so disabling the `autoescape` B701 security rule
    env = Environment(
        loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )  # nosec B701

    template = env.get_template("terraform_plan.md.jinja2")

    body = template.render(
        {
            "terraform_plan_output": plan_output,
            "commit_ref": args.commit_ref,
            "generated_on": datetime.now().isoformat(),
        }
    )

    upsert_helperbot_comment(
        pull_request_number=args.pull_request_number,
        body=body,
        prefix="# Terraform plan",
    )

    logging.info(body)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        main(get_parser().parse_args())
