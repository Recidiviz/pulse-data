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
"""Triggers a Dataflow pipeline from a deployed template. Should be used only on rare
occasions when it is necessary to trigger a single Dataflow job without triggering the entire
calculation pipeline DAG.

For a list of deployed templates, see production_calculation_pipeline_templates.yaml.

usage: python -m recidiviz.tools.calculator.trigger_deployed_pipeline_template \
        --project_id [PROJECT_ID] \
        --template_name [TEMPLATE_NAME]
"""
from __future__ import absolute_import

import argparse
import logging
import sys
from typing import Any, Dict, List, Tuple

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from recidiviz.calculator.dataflow_config import PRODUCTION_TEMPLATES_PATH
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.yaml_dict import YAMLDict


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to trigger the Dataflow pipeline template."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--template_name",
        dest="template_name",
        type=str,
        help="The name of the pipeline template to be run.",
        required=True,
    )

    return parser.parse_known_args(argv)


def _trigger_dataflow_job_from_template(
    project_id: str, bucket: str, template: str, job_name: str, location: str
) -> Dict[str, Any]:
    """Trigger the Dataflow job at the given template location and execute it
    with the given `job_name`."""
    credentials = GoogleCredentials.get_application_default()
    service = build("dataflow", "v1b3", credentials=credentials)

    body = {
        "jobName": f"{job_name}",
        "gcsPath": f"gs://{bucket}/templates/{template}",
        "environment": {
            "tempLocation": f"gs://{bucket}/temp",
        },
    }

    request = (
        service.projects()
        .locations()
        .templates()
        .create(projectId=project_id, body=body, location=location)
    )
    response = request.execute()
    return response


def _pipeline_regions_by_job_name() -> Dict[str, str]:
    """Parses the production_calculation_pipeline_templates.yaml config file to determine
    which region a pipeline should be run in."""
    incremental_pipelines = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH).pop_dicts(
        "incremental_pipelines"
    )
    historical_pipelines = YAMLDict.from_path(PRODUCTION_TEMPLATES_PATH).pop_dicts(
        "historical_pipelines"
    )

    pipeline_regions = {
        pipeline.pop("job_name", str): pipeline.pop("region", str)
        for pipeline in incremental_pipelines
    }

    pipeline_regions.update(
        {
            pipeline.pop("job_name", str): pipeline.pop("region", str)
            for pipeline in historical_pipelines
        }
    )

    return pipeline_regions


def trigger_dataflow_template() -> None:
    """Triggers a Dataflow job from a deployed pipeline template."""
    known_args, _ = parse_arguments(sys.argv[1:])

    template_name = known_args.template_name
    job_name = known_args.template_name

    pipeline_regions = _pipeline_regions_by_job_name()

    if template_name not in pipeline_regions:
        raise ValueError(
            f"Trying to trigger a pipeline for a template that does not exist: [{template_name}]"
        )

    location = pipeline_regions.get(known_args.template_name)

    if not location:
        # This should never happen
        raise ValueError(
            f"No defined region in {PRODUCTION_TEMPLATES_PATH} for job_name {job_name}."
        )

    response = _trigger_dataflow_job_from_template(
        project_id=known_args.project_id,
        bucket=f"{known_args.project_id}-dataflow-templates",
        template=template_name,
        job_name=job_name,
        location=location,
    )

    logging.info("Response: %s", response)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    trigger_dataflow_template()
