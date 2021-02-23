# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

Invoking this script deploys calculation pipelines in parallel.

Run the following command to execute from the command-line:

    python -m recidiviz.tools.deploy.deploy_pipeline_templates
        --templates_to_deploy [TEMPLATES_TO_DEPLOY]
        --project_id [PROJECT_ID]
"""
import argparse
import logging
import os
import sys
from concurrent import futures
from typing import List, Tuple, Any

import yaml
from progress.bar import Bar

from recidiviz.calculator import pipeline
from recidiviz.tools.run_calculation_pipelines import run_pipeline, get_pipeline_module
from recidiviz.utils import structured_logging

STAGING_ONLY_TEMPLATES_PATH = os.path.join(os.path.dirname(pipeline.__file__),
                                           'staging_only_calculation_pipeline_templates.yaml')


PRODUCTION_TEMPLATES_PATH = os.path.join(os.path.dirname(pipeline.__file__),
                                         'production_calculation_pipeline_templates.yaml')

TEMPLATE_PATHS = {
    'production': PRODUCTION_TEMPLATES_PATH,
    'staging': STAGING_ONLY_TEMPLATES_PATH
}


def deploy_pipeline_templates(template_yaml_path: str, project_id: str) -> None:
    """Deploys all pipelines listed in the file at the template_yaml_path to templates in the given project."""
    logging.info("Deploying pipeline templates at %s to %s", template_yaml_path, project_id)

    templates_and_args: List[Tuple[Any, List[str]]] = []

    with open(template_yaml_path, 'r') as yaml_file:
        pipeline_config_yaml = yaml.full_load(yaml_file)

        if pipeline_config_yaml:
            pipeline_config_yaml_all_pipelines = pipeline_config_yaml['daily_pipelines'] + \
                pipeline_config_yaml['historical_pipelines']
            for pipeline_yaml_dict in pipeline_config_yaml_all_pipelines:
                argv = ['--project', project_id,
                        '--save_as_template']

                pipeline_type = ''

                for key, value in pipeline_yaml_dict.items():
                    if key == 'pipeline':
                        pipeline_type = value
                    elif key == 'metric_types':  # The only arg that allows a list
                        argv.extend([f'--{key}'])
                        metric_type_values = value.split(' ')

                        for metric_type_value in metric_type_values:
                            argv.extend([metric_type_value])
                    else:
                        argv.extend([f'--{key}', f'{value}'])

                pipeline_module = get_pipeline_module(pipeline_type)
                templates_and_args.append((pipeline_module, argv))
        else:
            logging.info("Empty pipeline yaml dict at: %s", yaml_file)

    if not templates_and_args:
        logging.info("No pipeline templates to deploy")
        return

    _do_parallel_deploy(templates_and_args)


def _do_parallel_deploy(templates_and_args: List[Tuple[Any, List[str]]]) -> None:
    """Deploys the given pipeline templates and their execution args in parallel."""
    progress_bar = Bar("Deploying calculation pipeline templates...", max=len(templates_and_args))

    with futures.ThreadPoolExecutor() as executor:
        future_to_templates = {
            executor.submit(structured_logging.with_context(_deploy_template),
                            template_with_args): template_with_args
            for template_with_args in templates_and_args
        }

        for future in futures.as_completed(future_to_templates):
            template_with_args = future_to_templates[future]
            logging.info('Deployed calculation pipeline template [%s]', template_with_args)

            # Absolutely bizarre that pylint calls this not callable, but it definitely is and definitely works
            # pylint:disable=not-callable
            progress_bar.next()

    progress_bar.finish()


def _deploy_template(template_and_args: Tuple[Any, List[str]]) -> None:
    """Deploys a single pipeline template and its execution args."""
    pipeline_module, argv = template_and_args
    run_pipeline(pipeline_module, argv)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to deploy the pipeline templates."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--templates_to_deploy',
                        dest='templates_to_deploy',
                        type=str,
                        choices=TEMPLATE_PATHS.keys(),
                        required=True)

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=['recidiviz-123', 'recidiviz-staging'],
                        required=True)

    return parser.parse_known_args(argv)


def deploy_pipeline_templates_to_project() -> None:
    """Deploys either prod or staging pipelines to the project given by the --project_id argument."""
    known_args, _ = parse_arguments(sys.argv)

    template_yaml_path = TEMPLATE_PATHS.get(known_args.templates_to_deploy)

    if template_yaml_path:
        deploy_pipeline_templates(template_yaml_path=template_yaml_path, project_id=known_args.project_id)
    else:
        # Bad arg should be caught by the arg parser before we get here
        raise ValueError(f"No template yaml file corresponding to --templates_to_deploy="
                         f"{known_args.templates_to_deploy}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    deploy_pipeline_templates_to_project()
