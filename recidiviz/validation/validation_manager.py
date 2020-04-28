# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Contains the API for automated data validation."""

from http import HTTPStatus
import logging
from typing import List, Dict, Any

from opencensus.stats import aggregation, measure, view

import flask

from recidiviz.utils import monitoring
from recidiviz.utils.auth import authenticate_request
from recidiviz.validation.checks.check_resolver import checker_for_validation

from recidiviz.validation.configured_validations import get_all_validations, STATES_TO_VALIDATE
from recidiviz.validation.validation_models import DataValidationJob, DataValidationJobResult

m_failed_validations = measure.MeasureInt("validation/num_failures", "The number of failed validations", "1")

failed_validations_view = view.View("recidiviz/validation/num_failures",
                                    "The sum of failed validations",
                                    [monitoring.TagKey.REGION, monitoring.TagKey.VALIDATION_CHECK_TYPE],
                                    m_failed_validations,
                                    aggregation.SumAggregation())

monitoring.register_views([failed_validations_view])


validation_manager_blueprint = flask.Blueprint('validation_manager', __name__)


@validation_manager_blueprint.route('/validate', methods=['POST'])
@authenticate_request
def handle_validation_request():
    """API endpoint to service data validation requests."""

    # Fetch collection of validation jobs to perform
    validation_jobs = _fetch_validation_jobs_to_perform()
    logging.info('Performing a total of %s validation jobs...', len(validation_jobs))

    # Perform all validations and track failures
    failed_validations: List[DataValidationJobResult] = []
    for job in validation_jobs:
        validation_checker = checker_for_validation(job)
        result = validation_checker.run_check(job)
        if not result.was_successful:
            failed_validations.append(result)

    if failed_validations:
        logging.error('Found a total of %s failures. Emitting results...', len(failed_validations))
    else:
        logging.info('Found no failed validations...')

    # Emit metrics for all failures
    _emit_failures(failed_validations)

    logging.info('Validation run complete. Analyzed a total of %s jobs.', len(validation_jobs))
    return f"Validation failures identified: {len(failed_validations) > 0}", HTTPStatus.OK


def _fetch_validation_jobs_to_perform() -> List[DataValidationJob]:
    validation_checks = get_all_validations()

    validation_jobs: List[DataValidationJob] = []
    for check in validation_checks:
        for state_code in STATES_TO_VALIDATE:
            validation_jobs.append(DataValidationJob(validation=check, region_code=state_code))

    return validation_jobs


def _emit_failures(failed_validations: List[DataValidationJobResult]):
    monitoring_tags: Dict[str, Any] = {}
    with monitoring.measurements(monitoring_tags) as measurements:
        for result in failed_validations:
            logging.error("Failed data validation: %s", result)

            monitoring_tags[monitoring.TagKey.REGION] = result.validation_job.region_code
            monitoring_tags[monitoring.TagKey.VALIDATION_CHECK_TYPE] = result.validation_job.validation.validation_type
            measurements.measure_int_put(m_failed_validations, 1)
