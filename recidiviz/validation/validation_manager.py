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
import logging
import re
from concurrent import futures
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Pattern, Tuple

from opencensus.stats import aggregation, measure, view

from flask import Blueprint

from recidiviz.big_query import view_update_manager
from recidiviz.utils import monitoring, structured_logging
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.validation.checks.check_resolver import checker_for_validation

from recidiviz.validation.configured_validations import (
    get_all_validations,
    get_validation_region_configs,
    get_validation_global_config,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
)

m_failed_to_run_validations = measure.MeasureInt(
    "validation/num_fail_to_run",
    "The number of validations that failed to run entirely",
    "1",
)

failed_to_run_validations_view = view.View(
    "recidiviz/validation/num_fail_to_run",
    "The sum of validations that failed to run",
    [
        monitoring.TagKey.REGION,
        monitoring.TagKey.VALIDATION_CHECK_TYPE,
        monitoring.TagKey.VALIDATION_VIEW_ID,
    ],
    m_failed_to_run_validations,
    aggregation.SumAggregation(),
)

m_failed_validations = measure.MeasureInt(
    "validation/num_failures", "The number of failed validations", "1"
)

failed_validations_view = view.View(
    "recidiviz/validation/num_failures",
    "The sum of failed validations",
    [
        monitoring.TagKey.REGION,
        monitoring.TagKey.VALIDATION_CHECK_TYPE,
        monitoring.TagKey.VALIDATION_VIEW_ID,
    ],
    m_failed_validations,
    aggregation.SumAggregation(),
)

monitoring.register_views([failed_validations_view, failed_to_run_validations_view])


validation_manager_blueprint = Blueprint("validation_manager", __name__)


@validation_manager_blueprint.route("/validate")
@requires_gae_auth
def handle_validation_request() -> Tuple[str, HTTPStatus]:
    """API endpoint to service data validation requests."""
    failed_validations = execute_validation(rematerialize_views=True)

    return _readable_response(failed_validations), HTTPStatus.OK


def execute_validation(
    rematerialize_views: bool,
    region_code_filter: Optional[str] = None,
    validation_name_filter: Optional[Pattern] = None,
) -> List[DataValidationJobResult]:
    """Executes all validation checks.

    If |region_code_filter| is supplied, limits validations to just that region.
    If |validation_name_filter| is supplied, only performs validations on those
    that have a regex match.
    """
    if rematerialize_views:
        logging.info(
            'Received query param "should_update_views" = true, updating validation dataset and views... '
        )
        view_update_manager.rematerialize_views()

    # Fetch collection of validation jobs to perform
    validation_jobs = _fetch_validation_jobs_to_perform(
        region_code_filter=region_code_filter,
        validation_name_filter=validation_name_filter,
    )
    logging.info("Performing a total of %s validation jobs...", len(validation_jobs))

    # Perform all validations and track failures
    failed_to_run_validations: List[DataValidationJob] = []
    failed_validations: List[DataValidationJobResult] = []
    with futures.ThreadPoolExecutor() as executor:
        future_to_jobs = {
            executor.submit(structured_logging.with_context(_run_job), job): job
            for job in validation_jobs
        }

        for future in futures.as_completed(future_to_jobs):
            job = future_to_jobs[future]
            try:
                result = future.result()
                if not result.was_successful:
                    failed_validations.append(result)
                logging.info(
                    "Finished job [%s] for region [%s]",
                    job.validation.validation_name,
                    job.region_code,
                )
            except Exception as e:
                logging.error(
                    "Failed to execute asynchronous query for validation job [%s] due to error: %s",
                    job,
                    e,
                )
                failed_to_run_validations.append(job)

    if failed_validations or failed_to_run_validations:
        logging.error(
            "Found a total of [%s] failures, with [%s] failing to run entirely. Emitting results...",
            len(failed_validations) + len(failed_to_run_validations),
            len(failed_to_run_validations),
        )
        # Emit metrics for all failures
        _emit_failures(failed_to_run_validations, failed_validations)
    else:
        logging.info("Found no failed validations...")

    logging.info(
        "Validation run complete. Analyzed a total of %s jobs.", len(validation_jobs)
    )
    return failed_validations


def _run_job(job: DataValidationJob) -> DataValidationJobResult:
    validation_checker = checker_for_validation(job)
    return validation_checker.run_check(job)


def _fetch_validation_jobs_to_perform(
    region_code_filter: Optional[str] = None,
    validation_name_filter: Optional[Pattern] = None,
) -> List[DataValidationJob]:
    validation_checks = get_all_validations()
    region_configs = get_validation_region_configs()
    global_config = get_validation_global_config()

    validation_jobs: List[DataValidationJob] = []
    for check in validation_checks:
        if check.validation_name in global_config.disabled:
            continue
        if validation_name_filter is not None and not re.search(
            validation_name_filter, check.validation_name
        ):
            continue

        for region_code, region_config in region_configs.items():
            if region_code_filter and region_code != region_code_filter:
                continue
            if check.validation_name not in region_config.exclusions:
                updated_check = check.updated_for_region(region_config)
                validation_jobs.append(
                    DataValidationJob(validation=updated_check, region_code=region_code)
                )

    return validation_jobs


def _emit_failures(
    failed_to_run_validations: List[DataValidationJob],
    failed_validations: List[DataValidationJobResult],
) -> None:
    def tags_for_job(job: DataValidationJob) -> Dict[str, Any]:
        return {
            monitoring.TagKey.REGION: job.region_code,
            monitoring.TagKey.VALIDATION_CHECK_TYPE: job.validation.validation_type,
            monitoring.TagKey.VALIDATION_VIEW_ID: job.validation.validation_name,
        }

    for validation_job in failed_to_run_validations:
        logging.error("Failed to run data validation job: %s", validation_job)

        monitoring_tags = tags_for_job(validation_job)
        with monitoring.measurements(monitoring_tags) as measurements:
            measurements.measure_int_put(m_failed_to_run_validations, 1)

    for result in failed_validations:
        logging.error("Failed data validation: %s", result)

        monitoring_tags = tags_for_job(result.validation_job)
        with monitoring.measurements(monitoring_tags) as measurements:
            measurements.measure_int_put(m_failed_validations, 1)


def _readable_response(failed_validations: List[DataValidationJobResult]) -> str:
    readable_output = "\n".join([f.__str__() for f in failed_validations])
    return f"Failed validations:\n{readable_output}"
