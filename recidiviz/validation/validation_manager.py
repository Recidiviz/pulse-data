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
import datetime
import logging
import re
import uuid
from concurrent import futures
from http import HTTPStatus
from time import sleep
from typing import Any, Dict, List, Optional, Pattern, Tuple

import pytz
from flask import Blueprint
from opencensus.stats import aggregation, measure, view

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.cloud_tasks.utils import get_current_cloud_task_id
from recidiviz.utils import metadata, monitoring, structured_logging
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.validation.configured_validations import (
    get_all_validations,
    get_validation_global_config,
    get_validation_region_configs,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationResultStatus,
)
from recidiviz.validation.validation_result_storage import (
    ValidationResultForStorage,
    store_validation_results_in_big_query,
    store_validation_run_completion_in_big_query,
)
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders

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

# TODO(#13775): Delete this endpoint once we have verified Airflow orchestration works.
@validation_manager_blueprint.route(
    "/test_validate_orchestration", methods=["GET", "POST"]
)
@requires_gae_auth
def test_handle_validation_request() -> Tuple[str, HTTPStatus]:
    """TEST ONLY API endpoint that that enables us to validate Airflow orchestration."""
    cloud_task_id = get_current_cloud_task_id()
    start_datetime = datetime.datetime.now()
    sleep(3)
    num_validations_run = 5
    end_datetime = datetime.datetime.now()

    runtime_sec = int((end_datetime - start_datetime).total_seconds())

    store_validation_run_completion_in_big_query(
        validation_run_id="dummy_run_id",
        num_validations_run=num_validations_run,
        cloud_task_id=cloud_task_id,
        validations_runtime_sec=runtime_sec,
    )

    return "", HTTPStatus.OK


@validation_manager_blueprint.route("/validate", methods=["GET", "POST"])
@requires_gae_auth
def handle_validation_request() -> Tuple[str, HTTPStatus]:
    """API endpoint to service data validation requests."""
    cloud_task_id = get_current_cloud_task_id()
    start_datetime = datetime.datetime.now()
    run_id, num_validations_run = execute_validation()
    end_datetime = datetime.datetime.now()

    runtime_sec = int((end_datetime - start_datetime).total_seconds())

    store_validation_run_completion_in_big_query(
        validation_run_id=run_id,
        num_validations_run=num_validations_run,
        cloud_task_id=cloud_task_id,
        validations_runtime_sec=runtime_sec,
    )

    return "", HTTPStatus.OK


def execute_validation(
    region_code_filter: Optional[str] = None,
    validation_name_filter: Optional[Pattern] = None,
    sandbox_dataset_prefix: Optional[str] = None,
) -> Tuple[str, int]:
    """Executes all validation checks.
    If |region_code_filter| is supplied, limits validations to just that region.
    If |validation_name_filter| is supplied, only performs validations on those
    that have a regex match.
    If |sandbox_dataset_prefix| is supplied, performs validation using sandbox dataset

    Returns a tuple with the validation run_id and the number of validation jobs run.
    """

    # Fetch collection of validation jobs to perform
    validation_jobs = _get_validations_jobs(
        region_code_filter=region_code_filter,
        validation_name_filter=validation_name_filter,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    run_datetime = datetime.datetime.now(tz=pytz.UTC)
    run_id = uuid.uuid4().hex
    logging.info(
        "Performing a total of %s validation jobs [run_datetime: %s, run_id: %s]...",
        len(validation_jobs),
        run_datetime.isoformat(),
        run_id,
    )

    # Perform all validations and track failures
    failed_to_run_validations: List[DataValidationJob] = []
    failed_soft_validations: List[DataValidationJobResult] = []
    failed_hard_validations: List[DataValidationJobResult] = []
    results_to_store: List[ValidationResultForStorage] = []
    with futures.ThreadPoolExecutor() as executor:
        future_to_jobs = {
            executor.submit(structured_logging.with_context(_run_job), job): job
            for job in validation_jobs
        }

        for future in futures.as_completed(future_to_jobs):
            job = future_to_jobs[future]
            try:
                result: DataValidationJobResult = future.result()
                results_to_store.append(
                    ValidationResultForStorage.from_validation_result(
                        run_id=run_id,
                        run_datetime=run_datetime,
                        result=result,
                    )
                )
                if result.validation_result_status == ValidationResultStatus.FAIL_HARD:
                    failed_hard_validations.append(result)
                if result.validation_result_status == ValidationResultStatus.FAIL_SOFT:
                    failed_soft_validations.append(result)
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
                results_to_store.append(
                    ValidationResultForStorage.from_validation_job(
                        run_id=run_id,
                        run_datetime=run_datetime,
                        job=job,
                        exception_log=e,
                    )
                )
                failed_to_run_validations.append(job)

    store_validation_results_in_big_query(results_to_store)
    if failed_to_run_validations or failed_hard_validations:
        # Emit metrics for all hard and total failures
        _emit_opencensus_failure_events(
            failed_to_run_validations,
            # Skip dev mode failures when emitting metrics
            [
                result
                for result in failed_hard_validations
                if not result.result_details.is_dev_mode
            ],
        )
    # Log results to console
    _log_results(
        failed_to_run_validations=failed_to_run_validations,
        failed_soft_validations=failed_soft_validations,
        failed_hard_validations=failed_hard_validations,
    )
    logging.info(
        "Validation run complete. Analyzed a total of %s jobs.", len(validation_jobs)
    )
    return run_id, len(validation_jobs)


def _get_validations_jobs(
    region_code_filter: Optional[str] = None,
    validation_name_filter: Optional[Pattern] = None,
    sandbox_dataset_prefix: Optional[str] = None,
) -> List[DataValidationJob]:
    view_builders = deployed_view_builders(metadata.project_id())

    sandbox_address_overrides = None
    if sandbox_dataset_prefix:
        sandbox_address_overrides = address_overrides_for_view_builders(
            sandbox_dataset_prefix, view_builders
        )

    # Fetch collection of validation jobs to perform
    return _fetch_validation_jobs_to_perform(
        region_code_filter=region_code_filter,
        validation_name_filter=validation_name_filter,
        address_overrides=sandbox_address_overrides,
    )


def _log_results(
    *,
    failed_to_run_validations: List[DataValidationJob],
    failed_soft_validations: List[DataValidationJobResult],
    failed_hard_validations: List[DataValidationJobResult],
) -> None:
    """Writes summary of failed validations, including dev mode, to output logs."""
    total_failed_validations = (
        len(failed_hard_validations)
        + len(failed_to_run_validations)
        + len(failed_soft_validations)
    )
    dev_mode_failures = [
        result
        for result in failed_soft_validations + failed_hard_validations
        if result.result_details.is_dev_mode
    ]
    if failed_hard_validations or failed_soft_validations or failed_to_run_validations:
        logging.error(
            "Found a total of [%d] failure(s), plus [%d] dev mode failure(s). "
            "In total, there were [%d] soft failure(s), [%d] hard failure(s), "
            "and [%d] that failed to run entirely. Emitting results...",
            total_failed_validations - len(dev_mode_failures),
            len(dev_mode_failures),
            len(failed_soft_validations),
            len(failed_hard_validations),
            len(failed_to_run_validations),
        )
        for validation_job in failed_to_run_validations:
            logging.error("Failed to run data validation job: %s", validation_job)
        for result in failed_hard_validations:
            logging.error("Failed data validation HARD threshold: %s", result)
        for result in failed_soft_validations:
            logging.error("Failed data validation SOFT threshold: %s", result)
    else:
        logging.info("Found no failed validations...")


def _run_job(job: DataValidationJob) -> DataValidationJobResult:
    return job.validation.get_checker().run_check(job)


def _fetch_validation_jobs_to_perform(
    region_code_filter: Optional[str] = None,
    validation_name_filter: Optional[Pattern] = None,
    address_overrides: Optional[BigQueryAddressOverrides] = None,
) -> List[DataValidationJob]:
    """
    Creates and returns validation jobs for all validations meeting the name filter,
    for the given region code, and with the dataset overrides if given.
    """
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
                    DataValidationJob(
                        validation=updated_check,
                        region_code=region_code,
                        address_overrides=address_overrides,
                    )
                )

    return validation_jobs


def _emit_opencensus_failure_events(
    failed_to_run_validations: List[DataValidationJob],
    failed_hard_validations: List[DataValidationJobResult],
) -> None:
    def tags_for_job(job: DataValidationJob) -> Dict[str, Any]:
        return {
            monitoring.TagKey.REGION: job.region_code,
            monitoring.TagKey.VALIDATION_CHECK_TYPE: job.validation.validation_type,
            monitoring.TagKey.VALIDATION_VIEW_ID: job.validation.validation_name,
        }

    for validation_job in failed_to_run_validations:
        monitoring_tags = tags_for_job(validation_job)
        with monitoring.measurements(monitoring_tags) as measurements:
            measurements.measure_int_put(m_failed_to_run_validations, 1)

    for result in failed_hard_validations:
        monitoring_tags = tags_for_job(result.validation_job)
        with monitoring.measurements(monitoring_tags) as measurements:
            measurements.measure_int_put(m_failed_validations, 1)


def _readable_response(failed_validations: List[DataValidationJobResult]) -> str:
    readable_output = "\n".join([str(f) for f in failed_validations])
    return f"Failed validations:\n{readable_output}"
