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
from collections import defaultdict
from concurrent import futures
from itertools import chain, groupby
from typing import Any, Dict, List, Optional, Pattern, Tuple

import pytz

from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.monitoring import trace
from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey
from recidiviz.utils import metadata, structured_logging
from recidiviz.utils.environment import gcp_only, get_environment_for_project
from recidiviz.utils.github import RECIDIVIZ_DATA_REPO, github_helperbot_client
from recidiviz.validation.configured_validations import (
    get_all_deployed_validations,
    get_validation_global_config,
    get_validation_region_configs,
)
from recidiviz.validation.validation_github_ticket_manager import (
    ValidationGithubTicketRegionManager,
)
from recidiviz.validation.validation_models import (
    DataValidationJob,
    DataValidationJobResult,
    ValidationResultStatus,
)
from recidiviz.validation.validation_outputs import (
    store_validation_results_in_big_query,
    store_validation_run_completion_in_big_query,
)
from recidiviz.validation.validation_result_for_storage import (
    ValidationResultForStorage,
)
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import deployed_view_builders


def attributes_for_job(job: DataValidationJob) -> Dict[str, Any]:
    return {
        AttributeKey.REGION: job.region_code,
        AttributeKey.VALIDATION_CHECK_TYPE: job.validation.validation_type,
        AttributeKey.VALIDATION_VIEW_ID: job.validation.validation_name,
    }


def capture_metrics(
    failed_to_run_validations: List[DataValidationJob],
    failed_hard_validations: List[DataValidationJobResult],
) -> None:
    for validation_job in failed_to_run_validations:
        get_monitoring_instrument(CounterInstrumentKey.VALIDATION_FAILURE_TO_RUN).add(
            amount=1,
            attributes=attributes_for_job(validation_job),
        )

    for result in failed_hard_validations:
        # Skip dev mode failures when emitting metrics
        if not result.result_details.is_dev_mode:
            get_monitoring_instrument(CounterInstrumentKey.VALIDATION_FAILURE).add(
                amount=1,
                attributes=attributes_for_job(result.validation_job),
            )


@gcp_only
def execute_validation_request(state_code: StateCode) -> None:
    start_datetime = datetime.datetime.now()
    run_id, num_validations_run = execute_validation(
        region_code=state_code.value,
        sandbox_dataset_prefix=None,
    )
    end_datetime = datetime.datetime.now()

    runtime_sec = int((end_datetime - start_datetime).total_seconds())

    store_validation_run_completion_in_big_query(
        state_code=state_code,
        validation_run_id=run_id,
        num_validations_run=num_validations_run,
        validations_runtime_sec=runtime_sec,
        sandbox_dataset_prefix=None,
    )


def execute_validation(
    *,
    region_code: str,
    validation_name_filter: Optional[Pattern] = None,
    sandbox_dataset_prefix: Optional[str],
    file_tickets_on_failure: bool = True,
) -> Tuple[str, int]:
    """Executes validation checks for |region_code|.
    |ingest_instance| is the ingest instance used to generate the data that is being validated. This determines which
    validations are allowed to run.
    If |validation_name_filter| is supplied, only performs validations on those
    that have a regex match.
    If |sandbox_dataset_prefix| is supplied, performs validation using sandbox dataset

    Returns a tuple with the validation run_id and the number of validation jobs run.
    """

    # Fetch collection of validation jobs to perform
    validation_jobs = _get_validations_jobs(
        region_code=region_code,
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
    validation_results: dict[
        ValidationResultStatus, list[DataValidationJobResult]
    ] = defaultdict(list)
    results_to_store: List[ValidationResultForStorage] = []
    with futures.ThreadPoolExecutor(
        # Conservatively allow only half as many workers as allowed connections.
        # Lower this number if we see "urllib3.connectionpool:Connection pool is
        # full, discarding connection" errors.
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        future_to_jobs: Dict[
            futures.Future[Tuple[float, DataValidationJobResult]], DataValidationJob
        ] = {
            executor.submit(
                trace.time_and_trace(structured_logging.with_context(_run_job)), job
            ): job
            for job in validation_jobs
        }

        for future in futures.as_completed(future_to_jobs):
            job = future_to_jobs[future]
            try:
                runtime_seconds, result = future.result()
                results_to_store.append(
                    ValidationResultForStorage.from_validation_result(
                        run_id=run_id,
                        run_datetime=run_datetime,
                        result=result,
                        runtime_seconds=runtime_seconds,
                    )
                )
                validation_results[result.validation_result_status].append(result)
                logging.info(
                    "Finished job [%s] for region [%s] in %.2f seconds",
                    job.validation.validation_name,
                    job.region_code,
                    runtime_seconds,
                )
            except Exception as e:
                logging.error(
                    "Failed to execute asynchronous query for validation job [%s] due to error: %r",
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

    capture_metrics(
        failed_to_run_validations, validation_results[ValidationResultStatus.FAIL_HARD]
    )

    # Log results to console
    _log_results(
        failed_to_run_validations=failed_to_run_validations,
        failed_soft_validations=validation_results[ValidationResultStatus.FAIL_SOFT],
        failed_hard_validations=validation_results[ValidationResultStatus.FAIL_HARD],
    )
    logging.info(
        "Validation run complete. Analyzed a total of %s jobs.", len(validation_jobs)
    )
    if file_tickets_on_failure and validation_results:
        # Put GitHub filing in a try/except so we don't fail the endpoint completely if we can't
        # talk to GitHub for whatever reason.
        try:
            _handle_tickets_for_validations(validation_results)
        except Exception as e:
            logging.error("Error filing github tickets: %s", e)
    return run_id, len(validation_jobs)


def _get_validations_jobs(
    region_code: str,
    validation_name_filter: Optional[Pattern] = None,
    sandbox_dataset_prefix: Optional[str] = None,
) -> List[DataValidationJob]:
    view_builders = deployed_view_builders()

    sandbox_context = None
    if sandbox_dataset_prefix:
        sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides_for_view_builders(
                sandbox_dataset_prefix, view_builders
            ),
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix=sandbox_dataset_prefix,
            state_code_filter=None,
        )

    # Fetch collection of validation jobs to perform
    return _fetch_validation_jobs_to_perform(
        region_code=region_code,
        validation_name_filter=validation_name_filter,
        sandbox_context=sandbox_context,
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
    region_code: str,
    validation_name_filter: Optional[Pattern] = None,
    sandbox_context: BigQueryViewSandboxContext | None = None,
) -> List[DataValidationJob]:
    """
    Creates and returns validation jobs for all validations meeting the name filter,
    for the given region code, and with the dataset overrides if given.
    """
    validation_checks = get_all_deployed_validations()
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

        region_config = region_configs[region_code]
        if check.validation_name not in region_config.exclusions:
            updated_check = check.updated_for_region(region_config)
            validation_jobs.append(
                DataValidationJob(
                    validation=updated_check,
                    region_code=region_code,
                    sandbox_context=sandbox_context,
                )
            )

    return validation_jobs


def _handle_tickets_for_validations(
    validation_results: dict[ValidationResultStatus, list[DataValidationJobResult]],
) -> None:
    """Files GitHub tickets for failed validations that do not already have an associated
    ticket, and closes GitHub tickets for validations that have an open ticket but are
    no longer in a failing state.
    """

    logging.info("Filing GitHub tickets for failed validations")
    env = get_environment_for_project(project=metadata.project_id()).value
    github_client = github_helperbot_client().get_repo(RECIDIVIZ_DATA_REPO)

    for region, validations in groupby(
        sorted(
            chain.from_iterable(validation_results.values()),
            key=lambda v: v.validation_job.region_code,
        ),
        lambda v: v.validation_job.region_code,
    ):
        ValidationGithubTicketRegionManager(
            client=github_client,
            state_code=StateCode(region.upper()),
            env=env,
        ).handle_results(results=validations)
