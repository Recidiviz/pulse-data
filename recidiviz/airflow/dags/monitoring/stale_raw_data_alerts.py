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
"""Creates and manages GitHub issues for stale raw data files."""
import logging
from datetime import datetime, timezone

from recidiviz.airflow.dags.monitoring.stale_raw_data_alerting_incident import (
    StaleRawDataAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.stale_raw_data_github_alerting_service import (
    StaleRawDataGitHubService,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.utils.environment import in_gcp_production


def report_stale_raw_data_to_github(
    update_datetimes_by_region: dict[str, dict[str, str]],
) -> None:
    """Reports stale raw data files to GitHub, one issue per file.

    For each regularly-updated file in each launched region, creates or updates a GitHub issue
    if the file is stale, or closes the issue if the file becomes fresh.

    Args:
        update_datetimes_by_region: Nested dictionary mapping upper case region codes
            to a dictionary of file_tag -> ISO-formatted utc update datetime string.
    """
    if not in_gcp_production():
        logging.info("Skipping GitHub alerts in non-production environment.")
        return

    current_utc_datetime = datetime.now(tz=timezone.utc)
    project_id = get_project_id()

    errors = []
    for state_code in get_direct_ingest_states_launched_in_env():
        if state_code.value not in update_datetimes_by_region:
            logging.warning(
                "No update datetime data for launched state [%s]",
                state_code.value,
            )
            continue

        service = StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code(
            project_id=project_id, state_code=state_code
        )

        region_config = get_region_raw_file_config(state_code.value)
        regularly_updated_configs = {
            config.file_tag: config
            for config in region_config.get_configs_with_regularly_updated_data()
        }

        most_recent_import_datetime_by_file_tag = {
            file_tag: datetime.fromisoformat(
                update_datetimes_by_region[state_code.value][file_tag]
            )
            for file_tag in update_datetimes_by_region[state_code.value]
        }

        for file_tag, config in regularly_updated_configs.items():
            if file_tag not in most_recent_import_datetime_by_file_tag:
                # if we have not yet imported this file, we can ignore
                logging.info(
                    "No update datetime for regularly-updated file [%s][%s]",
                    state_code.value,
                    file_tag,
                )
                continue

            time_since_update = (
                current_utc_datetime - most_recent_import_datetime_by_file_tag[file_tag]
            )
            hours_stale = (
                time_since_update.total_seconds() / 3600
                - config.max_hours_before_stale()
            )
            if hours_stale < 0:
                hours_stale = 0.0
            else:
                logging.info(
                    "STALE - Raw data file [%s] is above alert threshold, %.2f hours stale.",
                    file_tag,
                    hours_stale,
                )

            # we create incidents for all files, even those that are not currently stale.
            # this allows the github service to close out issues for files that become fresh
            incident = StaleRawDataAlertingIncident(
                state_code=state_code.value,
                file_tag=file_tag,
                hours_stale=hours_stale,
                most_recent_import_date=most_recent_import_datetime_by_file_tag[
                    file_tag
                ],
            )

            try:
                service.handle_incident(incident)
            except Exception as e:
                errors.append(
                    f"Failed to report incident for file [{file_tag}] in state [{state_code.value}], error: {str(e)}"
                )

    if errors:
        raise ValueError(
            f"Encountered the following errors while reporting incidents: {', '.join(errors)}"
        )
