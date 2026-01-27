# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""SFTP ingest ready file upload timeliness monitoring"""
import logging
from datetime import datetime, timedelta, timezone

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_config_enums import (
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    states_with_sftp_delegates,
)
from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, GaugeInstrumentKey
from recidiviz.utils import metadata
from recidiviz.utils.environment import gcp_only

CADENCE_ALERT_MULTIPLIER = 1.25


def _get_alert_threshold(region_code: str) -> timedelta:
    """Returns the alert threshold for a given region based on its default update cadence."""
    default_update_cadence = get_region_raw_file_config(
        region_code=region_code
    ).default_update_cadence
    update_cadence_in_days = RawDataFileUpdateCadence.interval_from_cadence(
        default_update_cadence
    )
    return timedelta(days=update_cadence_in_days * CADENCE_ALERT_MULTIPLIER)


@gcp_only
def report_sftp_ingest_ready_file_timeliness_metrics(
    upload_times_by_region_code: dict[str, str],
) -> None:
    """Reports SFTP ingest ready file timeliness metrics to OpenTelemetry.

    Args:
        upload_times_by_region_code: Dictionary mapping upper case region codes
            to ISO-formatted utc upload datetime strings.

    Emits gauge observations for SFTP regions whose most recent ingest ready file
    upload time exceeds the alert threshold, determined by the state's default
    update cadence.
    """
    utc_upload_times_by_region = {
        StateCode(region_code_str): datetime.fromisoformat(upload_time_str)
        for region_code_str, upload_time_str in upload_times_by_region_code.items()
    }

    if not (sftp_states := states_with_sftp_delegates(metadata.project_id())):
        raise ValueError(
            f"No SFTP-enabled states found for project [{metadata.project_id()}]."
        )

    current_utc_datetime = datetime.now(tz=timezone.utc)
    gauge = get_monitoring_instrument(GaugeInstrumentKey.SFTP_INGEST_READY_FILE_AGE)

    for state_code in sftp_states:
        # If this is a newly added SFTP state, there may be no upload data yet
        if state_code not in utc_upload_times_by_region:
            logging.info(
                "No SFTP ingest ready file uploads found for region: %s",
                state_code.value,
            )
            continue

        alert_threshold = _get_alert_threshold(state_code.value)
        time_since_upload = (
            current_utc_datetime - utc_upload_times_by_region[state_code]
        )

        logging.info(
            "SFTP ingest ready file upload for [%s]: time since last upload: [%s], alert threshold: [%s]",
            state_code.value,
            time_since_upload,
            alert_threshold,
        )

        # Only emit observation if time since upload exceeds alert threshold
        # this allows us to easily control the staleness threshold on a per-state
        # basis as then we can alert on any observation for this metric
        if time_since_upload <= alert_threshold:
            continue

        logging.info(
            "STALE - SFTP ingest ready files for [%s] are above alert threshold.",
            state_code.value,
        )

        hours_stale = time_since_upload.total_seconds() / 3600
        gauge.set(
            amount=hours_stale, attributes={AttributeKey.REGION: state_code.value}
        )
