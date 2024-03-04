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
"""GCS Store used to keep counts of column values across the state ingest dataset
specifically."""
import datetime
from typing import Dict, List, Optional, Union

import attr
import pytz
from google.cloud.bigquery.table import Row

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContentsImpl,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.bq_refresh_status_storage import (
    CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS,
    CloudSqlToBqRefreshStatus,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils import metadata


def cloud_sql_current_refresh_status_query_for_schema(
    project_id: str, bq_refresh_address: BigQueryAddress, schema: SchemaType
) -> str:
    return f"""
    SELECT
        refresh_run_id,
        schema,
        last_refresh_datetime,
        region_code,
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY schema, region_code
            -- Orders by recency of the compared data
            ORDER BY last_refresh_datetime DESC) as ordinal
        FROM `{project_id}.{bq_refresh_address.dataset_id}.{bq_refresh_address.table_id}`
    )
    -- Get the row with the most recent status
    WHERE ordinal = 1
    AND schema = "{schema.name}"
    """


def cloud_sql_refresh_status_history_for_schema_and_state(
    project_id: str,
    bq_refresh_address: BigQueryAddress,
    schema: SchemaType,
    state_code: StateCode,
    start_timestamp: datetime.datetime,
) -> str:
    return f"""
    SELECT last_refresh_datetime
    FROM `{project_id}.{bq_refresh_address.dataset_id}.{bq_refresh_address.table_id}`
    WHERE region_code = "{state_code.value}"
    AND schema = "{schema.name}"
    AND last_refresh_datetime > "{start_timestamp.isoformat()}"
    ORDER BY last_refresh_datetime DESC
    """


@attr.define(frozen=True, kw_only=True)
class StateDataFreshnessInfo:
    state_code: StateCode
    state_dataset_data_freshness: Optional[datetime.datetime]
    last_state_dataset_refresh_time: Optional[datetime.datetime]


# TODO(#20930): The data freshness page for STATE does not make any sense in a post-IID
#  world. We should delete it entirely and replace it with messaging around how it
#  has been replaced by the Raw Data Freshness + Latest Pipeline Run/Job End Time
#  sections on the state-specific admin panel page.
class IngestDataFreshnessStore(AdminPanelStore):
    """An AdminPanelStore for or tracking data freshness of data ingested from our
    states.
    """

    def __init__(self) -> None:
        self._data_freshness_results: List[Dict[str, Union[Optional[str], bool]]] = []
        self._data_freshness_results_last_calculated: Optional[datetime.datetime] = None
        self.gcs_fs = GcsfsFactory.build()
        self.bq_client: BigQueryClient = BigQueryClientImpl()

    def hydrate_cache(self) -> None:
        # not implemented
        pass

    @property
    def data_freshness_results(self) -> List[Dict[str, Union[Optional[str], bool]]]:
        """Refreshes information in the metadata store about freshness of ingested data
        for all states."""

        if self._data_freshness_results_last_calculated:
            time_since_last_calculated = (
                datetime.datetime.now() - self._data_freshness_results_last_calculated
            ).total_seconds()

            if time_since_last_calculated < 15 * 60:
                # Return cached results if results were calculated in last 15 min
                return self._data_freshness_results

        latest_upper_bounds: List[Dict[str, Union[Optional[str], bool]]] = []

        ingested_states = get_direct_ingest_states_launched_in_env()

        state_data_freshness = self.get_data_freshness_by_state(
            state_codes=ingested_states,
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        for state_code in ingested_states:
            latest_upper_bounds.append(
                {
                    "state": state_code.name,
                    "date": _iso_date_string_from_optional_datetime(
                        state_data_freshness[state_code].state_dataset_data_freshness
                    ),
                    "lastRefreshDate": _iso_date_string_from_optional_datetime(
                        state_data_freshness[state_code].last_state_dataset_refresh_time
                    ),
                    "ingestPaused": False,
                }
            )

        self._data_freshness_results = latest_upper_bounds
        self._data_freshness_results_last_calculated = datetime.datetime.now()
        return self._data_freshness_results

    def get_current_statuses_for_schema(
        self, schema: SchemaType
    ) -> Dict[Optional[StateCode], CloudSqlToBqRefreshStatus]:
        query_job = self.bq_client.run_query_async(
            query_str=cloud_sql_current_refresh_status_query_for_schema(
                metadata.project_id(),
                CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS,
                schema,
            ),
            use_query_cache=True,
        )

        # Build up new results
        records: Dict[Optional[StateCode], CloudSqlToBqRefreshStatus] = {}

        for row in query_job:
            record = _bq_refresh_status_record_for_row(row)
            records[
                StateCode(record.region_code) if record.region_code else None
            ] = record
        return records

    def get_refresh_timestamps_for_schema_and_state_since(
        self,
        state_code: StateCode,
        start_timestamp: datetime.datetime,
    ) -> List[datetime.datetime]:
        # TODO(#20103): Add most recent data processed times
        query_job = self.bq_client.run_query_async(
            query_str=cloud_sql_refresh_status_history_for_schema_and_state(
                metadata.project_id(),
                CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS,
                SchemaType.STATE,
                state_code=state_code,
                start_timestamp=start_timestamp,
            ),
            use_query_cache=True,
        )

        return [
            row["last_refresh_datetime"].replace(tzinfo=pytz.UTC) for row in query_job
        ]

    def get_data_freshness_by_state(
        self,
        state_codes: List[StateCode],
        ingest_instance: DirectIngestInstance,
        dataset_prefix: Optional[str],
    ) -> Dict[StateCode, StateDataFreshnessInfo]:
        """Returns the ingest "high water mark" for each state, i.e. the latest date
        where all files on or before that date are processed for a that state
        """
        date_freshness_by_state: Dict[StateCode, StateDataFreshnessInfo] = {}
        refresh_status_bq = self.get_current_statuses_for_schema(SchemaType.STATE)

        for state_code in state_codes:
            content = InstanceIngestViewContentsImpl(
                big_query_client=self.bq_client,
                region_code=state_code.name.lower(),
                dataset_prefix=dataset_prefix,
                ingest_instance=ingest_instance,
            )
            max_dates = content.get_max_date_of_data_processed_before_datetime(
                datetime_utc=refresh_status_bq[state_code].last_refresh_datetime
            )
            min_dates = content.get_min_date_of_unprocessed_data()

            done_processing_views_max_date = None
            still_processing_views_min_date = None
            all_ingest_view_names = {*max_dates.keys(), *min_dates.keys()}
            for ingest_view_name in all_ingest_view_names:
                max_processed_date = max_dates.get(ingest_view_name, None)
                min_unprocessed_date = min_dates.get(ingest_view_name, None)

                if min_unprocessed_date is not None:
                    # If there are still unprocessed data for this ingest view, then
                    # we can't show a "freshness" date after the max_processed_date
                    # of this view.
                    still_processing_views_min_date = _pick_min_date(
                        [still_processing_views_min_date, max_processed_date]
                    )
                else:
                    # For all views that are done processing, we pick the max date
                    # of all those views to show the freshness of the data.
                    done_processing_views_max_date = _pick_max_date(
                        [done_processing_views_max_date, max_processed_date]
                    )

            state_dataset_data_freshness = (
                still_processing_views_min_date
                if still_processing_views_min_date
                else done_processing_views_max_date
            )
            date_freshness_by_state[state_code] = StateDataFreshnessInfo(
                state_code=state_code,
                state_dataset_data_freshness=state_dataset_data_freshness,
                last_state_dataset_refresh_time=refresh_status_bq[
                    state_code
                ].last_refresh_datetime
                if refresh_status_bq.get(state_code)
                else None,
            )

        return date_freshness_by_state


def _iso_date_string_from_optional_datetime(
    dt: Optional[datetime.datetime],
) -> Optional[str]:
    if not dt:
        return None
    return dt.date().isoformat()


def _pick_min_date(
    dates: List[Optional[datetime.datetime]],
) -> Optional[datetime.datetime]:
    non_optional_dates = [d for d in dates if d is not None]
    if not non_optional_dates:
        return None
    return min(non_optional_dates)


def _pick_max_date(
    dates: List[Optional[datetime.datetime]],
) -> Optional[datetime.datetime]:
    non_optional_dates = [d for d in dates if d is not None]
    if not non_optional_dates:
        return None
    return max(non_optional_dates)


def _bq_refresh_status_record_for_row(row: Row) -> CloudSqlToBqRefreshStatus:
    """Takes a BigQuery row from the query template and converts it to an object"""

    return CloudSqlToBqRefreshStatus(
        refresh_run_id=row["refresh_run_id"],
        last_refresh_datetime=row["last_refresh_datetime"].replace(tzinfo=pytz.UTC),
        schema=row["schema"],
        region_code=row["region_code"],
    )
