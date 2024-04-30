# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines configuration related to updating / materializing our deployed views."""
from typing import Dict

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import ProcessDagPerfConfig
from recidiviz.utils import environment

_MAX_SINGLE_VIEW_MATERIALIZATION_TIME_SECONDS = 60 * 6  # 6 min

# Add overrides here for graph nodes that are known to be more expensive to process.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IF YOU MUST MODIFY THIS MAP, PLEASE ADD Recidiviz/infra-review AS A REVIEWER TO YOUR
# PULL REQUEST.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
_ALLOWED_MATERIALIZATION_TIME_OVERRIDES: Dict[BigQueryAddress, float] = {
    # This is a foundational view that is known to be expensive - we're ok with this
    # taking a bit longer to materialize.
    BigQueryAddress(
        dataset_id="sessions",
        table_id="dataflow_sessions",
    ): (60 * 10),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_officer_period_event_aggregated_metrics",
    ): (60 * 20),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_office_period_event_aggregated_metrics",
    ): (60 * 20),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_district_period_event_aggregated_metrics",
    ): (60 * 20),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_state_period_event_aggregated_metrics",
    ): (60 * 20),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_unit_period_event_aggregated_metrics",
    ): (60 * 20),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="incarceration_facility_period_event_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_district_period_span_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_officer_period_span_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_state_period_span_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="incarceration_facility_period_span_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_office_period_span_aggregated_metrics",
    ): (60 * 15),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="justice_involved_state_period_span_aggregated_metrics",
    ): (60 * 10),
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="incarceration_state_period_event_aggregated_metrics",
    ): (60 * 10),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="incarceration_state_period_span_aggregated_metrics",
    ): (60 * 10),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="supervision_unit_period_span_aggregated_metrics",
    ): (60 * 10),
    # TODO(#29291): Improve the performance of this view
    BigQueryAddress(
        dataset_id="aggregated_metrics",
        table_id="incarceration_facility_assignment_span_aggregated_metrics",
    ): (60 * 10),
    # TODO(#25452) Improve performance of supervision_remaining_sentences
    BigQueryAddress(
        dataset_id="population_projection_data",
        table_id="supervision_remaining_sentences",
    ): (60 * 10),
}


def get_deployed_view_dag_update_perf_config() -> ProcessDagPerfConfig:
    """Returns perf configuration for any process_dag() calls that materializes views"""

    node_max_processing_time_seconds = _MAX_SINGLE_VIEW_MATERIALIZATION_TIME_SECONDS
    if environment.in_gcp():
        # Add extra buffer when in GCP because sometimes materialization takes longer
        # due to contention with other AppEngine processes.
        node_max_processing_time_seconds = 60 + node_max_processing_time_seconds

    return ProcessDagPerfConfig(
        node_max_processing_time_seconds=node_max_processing_time_seconds,
        node_allowed_process_time_overrides=_ALLOWED_MATERIALIZATION_TIME_OVERRIDES,
    )
