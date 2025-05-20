# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines helpers that give us insight into the schemas of our deployed views and
tables.
"""
from functools import cache

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_action_queue import (
    US_AZ_ACTION_QUEUE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_home_plan_preprocessed import (
    US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ca.us_ca_sustainable_housing_status_periods import (
    US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_supervision_level_raw_text_mappings import (
    US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_mosop_prio_groups import (
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_program_tracks import (
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_sentencing_dates_preprocessed import (
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_contact_comments_preprocessed import (
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_relevant_contact_codes import (
    US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.population_projection.population_projection_outputs import (
    POPULATION_PROJECTION_OUTPUT_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.population_projection.simulation_run_dates import (
    SIMULATION_RUN_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.population_projection.spark.cost_avoidance_estimate_most_recent import (
    SPARK_COST_AVOIDANCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.population_projection.spark.cost_avoidance_non_cumulative_estimate_most_recent import (
    SPARK_COST_AVOIDANCE_NON_CUMULATIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.population_projection.spark.life_years_estimate_most_recent import (
    SPARK_LIFE_YEARS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.population_projection.spark.population_estimate_most_recent import (
    SPARK_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_notes_data_store import (
    CASE_NOTES_DATA_STORE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.cleaned_offense_description_to_labels import (
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.completion_event_type_metadata import (
    get_completion_event_metadata_view_builder,
)
from recidiviz.calculator.query.state.views.sessions.admission_start_reason_dedup_priority import (
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_level_dedup_priority import (
    ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_scoring_key import (
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.cohort_month_index import (
    COHORT_MONTH_INDEX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_dedup_priority import (
    COMPARTMENT_LEVEL_1_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_2_dedup_priority import (
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_dedup_priority import (
    CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.release_termination_reason_dedup_priority import (
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.state_staff_role_subtype_dedup_priority import (
    STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_dedup_priority import (
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ar.resident_metadata import (
    US_AR_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ix.resident_metadata import (
    US_IX_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ma.resident_metadata import (
    US_MA_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_me.resident_metadata import (
    US_ME_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_mo.resident_metadata import (
    US_MO_RESIDENT_METADATA_VIEW_VIEW_BUILDER,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
    DirectIngestRawDataTableLatestViewCollector,
)
from recidiviz.monitoring.platform_kpis.cost.bq_monthly_costs_by_dataset import (
    BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.stale_metric_exports import (
    STALE_METRIC_EXPORTS_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.velocity.dag_runtimes import (
    DAG_RUNTIMES_VIEW_BUILDER,
)
from recidiviz.outcome_metrics.views.transitions_metric_utils import (
    collect_view_builders_for_breadth_depth_metrics,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type
from recidiviz.view_registry.deployed_views import deployed_view_builders

STATE_CODE_COLUMN_NAME = "state_code"

# Views that provide simple mappings that can be used in joins which we do not expect to
#  have a state_code column. These tables will generally have no parent tables, instead
#  generating data via an UNNEST statement or other static means. If these tables do
#  have parent tables, they should be source tables that also don't have a state_code
#  column.
STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS = {
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER.address,
    ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.address,
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER.address,
    CLEANED_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_BUILDER.address,
    COHORT_MONTH_INDEX_VIEW_BUILDER.address,
    COMPARTMENT_LEVEL_1_DEDUP_PRIORITY_VIEW_BUILDER.address,
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER.address,
    CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.address,
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER.address,
    SIMULATION_RUN_DATES_VIEW_BUILDER.address,
    STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER.address,
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER.address,
    get_completion_event_metadata_view_builder().address,
}


def state_specific_deployed_views_without_state_code_columns(
    deployed_view_builders_by_address: dict[BigQueryAddress, BigQueryViewBuilder]
) -> set[BigQueryAddress]:
    """Returns the addresses for state-specific views that we do not expect to have a
    state_code column. While it's generally good to add a state_code column to views
    that contain data for a single state, it's not extremely concerning to add
    exemptions here if you have a good reason.
    """
    # Raw data tables / associated latest views generally do not have `state_code`
    # columns, but it is not an issue if one does. Only include ones that do not.
    raw_data_views_no_state_code_column = {
        latest_view_builder.address
        for instance in DirectIngestInstance
        for state_code in get_existing_direct_ingest_states()
        for latest_view_builder in DirectIngestRawDataTableLatestViewCollector(
            region_code=state_code.value,
            raw_data_source_instance=instance,
            # Regardless of whether a raw data table is properly documented, do
            # not ingest.
            filter_to_documented=False,
        ).collect_view_builders()
        if (
            STATE_CODE_COLUMN_NAME
            not in {
                c.name
                for c in assert_type(
                    latest_view_builder, DirectIngestRawDataTableLatestViewBuilder
                ).raw_file_config.current_columns
            }
        )
    }

    missing_state_code_col_addresses = {
        *raw_data_views_no_state_code_column,
        US_AR_RESIDENT_METADATA_VIEW_VIEW_BUILDER.address,
        US_AZ_ACTION_QUEUE_VIEW_BUILDER.address,
        US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER.address,
        US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER.address,
        US_IX_RESIDENT_METADATA_VIEW_BUILDER.address,
        US_MA_RESIDENT_METADATA_VIEW_BUILDER.address,
        US_ME_RESIDENT_METADATA_VIEW_BUILDER.address,
        US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER.address,
        US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER.address,
        US_MO_PROGRAM_TRACKS_VIEW_BUILDER.address,
        US_MO_RESIDENT_METADATA_VIEW_VIEW_BUILDER.address,
        US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER.address,
        US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER.address,
        US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER.address,
    }

    return {
        a
        for a in deployed_view_builders_by_address
        if a in missing_state_code_col_addresses
    }


def state_agnostic_deployed_views_without_state_code_column(
    deployed_view_builders_by_address: dict[BigQueryAddress, BigQueryViewBuilder]
) -> set[BigQueryAddress]:
    """Returns the addresses for state-agnostic views that we do not expect to have a
    state_code column. This list should be added to *very* sparingly - generally, all
    views that have rows that can each be attributed to a single state should have a
    state_code column. Views in this list cannot be filtered by state_code when loading
    data for a single state to a sandbox.
    """
    missing_state_code_col_addresses = {
        *STATE_AGNOSTIC_MAPPINGS_VIEWS_WITHOUT_STATE_CODE_COLUMNS,
        # These views produce inputs to Spark population projection modeling, which
        # expects a certain input schema
        *{vb.address for vb in POPULATION_PROJECTION_OUTPUT_VIEW_BUILDERS},
        # These views do simple processing on Spark modeling outputs, which do not
        # produce results with state_code columns.
        SPARK_COST_AVOIDANCE_VIEW_BUILDER.address,
        SPARK_COST_AVOIDANCE_NON_CUMULATIVE_VIEW_BUILDER.address,
        SPARK_POPULATION_VIEW_BUILDER.address,
        SPARK_LIFE_YEARS_VIEW_BUILDER.address,
        # This view backs the Vertex AI for case note search and is not allowed to have a state_code column (only allowed to have id and jsonData columns)
        CASE_NOTES_DATA_STORE_VIEW_BUILDER.address,
        # These views look at the platform as a whole, not breaking data down by state.
        STALE_METRIC_EXPORTS_VIEW_BUILDER.address,
        BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER.address,
        DAG_RUNTIMES_VIEW_BUILDER.address,
        # These views calculate cross-state metrics for orgwide impact tracking
        *[
            builder.address
            for builder in collect_view_builders_for_breadth_depth_metrics(
                metric_year=2024, attribute_cols=["product_transition_type"]
            )
        ],
        *[
            builder.address
            for builder in collect_view_builders_for_breadth_depth_metrics(
                metric_year=2024,
                attribute_cols=[
                    "decarceral_impact_type",
                    "has_mandatory_due_date",
                    "is_jii_transition",
                ],
            )
        ],
        *[
            builder.address
            for builder in collect_view_builders_for_breadth_depth_metrics(
                metric_year=2025, attribute_cols=["product_transition_type"]
            )
        ],
        *[
            builder.address
            for builder in collect_view_builders_for_breadth_depth_metrics(
                metric_year=2025,
                attribute_cols=[
                    "decarceral_impact_type",
                    "has_mandatory_due_date",
                    "is_jii_transition",
                ],
            )
        ],
    }

    return {
        a
        for a in deployed_view_builders_by_address
        if a in missing_state_code_col_addresses
    }


@cache
def get_deployed_addresses_without_state_code_column(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns all the addresses involved in the full deployed view graph (view
    addresses, materialized table addresses, source table addresses) which we expect
    to not have any state_code column.
    """
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match metadata.project_id() "
            f"[{metadata.project_id()}]"
        )

    source_table_configs_by_address = (
        build_source_table_repository_for_collected_schemata(
            project_id=project_id
        ).source_tables
    )
    deployed_view_builders_by_address = {
        vb.address: vb for vb in deployed_view_builders()
    }

    views_no_state_code = state_specific_deployed_views_without_state_code_columns(
        deployed_view_builders_by_address
    ) | state_agnostic_deployed_views_without_state_code_column(
        deployed_view_builders_by_address
    )

    materialized_tables_no_state_code = {
        deployed_view_builders_by_address[view_address].table_for_query
        for view_address in views_no_state_code
    }

    source_tables_no_state_code = {
        a
        for a, config in source_table_configs_by_address.items()
        if not config.has_column(STATE_CODE_COLUMN_NAME)
    }

    return (
        source_tables_no_state_code
        | views_no_state_code
        | materialized_tables_no_state_code
    )


def get_source_tables_to_pseudocolumns() -> dict[BigQueryAddress, list[str]]:
    """Returns a map of source table addresses to any pseudocolumns present on that
    table.
    """
    source_table_configs_by_address = (
        build_source_table_repository_for_collected_schemata(
            project_id=metadata.project_id()
        ).source_tables
    )

    return {
        a: [c.name for c in config.pseudocolumns]
        for a, config in source_table_configs_by_address.items()
        if config.pseudocolumns
    }
