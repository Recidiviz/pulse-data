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
"""Views that are regularly updated and materialized as necessary via the deploy, and
may be referenced in product exports.
"""
import itertools
import logging
from typing import Dict, List, Set

from recidiviz.aggregated_metrics.view_config import (
    get_aggregated_metrics_view_builders,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.experiments_metadata.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXPERIMENTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.externally_shared_views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXTERNALLY_SHARED_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as STATE_VIEW_BUILDERS,
)
from recidiviz.datasets.static_data.views.view_config import (
    get_static_data_view_builders,
)
from recidiviz.ingest.direct.views.view_config import (
    get_view_builders_for_views_to_update as get_direct_ingest_view_builders,
)
from recidiviz.ingest.views.view_config import (
    get_view_builders_for_views_to_update as get_ingest_infra_view_builders,
)
from recidiviz.monitoring.platform_kpis.view_config import (
    get_platform_kpi_views_to_update,
)
from recidiviz.observations.view_config import (
    get_view_builders_for_views_to_update as get_observations_view_builders,
)
from recidiviz.outcome_metrics.view_config import (
    get_transitions_view_builders_for_views_to_update as get_transitions_view_builders,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.segment.view_config import (
    get_view_builders_for_views_to_update as get_segment_view_builders,
)
from recidiviz.task_eligibility.view_config import (
    get_view_builders_for_views_to_update as get_task_eligibility_view_builders,
)
from recidiviz.utils import environment, metadata
from recidiviz.validation.views.view_config import (
    build_validation_metadata_view_builders,
)
from recidiviz.validation.views.view_config import (
    get_view_builders_for_views_to_update as get_validation_view_builders,
)


def _all_deployed_view_builders() -> List[BigQueryViewBuilder]:
    logging.info("Gathering all deployed view builders...")
    return list(
        itertools.chain(
            get_aggregated_metrics_view_builders(),
            get_direct_ingest_view_builders(),
            EXPERIMENTS_VIEW_BUILDERS,
            EXTERNALLY_SHARED_VIEW_BUILDERS,
            get_ingest_infra_view_builders(),
            get_observations_view_builders(),
            get_segment_view_builders(),
            STATE_VIEW_BUILDERS,
            get_task_eligibility_view_builders(),
            get_validation_view_builders(),
            build_validation_metadata_view_builders(),
            get_platform_kpi_views_to_update(),
            get_transitions_view_builders(),
            get_static_data_view_builders(),
        )
    )


def deployed_view_builders() -> List[BigQueryViewBuilder]:
    """Returns the set of view builders which can/should be deployed to the current
    project (as defined by metadata.project_id()).
    """
    return [
        builder
        for builder in _all_deployed_view_builders()
        if builder.should_deploy_in_project(metadata.project_id())
    ]


@environment.local_only
def all_deployed_view_builders() -> List[BigQueryViewBuilder]:
    """Returns a list of all view builders for views that are candidates for deploy.
    Some of these views may not actually get deployed to a given project based on
    the builder configuration.
    """
    return _all_deployed_view_builders()


# A list of all datasets that have ever held managed views that were updated by our
# deploy process. This list is used to identify places where we should look for
# legacy views that we need to clean up.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY
# DELETED FROM BOTH PROD AND STAGING.
DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED: Set[str] = {
    "aggregated_metrics",
    "analyst_data",
    "case_notes_prototype_views",
    "case_triage",
    "census_managed_views",
    "compliance_task_eligibility_spans",
    "compliance_task_eligibility_spans_us_ne",
    "compliance_task_eligibility_spans_us_tx",
    "covid_public_data",
    "dashboard_views",
    "dataflow_metrics_materialized",
    "experiments",
    "experiments_metadata",
    "external_reference_views",
    "externally_shared_views",
    "impact_dashboard",
    "impact_reports",
    "ingest_metadata",
    "justice_counts",
    "justice_counts_corrections",
    "justice_counts_dashboard",
    "justice_counts_jails",
    "lantern_revocations_matrix",
    "linestaff_data_validation",
    "meetings",
    "normalized_state",
    "normalized_state_views",
    "outliers_views",
    "overdue_discharge_alert",
    "partner_data_csg",
    "platform_kpis",
    "po_report_views",
    "population_projection_data",
    "practices_views",
    "workflows_views",
    "public_dashboard_views",
    "reference_views",
    "observations__officer_event",
    "observations__officer_span",
    "observations__person_event",
    "observations__person_span",
    "observations__workflows_surfaceable_caseload_span",
    "observations__workflows_surfaceable_caseload_event",
    "observations__workflows_primary_user_event",
    "observations__workflows_primary_user_span",
    "observations__workflows_provisioned_user_span",
    "observations__insights_primary_user_event",
    "observations__insights_primary_user_span",
    "observations__insights_provisioned_user_span",
    "observations__tasks_primary_user_event",
    "observations__tasks_primary_user_span",
    "observations__tasks_provisioned_user_span",
    "observations__global_provisioned_user_span",
    "observations__global_provisioned_user_event",
    "observations__jii_tablet_app_provisioned_user_event",
    "observations__jii_tablet_app_provisioned_user_span",
    "reentry",
    "segment_events",
    "segment_events__case_note_search",
    "segment_events__client_page",
    "segment_events__milestones",
    "segment_events__psi_case_insights",
    "segment_events__supervisor_homepage_operations_module",
    "segment_events__supervisor_homepage_opportunities_module",
    "segment_events__supervisor_homepage_outcomes_module",
    "segment_events__tasks",
    "segment_events__workflows",
    "sessions",
    "sessions_validation",
    "sentence_sessions",
    "sentence_sessions_v2_all",
    "sentencing_views",
    "spark_public_output_data_most_recent",
    "state",
    "state_views",
    "static_reference_data_views",
    "shared_metric_views",
    "tasks_views",
    "task_eligibility",
    "task_eligibility_candidates_general",
    "task_eligibility_candidates_us_mi",
    "task_eligibility_candidates_us_tx",
    "task_eligibility_completion_events",
    "task_eligibility_completion_events_general",
    "task_eligibility_completion_events_us_nd",
    "task_eligibility_completion_events_us_ma",
    "task_eligibility_completion_events_us_mi",
    "task_eligibility_completion_events_us_mo",
    "task_eligibility_completion_events_us_me",
    "task_eligibility_completion_events_us_ne",
    "task_eligibility_completion_events_us_ix",
    "task_eligibility_completion_events_us_tn",
    "task_eligibility_completion_events_us_or",
    "task_eligibility_completion_events_us_pa",
    "task_eligibility_completion_events_us_az",
    "task_eligibility_completion_events_us_tx",
    "task_eligibility_completion_events_us_ar",
    "task_eligibility_completion_events_us_ia",
    "task_eligibility_criteria_general",
    "task_eligibility_criteria_us_ar",
    "task_eligibility_criteria_us_ia",
    "task_eligibility_criteria_us_id",
    "task_eligibility_criteria_us_ix",
    "task_eligibility_criteria_us_ca",
    "task_eligibility_criteria_us_nd",
    "task_eligibility_criteria_us_tn",
    "task_eligibility_criteria_us_ma",
    "task_eligibility_criteria_us_me",
    "task_eligibility_criteria_us_mi",
    "task_eligibility_criteria_us_mo",
    "task_eligibility_criteria_us_ne",
    "task_eligibility_criteria_us_co",
    "task_eligibility_criteria_us_pa",
    "task_eligibility_criteria_us_or",
    "task_eligibility_criteria_us_az",
    "task_eligibility_criteria_us_tx",
    "task_eligibility_criteria_us_ut",
    "task_eligibility_spans_us_co",
    "task_eligibility_spans_us_ca",
    "task_eligibility_spans_us_pa",
    "task_eligibility_spans_us_ia",
    "task_eligibility_spans_us_id",
    "task_eligibility_spans_us_ix",
    "task_eligibility_spans_us_nd",
    "task_eligibility_spans_us_tn",
    "task_eligibility_spans_us_ma",
    "task_eligibility_spans_us_me",
    "task_eligibility_spans_us_mi",
    "task_eligibility_spans_us_mo",
    "task_eligibility_spans_us_ne",
    "task_eligibility_spans_us_or",
    "task_eligibility_spans_us_ar",
    "task_eligibility_spans_us_az",
    "task_eligibility_spans_us_tx",
    "task_eligibility_spans_us_ut",
    "unit_of_analysis_assignments_by_time_period",
    "transitions",
    "user_metrics",
    "us_ar_raw_data_up_to_date_views",
    "us_ar_raw_data_up_to_date_views_secondary",
    "us_ca_raw_data_up_to_date_views",
    "us_ca_raw_data_up_to_date_views_secondary",
    "us_co_raw_data_up_to_date_views",
    "us_co_raw_data_up_to_date_views_secondary",
    "us_ia_raw_data_up_to_date_views",
    "us_ia_raw_data_up_to_date_views_secondary",
    "us_id_raw_data_up_to_date_views",
    "us_id_raw_data_up_to_date_views_secondary",
    "us_ix_raw_data_up_to_date_views",
    "us_ix_raw_data_up_to_date_views_secondary",
    "us_mo_raw_data_up_to_date_views",
    "us_mo_raw_data_up_to_date_views_secondary",
    "us_nc_raw_data_up_to_date_views",
    "us_nc_raw_data_up_to_date_views_secondary",
    "us_nd_raw_data_up_to_date_views",
    "us_nd_raw_data_up_to_date_views_secondary",
    "us_ny_raw_data_up_to_date_views",
    "us_ny_raw_data_up_to_date_views_secondary",
    "us_pa_raw_data_up_to_date_views",
    "us_pa_raw_data_up_to_date_views_secondary",
    "us_tn_raw_data_up_to_date_views",
    "us_tn_raw_data_up_to_date_views_secondary",
    "us_ma_raw_data_up_to_date_views",
    "us_ma_raw_data_up_to_date_views_secondary",
    "us_me_raw_data_up_to_date_views",
    "us_me_raw_data_up_to_date_views_secondary",
    "us_mi_raw_data_up_to_date_views",
    "us_mi_raw_data_up_to_date_views_secondary",
    "us_ne_raw_data_up_to_date_views",
    "us_ne_raw_data_up_to_date_views_secondary",
    "us_or_raw_data_up_to_date_views",
    "us_or_raw_data_up_to_date_views_secondary",
    "us_oz_raw_data_up_to_date_views",
    "us_oz_raw_data_up_to_date_views_secondary",
    "us_az_raw_data_up_to_date_views",
    "us_az_raw_data_up_to_date_views_secondary",
    "us_tx_raw_data_up_to_date_views",
    "us_tx_raw_data_up_to_date_views_secondary",
    "us_ut_raw_data_up_to_date_views",
    "us_ut_raw_data_up_to_date_views_secondary",
    "us_ar_raw_data_views",
    "us_ar_raw_data_views_secondary",
    "us_ca_raw_data_views",
    "us_ca_raw_data_views_secondary",
    "us_co_raw_data_views",
    "us_co_raw_data_views_secondary",
    "us_ia_raw_data_views",
    "us_ia_raw_data_views_secondary",
    "us_id_raw_data_views",
    "us_id_raw_data_views_secondary",
    "us_ix_raw_data_views",
    "us_ix_raw_data_views_secondary",
    "us_mo_raw_data_views",
    "us_mo_raw_data_views_secondary",
    "us_nc_raw_data_views",
    "us_nc_raw_data_views_secondary",
    "us_nd_raw_data_views",
    "us_nd_raw_data_views_secondary",
    "us_ny_raw_data_views",
    "us_ny_raw_data_views_secondary",
    "us_pa_raw_data_views",
    "us_pa_raw_data_views_secondary",
    "us_tn_raw_data_views",
    "us_tn_raw_data_views_secondary",
    "us_ma_raw_data_views",
    "us_ma_raw_data_views_secondary",
    "us_me_raw_data_views",
    "us_me_raw_data_views_secondary",
    "us_mi_raw_data_views",
    "us_mi_raw_data_views_secondary",
    "us_ne_raw_data_views",
    "us_ne_raw_data_views_secondary",
    "us_or_raw_data_views",
    "us_or_raw_data_views_secondary",
    "us_oz_raw_data_views",
    "us_oz_raw_data_views_secondary",
    "us_az_raw_data_views",
    "us_az_raw_data_views_secondary",
    "us_tx_raw_data_views",
    "us_tx_raw_data_views_secondary",
    "us_ut_raw_data_views",
    "us_ut_raw_data_views_secondary",
    "us_co_validation",
    "us_ix_validation",
    "us_ma_validation",
    "us_me_validation",
    "us_mi_validation",
    "us_mo_validation",
    "us_ne_validation",
    "us_nd_validation",
    "us_oz_validation",
    "us_tn_validation",
    "us_pa_validation",
    "us_az_validation",
    "us_tx_validation",
    "us_ut_validation",
    "validation_external_accuracy",
    "validation_metadata",
    "validation_views",
    "vitals_report_views",
    "jii_texting",
}


# A list of all datasets that have ever held managed views that were updated by our
# process that refreshes the data from CloudSQL into Bigquery. This list is used to
# identify places where we should look for legacy views that we need to clean up.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY
# DELETED FROM BOTH PROD AND STAGING.
CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA: Dict[
    SchemaType, Set[str]
] = {
    SchemaType.CASE_TRIAGE: {
        "case_triage_cloudsql_connection",
        "case_triage_federated_regional",
    },
    SchemaType.OPERATIONS: {
        # TODO(#8282): Remove this once we delete the v1 databases.
        "operations_cloudsql_connection",
        "operations_v2_cloudsql_connection",
        "us_ar_operations_regional",
        "us_ca_operations_regional",
        "us_co_operations_regional",
        "us_ia_operations_regional",
        "us_id_operations_regional",
        # TODO(#10703): Remove this after merging US_IX into US_ID
        "us_ix_operations_regional",
        "us_ma_operations_regional",
        "us_mi_operations_regional",
        "us_mo_operations_regional",
        "us_ne_operations_regional",
        "us_nc_operations_regional",
        "us_nd_operations_regional",
        "us_or_operations_regional",
        "us_oz_operations_regional",
        "us_pa_operations_regional",
        "us_tn_operations_regional",
        "us_me_operations_regional",
        "us_az_operations_regional",
        "us_tx_operations_regional",
        "operations_regional",
    },
}
