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
"""Views that are regularly updated with the deploy and rematerialized with metric exports.."""
import itertools
from typing import Dict, List, Set

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.county.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as COUNTY_VIEW_BUILDERS,
)
from recidiviz.calculator.query.experiments.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXPERIMENTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.externally_shared_views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXTERNALLY_SHARED_VIEW_BUILDERS,
)
from recidiviz.calculator.query.justice_counts.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as JUSTICE_COUNTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as STATE_VIEW_BUILDERS,
)
from recidiviz.case_triage.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as CASE_TRIAGE_VIEW_BUILDERS,
)
from recidiviz.ingest.direct.views.view_config import (
    get_view_builders_for_views_to_update as get_direct_ingest_view_builders,
)
from recidiviz.ingest.views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as INGEST_METADATA_VIEW_BUILDERS,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.task_eligibility.view_config import (
    get_view_builders_for_views_to_update as get_task_eligibility_view_builders,
)
from recidiviz.utils import environment
from recidiviz.validation.views.view_config import (
    METADATA_VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as VALIDATION_METADATA_VIEW_BUILDERS,
)
from recidiviz.validation.views.view_config import (
    get_view_builders_for_views_to_update as get_validation_view_builders,
)


def _all_deployed_view_builders() -> List[BigQueryViewBuilder]:
    return list(
        itertools.chain(
            CASE_TRIAGE_VIEW_BUILDERS,
            COUNTY_VIEW_BUILDERS,
            get_direct_ingest_view_builders(),
            EXPERIMENTS_VIEW_BUILDERS,
            EXTERNALLY_SHARED_VIEW_BUILDERS,
            JUSTICE_COUNTS_VIEW_BUILDERS,
            INGEST_METADATA_VIEW_BUILDERS,
            STATE_VIEW_BUILDERS,
            get_task_eligibility_view_builders(),
            get_validation_view_builders(),
            VALIDATION_METADATA_VIEW_BUILDERS,
        )
    )


def deployed_view_builders(project_id: str) -> List[BigQueryViewBuilder]:
    return [
        builder
        for builder in _all_deployed_view_builders()
        if builder.should_deploy_in_project(project_id)
    ]


# Full list of all deployed view builders
@environment.local_only
def all_deployed_view_builders() -> List[BigQueryViewBuilder]:
    return _all_deployed_view_builders()


# A list of all datasets that have ever held managed views that were updated by our
# deploy process. This list is used to identify places where we should look for
# legacy views that we need to clean up.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY
# DELETED FROM BOTH PROD AND STAGING.
DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED: Set[str] = {
    "analyst_data",
    "case_triage",
    "census_managed_views",
    "covid_public_data",
    "dashboard_views",
    "dataflow_metrics_materialized",
    "experiments",
    "externally_shared_views",
    "ingest_metadata",
    "justice_counts",
    "justice_counts_corrections",
    "justice_counts_dashboard",
    "justice_counts_jails",
    "linestaff_data_validation",
    "overdue_discharge_alert",
    "partner_data_csg",
    "po_report_views",
    "population_projection_data",
    "practices_views",
    "workflows_views",
    "public_dashboard_views",
    "reference_views",
    "sessions",
    "shared_metric_views",
    "task_eligibility",
    "task_eligibility_candidates_general",
    "task_eligibility_criteria_general",
    "task_eligibility_criteria_us_nd",
    "task_eligibility_spans_us_nd",
    "us_ca_raw_data_up_to_date_views",
    "us_co_raw_data_up_to_date_views",
    "us_id_raw_data_up_to_date_views",
    "us_mo_raw_data_up_to_date_views",
    "us_nd_raw_data_up_to_date_views",
    "us_pa_raw_data_up_to_date_views",
    "us_tn_raw_data_up_to_date_views",
    "us_me_raw_data_up_to_date_views",
    "us_mi_raw_data_up_to_date_views",
    "us_oz_raw_data_up_to_date_views",
    "us_co_validation",
    "us_mi_validation",
    "validation_external_accuracy",
    "validation_metadata",
    "validation_views",
    "vitals_report_views",
}


# A list of all datasets that have ever held managed views that were updated by our
# process that refreshes the data from CloudSQL into Bigquery. This list is used to
# identify places where we should look for legacy views that we need to clean up.
# NOTE: This list DOES NOT contain the unioned regional datasets for federated
# refreshes of state-segmented databases. Those datasets are stored in
# CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY
# DELETED FROM BOTH PROD AND STAGING.
CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA: Dict[
    SchemaType, Set[str]
] = {
    SchemaType.JAILS: {
        "census_regional",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "jails_cloudsql_connection",
        "jails_v2_cloudsql_connection",
    },
    SchemaType.CASE_TRIAGE: {
        "case_triage_cloudsql_connection",
        "case_triage_federated_regional",
    },
    SchemaType.STATE: {
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_id_primary_cloudsql_connection",
        "state_v2_us_id_primary_cloudsql_connection",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_mi_primary_cloudsql_connection",
        "state_v2_us_mi_primary_cloudsql_connection",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_mo_primary_cloudsql_connection",
        "state_v2_us_mo_primary_cloudsql_connection",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_nd_primary_cloudsql_connection",
        "state_v2_us_nd_primary_cloudsql_connection",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_pa_primary_cloudsql_connection",
        "state_v2_us_pa_primary_cloudsql_connection",
        "state_v2_us_co_primary_cloudsql_connection",
        # TODO(#8282): Remove this once we delete the v1 databases.
        "state_us_tn_primary_cloudsql_connection",
        "state_v2_us_tn_primary_cloudsql_connection",
        "state_v2_us_me_primary_cloudsql_connection",
        "state_v2_us_ca_primary_cloudsql_connection",
        "state_v2_us_oz_primary_cloudsql_connection",
        # TODO(#10703): Remove this after merging US_IX into US_ID
        "state_v2_us_ix_primary_cloudsql_connection",
        "us_ca_state_regional",
        "us_co_state_regional",
        "us_id_state_regional",
        # TODO(#10703): Remove this after merging US_IX into US_ID
        "us_ix_state_regional",
        "us_mi_state_regional",
        "us_mo_state_regional",
        "us_nd_state_regional",
        "us_oz_state_regional",
        "us_pa_state_regional",
        "us_tn_state_regional",
        "us_me_state_regional",
    },
    SchemaType.OPERATIONS: {
        # TODO(#8282): Remove this once we delete the v1 databases.
        "operations_cloudsql_connection",
        "operations_v2_cloudsql_connection",
        "us_ca_operations_regional",
        "us_co_operations_regional",
        "us_id_operations_regional",
        # TODO(#10703): Remove this after merging US_IX into US_ID
        "us_ix_operations_regional",
        "us_mi_operations_regional",
        "us_mo_operations_regional",
        "us_nd_operations_regional",
        "us_oz_operations_regional",
        "us_pa_operations_regional",
        "us_tn_operations_regional",
        "us_me_operations_regional",
    },
}


# A list of all datasets that have ever held managed views that were updated by the
# step in our CloudSQL into Bigquery refresh process that unions the contents of
# state-segmented datasets and copies the results to a dataset that lives in the same
# region as the CloudSQL instance. This list is used to identify places where we
# should look for legacy views that we need to clean up.
# NOTE: This list DOES NOT contain all datasets used in the CloudSQL to BigQuery
# refresh process. The rest of the datasets are stored in
# CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA.
# DO NOT DELETE ITEMS FROM THIS LIST UNLESS YOU KNOW THIS DATASET HAS BEEN FULLY
# DELETED FROM BOTH PROD AND STAGING.
CLOUDSQL_UNIONED_REGIONAL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA: Dict[
    SchemaType, Set[str]
] = {
    SchemaType.STATE: {
        "state_regional",
    },
    SchemaType.OPERATIONS: {
        "operations_regional",
    },
}
