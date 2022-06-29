# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Validation view configuration."""

from typing import List, Sequence

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.validation.views.external_data.county_jail_population_person_level import (
    COUNTY_JAIL_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_admission_person_level import (
    INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_population_by_facility import (
    INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_population_person_level import (
    INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_release_person_level import (
    INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.population_projection_monthly_population import (
    POPULATION_PROJECTION_MONTHLY_POPULATION_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.population_projection_monthly_population_per_facility import (
    POPULATION_PROJECTION_MONTHLY_POPULATION_PER_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.recidivism_person_level import (
    RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_co.incarceration_population_person_level import (
    US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.cb_971_report_unified import (
    CB_971_REPORT_UNIFIED_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.incarceration_population_by_facility import (
    US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.oor_report_unified import (
    OOR_REPORT_UNIFIED_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_early_discharge_person_level import (
    SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_population_person_level import (
    SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_start_person_level import (
    SUPERVISION_START_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_termination_person_level import (
    SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.metadata.column_counter import (
    ValidationTableColumnCounterBigQueryViewCollector,
)
from recidiviz.validation.views.metadata.validation_schema_config import (
    get_external_validation_schema,
)
from recidiviz.validation.views.state.po_report_clients import (
    PO_REPORT_CLIENTS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_admission_external_prod_staging_comparison import (
    INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_population_external_prod_staging_comparison import (
    INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_release_external_prod_staging_comparison import (
    INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.sessions_justice_counts_comparison import (
    SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.sessions_justice_counts_prod_staging_comparison import (
    SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.supervision_population_external_prod_staging_comparison import (
    SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.supervision_start_external_prod_staging_comparison import (
    SUPERVISION_START_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.supervision_termination_external_prod_staging_comparison import (
    SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions_validation.reincarcerations_from_dataflow_to_dataflow_disaggregated import (
    REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions_validation.reincarcerations_from_sessions_to_dataflow_disaggregated import (
    REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions_validation.revocation_sessions_to_dataflow_disaggregated import (
    REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions_validation.session_incarceration_admissions_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_incarceration_population_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_incarceration_releases_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_supervision_out_of_state_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_supervision_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_supervision_starts_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.state.sessions_validation.session_supervision_terminations_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
)
from recidiviz.validation.views.validation_views import (
    get_generated_validation_view_builders,
)

_CROSS_PROJECT_VALIDATION_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_START_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_BUILDER,
]

EXTERNAL_VALIDATION_DATA_VIEW_BUILDERS = [
    COUNTY_JAIL_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
    INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
    INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
    INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
    INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
    POPULATION_PROJECTION_MONTHLY_POPULATION_PER_FACILITY_VIEW_BUILDER,
    POPULATION_PROJECTION_MONTHLY_POPULATION_VIEW_BUILDER,
    RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER,
    SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
    SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
    SUPERVISION_START_PERSON_LEVEL_VIEW_BUILDER,
    SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
    CB_971_REPORT_UNIFIED_VIEW_BUILDER,
    OOR_REPORT_UNIFIED_VIEW_BUILDER,
    US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
    US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER,
]


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    return (
        [
            PO_REPORT_CLIENTS_VIEW_BUILDER,
            SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_SUPERVISION_STARTS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
            REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
            REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
            REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
            SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER,
        ]
        + _CROSS_PROJECT_VALIDATION_VIEW_BUILDERS
        + EXTERNAL_VALIDATION_DATA_VIEW_BUILDERS
        + get_generated_validation_view_builders()
    )


VALIDATION_METADATA_BUILDERS: Sequence[
    BigQueryViewBuilder
] = ValidationTableColumnCounterBigQueryViewCollector(
    schema_config=get_external_validation_schema()
).collect_view_builders()


METADATA_VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[
    BigQueryViewBuilder
] = VALIDATION_METADATA_BUILDERS
