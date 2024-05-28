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
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.views.external_data.county_jail_population_person_level import (
    COUNTY_JAIL_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_admission_person_level import (
    get_incarceration_admission_person_level_view_builder,
)
from recidiviz.validation.views.external_data.incarceration_population_by_facility import (
    INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.incarceration_population_person_level import (
    get_incarceration_population_person_level_view_builder,
)
from recidiviz.validation.views.external_data.incarceration_release_person_level import (
    get_incarceration_release_person_level_view_builder,
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
    US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.county_jail_incarceration_population_person_level import (
    US_IX_COUNTY_JAIL_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.incarceration_admission_person_level import (
    US_IX_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.incarceration_population_by_facility import (
    US_IX_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.incarceration_population_person_level import (
    US_IX_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.incarceration_release_person_level import (
    US_IX_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.population_projection_monthly_population import (
    US_IX_POPULATION_PROJECTION_MONTLY_POPULATION_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.population_projection_monthly_population_per_facility import (
    US_IX_POPULATION_PROJECTION_MONTLY_POPULATION_PER_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.supervision_early_discharge_person_level import (
    US_IX_SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.supervision_population_person_level import (
    US_IX_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.supervision_start_person_level import (
    US_IX_SUPERVISION_START_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_ix.supervision_termination_person_level import (
    US_IX_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.incarceration_population_by_facility import (
    US_ME_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.incarceration_population_person_level import (
    US_ME_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.incarceration_release_person_level import (
    US_ME_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.population_releases import (
    US_ME_POPULATION_RELEASES_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.supervision_population_person_level import (
    US_ME_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_me.supervision_termination_person_level import (
    US_ME_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.cb_971_report_supervision_unified import (
    CB_971_REPORT_SUPERVISION_UNIFIED_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.cb_971_report_unified import (
    CB_971_REPORT_UNIFIED_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.incarceration_population_by_facility import (
    US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.incarceration_population_person_level import (
    US_MI_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mi.supervision_population_by_type import (
    US_MI_SUPERVISION_POPULATION_BY_TYPE_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mo.incarceration_population_by_facility import (
    US_MO_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_mo.supervision_population_person_level import (
    US_MO_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_nd.incarceration_population_by_facility import (
    US_ND_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_nd.incarceration_population_person_level import (
    US_ND_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_nd.recidivism_person_level import (
    US_ND_RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_nd.supervision_population_person_level import (
    US_ND_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_oz.incarceration_population_person_level import (
    US_OZ_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_pa.incarceration_admission_person_level import (
    US_PA_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_pa.incarceration_population_person_level import (
    US_PA_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_pa.incarceration_release_person_level import (
    US_PA_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_pa.supervision_population_person_level import (
    US_PA_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_pa.supervision_termination_person_level import (
    US_PA_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_tn.incarceration_population_by_facility import (
    US_TN_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_tn.incarceration_population_person_level import (
    US_TN_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_tn.supervision_population_person_level import (
    US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_early_discharge_person_level import (
    SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.supervision_population_by_type import (
    get_supervision_population_by_type_view_builder,
)
from recidiviz.validation.views.external_data.supervision_population_person_level import (
    get_supervision_population_person_level_view_builder,
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
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_admission_external_prod_staging_comparison import (
    INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_population_external_prod_staging_comparison import (
    INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.prod_staging_comparison.incarceration_release_external_prod_staging_comparison import (
    INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
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

CROSS_PROJECT_VALIDATION_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_START_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
    SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
]


def get_view_builders_from_configured_validations() -> List[SimpleBigQueryViewBuilder]:
    # Creating set to remove possibility of duplicate view builders from validation checks list,
    # since some validation checks reuse the same view builder.
    return list(
        {
            view_builder
            for validation_check in get_all_validations()
            for view_builder in validation_check.managed_view_builders
        }
    )


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    return [
        # Views in the validation_views dataset which have no configured validation
        # jobs
        SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_SUPERVISION_STARTS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED,
        *CROSS_PROJECT_VALIDATION_VIEW_BUILDERS,
        # External Validation Data that feeds into configured validation views
        COUNTY_JAIL_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        get_incarceration_admission_person_level_view_builder(),
        INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        get_incarceration_population_person_level_view_builder(),
        get_incarceration_release_person_level_view_builder(),
        POPULATION_PROJECTION_MONTHLY_POPULATION_PER_FACILITY_VIEW_BUILDER,
        POPULATION_PROJECTION_MONTHLY_POPULATION_VIEW_BUILDER,
        RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER,
        SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
        get_supervision_population_person_level_view_builder(),
        SUPERVISION_START_PERSON_LEVEL_VIEW_BUILDER,
        SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
        get_supervision_population_by_type_view_builder(),
        CB_971_REPORT_UNIFIED_VIEW_BUILDER,
        CB_971_REPORT_SUPERVISION_UNIFIED_VIEW_BUILDER,
        US_MI_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_MI_SUPERVISION_POPULATION_BY_TYPE_VIEW_BUILDER,
        US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_COUNTY_JAIL_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_SUPERVISION_START_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_SUPERVISION_EARLY_DISCHARGE_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
        US_IX_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_IX_POPULATION_PROJECTION_MONTLY_POPULATION_VIEW_BUILDER,
        US_IX_POPULATION_PROJECTION_MONTLY_POPULATION_PER_FACILITY_VIEW_BUILDER,
        US_OZ_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ME_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_ME_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ME_POPULATION_RELEASES_VIEW_BUILDER,
        US_ME_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ME_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ME_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
        US_MO_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_MO_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_TN_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_TN_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ND_RECIDIVISM_PERSON_LEVEL_VIEW_BUILDER,
        US_ND_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ND_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_ND_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER,
        US_PA_INCARCERATION_ADMISSION_PERSON_LEVEL_VIEW_BUILDER,
        US_PA_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_PA_INCARCERATION_RELEASE_PERSON_LEVEL_VIEW_BUILDER,
        US_PA_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
        US_PA_SUPERVISION_TERMINATION_PERSON_LEVEL_VIEW_BUILDER,
        # All view builders for views with an associated configured validation job.
        *get_view_builders_from_configured_validations(),
    ]


VALIDATION_METADATA_BUILDERS: Sequence[
    BigQueryViewBuilder
] = ValidationTableColumnCounterBigQueryViewCollector(
    schema_config=get_external_validation_schema()
).collect_view_builders()


METADATA_VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[
    BigQueryViewBuilder
] = VALIDATION_METADATA_BUILDERS
