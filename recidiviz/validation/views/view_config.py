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
from recidiviz.validation.views.external_data.external_validation_data_view_collector import (
    ExternalValidationDataBigQueryViewCollector,
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
        *ExternalValidationDataBigQueryViewCollector().collect_state_specific_and_build_state_agnostic_external_validation_view_builders(),
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
