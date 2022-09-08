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
"""Unit test for all configured data validations"""
import unittest
from typing import Set
from unittest.mock import MagicMock, patch

import attr

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.validation import views as views_module
from recidiviz.validation.configured_validations import get_all_validations
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
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


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class TestConfiguredValidations(unittest.TestCase):
    """Unit test for all configured data validations"""

    def test_configured_validations_all_compile(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            try:
                attr.validate(validation)
            except Exception as e:
                self.fail(
                    f"{validation.validation_name} threw an unexpected exception: {e}"
                )

    def test_configured_validations_all_contain_region_code(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            validation_view = validation.view_builder.build()
            assert "region_code" in validation_view.view_query

    def test_all_validation_builders_configured_in_a_validation(self) -> None:
        found_builders = set(
            BigQueryViewCollector.collect_view_builders_in_module(
                # TODO(python/mypy#5374): Remove the ignore type when abstract class
                #  assignments are supported.
                builder_type=BigQueryViewBuilder,  # type: ignore[misc]
                view_dir_module=views_module,
                recurse=True,
                view_builder_attribute_name_regex=".*_VIEW_BUILDER",
                expect_builders_in_all_files=False,
            )
        )

        found_validation_builders = set(
            builder
            for builder in found_builders
            if builder.address.dataset_id == VIEWS_DATASET
            # TODO(#15080): Configure validations for these views which were
            #  created without an associated configured validation, or move
            #  out of the validation_views dataset.
            and builder
            not in [
                # Polaris
                PO_REPORT_CLIENTS_VIEW_BUILDER,
                # External validation data
                INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_START_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                # Sessions
                SESSIONS_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER,
                SESSIONS_JUSTICE_COUNTS_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
                REINCARCERATIONS_FROM_DATAFLOW_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
                REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
            ]
        )

        validations = get_all_validations()
        builders_in_validations: Set[BigQueryViewBuilder] = {
            v.view_builder for v in validations
        }

        if in_validation_but_not_found := (
            builders_in_validations - found_validation_builders
        ):
            raise ValueError(
                f"Found validation registered in a configured validation in "
                f"configured_validations.py, but which is not defined in the validation "
                f"views directory: "
                f"{[b.address for b in in_validation_but_not_found]}"
            )

        if not_registered_in_validation := (
            found_validation_builders - builders_in_validations
        ):
            raise ValueError(
                f"Found validation view builders that are not registered in a "
                f"configured validation in configured_validations.py: "
                f"{[b.address for b in not_registered_in_validation]}"
            )
