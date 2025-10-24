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

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.validation import views as views_module
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import (
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
)
from recidiviz.validation.configured_validations import (
    get_all_deployed_validations,
    get_all_validations,
)
from recidiviz.validation.validation_config import ValidationRegionConfig
from recidiviz.validation.validation_models import (
    ValidationCategory,
    ValidationCheckType,
)
from recidiviz.validation.views.dataset_config import (
    TASK_ELIGIBILITY_VALIDATION_VIEWS_DATASET,
    VIEWS_DATASET,
)
from recidiviz.validation.views.state.prod_staging_comparison.experiments_assigments_large_prod_staging_comparison import (
    EXPERIMENT_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_BUILDER,
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
from recidiviz.validation.views.task_eligibility.configured_validations import (
    get_all_task_eligibility_validations,
)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class TestConfiguredValidations(unittest.TestCase):
    """Unit test for all configured data validations"""

    def test_configured_validations_all_compile(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            try:
                attr.validate(validation)  # type: ignore[arg-type]
            except Exception as e:
                self.fail(
                    f"{validation.validation_name} threw an unexpected exception: {e}"
                )

    def test_configured_validations_all_contain_region_code(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            validation_view = validation.view_builder.build()
            assert "region_code" in validation_view.view_query

    def test_configured_validation_views_all_materialized(self) -> None:
        validations = get_all_validations()
        for validation in validations:
            builder = validation.view_builder
            self.assertIsNotNone(
                builder.materialized_address,
                f"Found validation view that is not materialized: {builder.address}",
            )

    def test_configured_tes_validations_all_have_tes_specific_dataset(self) -> None:
        validations = get_all_task_eligibility_validations()
        for validation in validations:
            assert (
                validation.view_builder.address.dataset_id
                == TASK_ELIGIBILITY_VALIDATION_VIEWS_DATASET
            )

    def test_validations_projects_subset_of_views_projects(self) -> None:
        validations = get_all_validations()
        not_subsets = []
        for validation in validations:
            validation_projects = validation.projects_to_deploy or set(
                DATA_PLATFORM_GCP_PROJECTS
            )
            view_builder_projects = validation.view_builder.projects_to_deploy or set(
                DATA_PLATFORM_GCP_PROJECTS
            )
            if not validation_projects <= view_builder_projects:
                not_subsets.append(validation)

        if not_subsets:
            raise ValueError(
                f"Found validation that whose projects_to_deploy is not a subset of "
                f"the view buuilder's projects to deploy: "
                f"{[v.validation_name for v in not_subsets]}"
            )

    def test_all_validation_builders_configured_in_a_validation(self) -> None:
        found_builders = set(
            BigQueryViewCollector.collect_view_builders_in_module(
                # TODO(python/mypy#5374): Remove the ignore type when abstract class
                #  assignments are supported.
                builder_type=BigQueryViewBuilder,  # type: ignore[type-abstract]
                view_dir_module=views_module,
                recurse=True,
                view_builder_attribute_name_regex=".*_VIEW_BUILDER(S?)",
                expect_builders_in_all_files=False,
                collect_builders_from_callables=True,
                builder_callable_name_regex="collect_.*view_builders",
            )
        )

        found_validation_builders = set(
            builder.address
            for builder in found_builders
            if builder.address.dataset_id
            in (VIEWS_DATASET, TASK_ELIGIBILITY_VALIDATION_VIEWS_DATASET)
            # TODO(#15080): Configure validations for these views which were
            #  created without an associated configured validation, or move
            #  out of the validation_views dataset.
            and builder
            not in [
                # External validation data
                EXPERIMENT_ASSIGNMENTS_LARGE_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                INCARCERATION_ADMISSION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                INCARCERATION_RELEASE_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                INCARCERATION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_START_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_TERMINATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
                SUPERVISION_POPULATION_EXTERNAL_PROD_STAGING_COMPARISON_VIEW_BUILDER,
            ]
        )

        validations = get_all_validations()
        builders_in_validations: Set[BigQueryAddress] = {
            v.view_builder.address for v in validations
        }

        if in_validation_but_not_found := (
            builders_in_validations - found_validation_builders
        ):
            raise ValueError(
                f"Found validation registered in a configured validation in "
                f"configured_validations.py, but which is not defined in the validation "
                f"views directory: {in_validation_but_not_found}"
            )

        if not_registered_in_validation := (
            found_validation_builders - builders_in_validations
        ):
            raise ValueError(
                f"Found validation view builders that are not registered in a "
                f"configured validation in configured_validations.py: "
                f"{not_registered_in_validation}"
            )

    @patch("recidiviz.validation.configured_validations.get_all_validations")
    @patch("recidiviz.validation.configured_validations.get_validation_region_configs")
    def test_get_all_deployed_validations(
        self,
        mock_get_region_configs: MagicMock,
        mock_all_validations: MagicMock,
    ) -> None:
        existence_builder = SimpleBigQueryViewBuilder(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="existence_view",
            description="existence_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        sameness_builder = SimpleBigQueryViewBuilder(
            project_id="my_project",
            dataset_id="my_dataset",
            view_id="sameness_view",
            description="sameness_view description",
            view_query_template="SELECT NULL LIMIT 1",
        )
        mock_get_region_configs.return_value = {
            "US_XX": ValidationRegionConfig(
                region_code="US_XX",
                dev_mode=False,
                exclusions={},
                num_allowed_rows_overrides={},
                max_allowed_error_overrides={},
            ),
        }
        excluded = [
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
                projects_to_deploy=set(),
            ),
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
                projects_to_deploy={"test-project-1"},
            ),
        ]

        included = [
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
            ),
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
                projects_to_deploy=None,
            ),
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
                projects_to_deploy={"test-project"},
            ),
            ExistenceDataValidationCheck(
                validation_category=ValidationCategory.INVARIANT,
                view_builder=existence_builder,
                validation_name_suffix=None,
                validation_type=ValidationCheckType.EXISTENCE,
                dev_mode=False,
                hard_num_allowed_rows=10,
                soft_num_allowed_rows=10,
                projects_to_deploy={"test-project", "test-project-2"},
            ),
            SamenessDataValidationCheck(
                validation_category=ValidationCategory.CONSISTENCY,
                view_builder=sameness_builder,
                validation_name_suffix=None,
                comparison_columns=["col1", "col2"],
                sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
                dev_mode=False,
                hard_max_allowed_error=0.3,
                soft_max_allowed_error=0.3,
                validation_type=ValidationCheckType.SAMENESS,
                region_configs=mock_get_region_configs(),
            ),
        ]

        mock_all_validations.return_value = [*included, *excluded]

        deployed_validations = get_all_deployed_validations()
        assert deployed_validations == included
