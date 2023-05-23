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
"""Tests the pipeline names."""
import unittest
from typing import Set

from recidiviz.calculator.pipeline.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.calculator.pipeline.utils.pipeline_run_delegate_utils import (
    collect_all_pipeline_names,
    collect_all_pipeline_run_delegate_classes,
)
from recidiviz.calculator.pipeline.utils.pipeline_run_utils import (
    collect_all_pipeline_classes,
)
from recidiviz.calculator.query.state.views.reference.reference_views import (
    REFERENCE_VIEW_BUILDERS,
)


class TestPipelineNames(unittest.TestCase):
    """Tests the names of all pipelines that can be run."""

    def test_collect_all_pipeline_names(self) -> None:
        """Tests that each pipeline run delegate has a config with a unique
        pipeline_name."""
        pipeline_names = collect_all_pipeline_names()

        self.assertCountEqual(set(pipeline_names), pipeline_names)


class TestReferenceViews(unittest.TestCase):
    """Tests the required_reference_tables and
    state_specific_required_reference_tables."""

    def test_all_reference_views_in_dataset(self) -> None:
        """Asserts that all the reference views required by the pipelines are in the
        reference_views dataset."""
        run_delegates = collect_all_pipeline_run_delegate_classes()

        all_required_reference_tables: Set[str] = set()

        for delegate in run_delegates:
            all_required_reference_tables.update(
                set(delegate.pipeline_config().required_reference_tables)
            )

            all_required_reference_tables.update(
                {
                    view
                    for state_views in delegate.pipeline_config().state_specific_required_reference_tables.values()
                    for view in state_views
                }
            )

        deployed_reference_view_names: Set[str] = {
            view_builder.view_id for view_builder in REFERENCE_VIEW_BUILDERS
        }

        self.assertEqual(
            set(),
            all_required_reference_tables.difference(deployed_reference_view_names),
        )


class TestPipelineValidations(unittest.TestCase):
    """Tests that specific pipelines are set up correctly."""

    def test_all_pipelines_are_validated(self) -> None:
        pipeline_classes = collect_all_pipeline_classes()

        for pipeline_class in pipeline_classes:
            if issubclass(pipeline_class, SupplementalDatasetPipeline):
                self.assertTrue("SUPPLEMENTAL" in pipeline_class.pipeline_name())
